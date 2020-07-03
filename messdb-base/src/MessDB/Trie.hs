{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, OverloadedStrings, PatternSynonyms, ViewPatterns #-}

module MessDB.Trie
  ( Node()
  , Trie(..)
  , Key(..)
  , Value
  , emptyTrie
  , singletonTrie
  , trieHash
  , unsafeTrieFromHash
  , reloadTrie
  , trieToItems
  , mergeTries
  , sortTrie
  , Func(..)
  , FuncKey(..)
  , TransformFunc
  , FoldFunc
  , itemsToTrie
  , foldToLast
  , checkTrie
  , debugPrintTrie
  ) where

import Control.Arrow
import Control.Monad
import qualified Crypto.Hash as C
import qualified Data.ByteArray as BA
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import Data.Int
import qualified Data.Serialize as S
import Data.String
import qualified Data.Vector as V
import Data.Word
import MessDB.Store
import System.IO.Unsafe(unsafePerformIO)

newtype Key = Key
  { unKey :: BS.ShortByteString
  } deriving (Eq, Ord, Semigroup, Monoid, S.Serialize, IsString, Show)

type Value = B.ByteString

data Item
  = ValueItem
    { item_path :: {-# UNPACK #-} !Key
    , item_value :: Value
    }
  | InlineNodeItem
    { item_path :: {-# UNPACK #-} !Key
    , item_node :: Node
    }
  | ExternalNodeItem
    { item_path :: {-# UNPACK #-} !Key
    , item_node :: Node
    }

instance Encodable Item where
  encode = \case
    ValueItem
      { item_path = path
      , item_value = value
      } -> do
      S.putWord8 0
      S.put path
      S.put value
    InlineNodeItem
      { item_path = path
      , item_node = Node
        { node_encoded = nodeEncoded
        }
      } -> do
      S.putWord8 1
      S.put path
      S.putLazyByteString nodeEncoded
    ExternalNodeItem
      { item_path = path
      , item_node = Node
        { node_hash = nodeHash
        }
      } -> do
      S.putWord8 2
      S.put path
      encode nodeHash
  decode = do
    itemType <- S.getWord8
    case itemType of
      0 -> do
        path <- S.get
        value <- S.get
        return ValueItem
          { item_path = path
          , item_value = value
          }
      1 -> do
        path <- S.get
        node <- decode
        return InlineNodeItem
          { item_path = path
          , item_node = node
          }
      2 -> do
        path <- S.get
        nodeHash <- decode
        return ExternalNodeItem
          { item_path = path
          , item_node = Node
            { node_hash = nodeHash
            , node_items = error "node_items: ExternalNodeItem not loaded"
            , node_encoded = error "node_encoded: ExternalNodeItem not loaded"
            }
          }
      _ -> fail "wrong item type"

-- | Node.
-- Node can be stored separately by hash, or inlined into parent node.
-- Basis for byte-based radix trie.
-- No two items can have the same first byte in their paths.
-- At most one item is allowed to have empty path, and it should be ValueItem.
-- Items are sorted by path.
data Node = Node
  { node_hash :: StoreKey
  , node_items :: V.Vector Item
  , node_encoded :: BL.ByteString
  }

instance Encodable Node where
  encode Node
    { node_items = items
    } = encode items
  decode = itemsToNode <$> decode

instance Persistable Node where
  save store Node
    { node_hash = nodeHash
    , node_items = items
    , node_encoded = nodeEncoded
    } = storeSave store nodeHash $ do
    V.forM_ items $ \case
      ExternalNodeItem
        { item_node = itemNode
        } -> save store itemNode
      _ -> return ()
    return nodeEncoded
  load store hash = do
    node@Node
      { node_hash = nodeHash
      , node_items = items
      } <- either fail return . S.runGetLazy decode =<< storeLoad store hash
    unless (nodeHash == hash) $ fail "node hash is wrong"
    let
      hydrateItems = V.map $ \item -> case item of
        ValueItem {} -> item
        InlineNodeItem
          { item_node = subNode@Node
            { node_items = subItems
            }
          } -> item
          { item_node = subNode
            { node_items = hydrateItems subItems
            }
          }
        ExternalNodeItem
          { item_node = subNode@Node
            { node_hash = itemNodeHash
            }
          } -> item
          { item_node = let
            Node
              { node_items = loadedNodeItems
              , node_encoded = loadedNodeEncoded
              } = unsafePerformIO $ load store itemNodeHash
            in subNode
              { node_items = loadedNodeItems
              , node_encoded = loadedNodeEncoded
              }
          }
    return node
      { node_items = hydrateItems items
      }

newtype Trie = Trie
  { unTrie :: Node
  }

itemsToNode :: V.Vector Item -> Node
itemsToNode items = node where
  node = Node
    { node_hash = StoreKey $ BS.toShort $ BA.convert h
    , node_items = items
    , node_encoded = nodeEncoded
    }
  nodeEncoded = S.runPutLazy $ encode node
  h :: C.Digest C.SHA256
  h = C.hashlazy nodeEncoded

maxInlineNodeSize :: Int64
maxInlineNodeSize = 4096

isNodeBig :: Node -> Bool
isNodeBig Node
  { node_encoded = nodeEncoded
  } = BL.length nodeEncoded > maxInlineNodeSize

emptyTrie :: Trie
emptyTrie = Trie emptyNode

emptyNode :: Node
emptyNode = itemsToNode V.empty

singletonTrie :: Key -> Value -> Trie
singletonTrie key value = Trie $ singletonNode key value

singletonNode :: Key -> Value -> Node
singletonNode key value = itemsToNode $ V.singleton ValueItem
  { item_path = key
  , item_value = value
  }

trieHash :: Trie -> StoreKey
trieHash (Trie Node
  { node_hash = nodeHash
  }) = nodeHash

-- | Unsafely create unloaded trie from hash.
unsafeTrieFromHash :: StoreKey -> Trie
unsafeTrieFromHash nodeHash = Trie Node
  { node_hash = nodeHash
  , node_items = error "node not loaded"
  , node_encoded = error "node not loaded"
  }

-- | Load trie from store again.
-- Uses only hash. Good for hydrating trie from `unsafeTrieFromHash`.
reloadTrie :: Store s => s -> Trie -> Trie
reloadTrie store trie = Trie $ unsafePerformIO $ load store $ trieHash trie

trieToItems :: Trie -> [(Key, Value)]
trieToItems (Trie node) = unpackNode mempty node where
  unpackNode prefix Node
    { node_items = items
    } = let
    unpackItem ValueItem
      { item_path = path
      , item_value = value
      } = [(prefix <> path, value)]
    unpackItem InlineNodeItem
      { item_path = path
      , item_node = subNode
      } = unpackNode (prefix <> path) subNode
    unpackItem ExternalNodeItem
      { item_path = path
      , item_node = subNode
      } = unpackNode (prefix <> path) subNode
    in concatMap unpackItem $ V.toList items

-- | Memoization for nodes.
-- Evaluates passed node only if it's missing from store.
memoize :: (Store s, MemoStore ms) => s -> ms -> StoreKey -> Node -> Node
memoize store memoStore memoHash node = unsafePerformIO $ do
  -- ensure node is stored and get hash
  r <- memoStoreCache memoStore memoHash $ do
    save store node
    let
      nodeHash = node_hash node
    return (Just $ unStoreKey nodeHash, nodeHash)

  case r of
    -- hash loaded from cache, load node
    Left nodeHash -> load store $ StoreKey nodeHash
    -- node was built, reload it by hash
    Right nodeHash -> load store nodeHash

-- | Merge tries.
mergeTries :: (Store s, MemoStore ms) => s -> ms -> FoldFunc -> V.Vector Trie -> Trie
mergeTries store memoStore foldFunc = Trie . mergeNodes store memoStore foldFunc mempty . V.map unTrie

-- | Merge non-zero number of nodes.
mergeNodes :: (Store s, MemoStore ms) => s -> ms -> FoldFunc -> Key -> V.Vector Node -> Node
mergeNodes store memoStore foldFunc@Func
  { func_key = foldKey
  , func_func = fold
  } pathPrefix rootNodes = memoize store memoStore (StoreKey $ BS.toShort $ BA.convert mergeOpHash) mergedNode where
  mergeOpHash :: C.Digest C.SHA256
  mergeOpHash = C.hashlazy $ S.runPutLazy $ do
    S.putWord8 OP_MERGE_NODES
    S.put foldKey
    S.put pathPrefix
    mapM_ (encode . node_hash) rootNodes

  mergedNode = itemsToNode $ V.fromList $ step rootItemsLists

  rootItemsLists :: V.Vector [Item]
  rootItemsLists = V.map (V.toList . node_items) rootNodes

  step :: V.Vector [Item] -> [Item]
  step itemsLists = let
    -- first byte of a path of first item
    maybeFirstPathByte :: [Item] -> Maybe (Maybe Word8)
    maybeFirstPathByte [] = Nothing
    maybeFirstPathByte ((item_path -> path) : _) = Just (maybeFirstByte path)

    maybeMinimumMaybeFirstByte :: Maybe (Maybe Word8)
    maybeMinimumMaybeFirstByte = let
      maybeFirstBytes = V.mapMaybe maybeFirstPathByte itemsLists
      in if V.null maybeFirstBytes
        then Nothing
        else Just $ V.minimum maybeFirstBytes

    in case maybeMinimumMaybeFirstByte of
      Just minimumMaybeFirstByte -> let
        -- predicate for extracting first items with the same first byte
        extractItem = \case
          item@(item_path -> path) : restItems | maybeFirstByte path == minimumMaybeFirstByte -> (Just item, restItems)
          items -> (Nothing, items)

        currentItems :: V.Vector Item
        restItemsLists :: V.Vector [Item]
        (currentItems, restItemsLists) = (V.mapMaybe fst) &&& (V.map snd) <<< V.map extractItem $ itemsLists

        nextStep = step restItemsLists

        in case V.length currentItems of
          0 -> nextStep
          1 -> V.head currentItems : nextStep
          _ -> let
            -- max shared prefix of two lists
            sharedPrefix (a : aa) (b : bb) = if a == b
              then a : sharedPrefix aa bb
              else []
            sharedPrefix _ _ = []
            -- max prefix of current items
            maxPrefix = BS.pack $ V.foldl1 sharedPrefix $ V.map (BS.unpack . unKey . item_path) currentItems
            maxPrefixLength = BS.length maxPrefix

            -- remove prefix from path
            dropPathPrefix = Key . BS.pack . drop maxPrefixLength . BS.unpack . unKey
            -- remove prefix from item path
            dropItemPathPrefix item = item
              { item_path = dropPathPrefix $ item_path item
              }

            -- pack item into node, cutting off prefix
            packItem item = case item of
              -- explode internal node items
              InlineNodeItem
                { item_path = path
                , item_node = Node
                  { node_items = subItems
                  }
                } -> let
                explodeSubItem prefix = \case
                  InlineNodeItem
                    { item_path = subPath
                    , item_node = Node
                      { node_items = subSubItems
                      }
                    } -> V.concatMap (explodeSubItem $ prefix <> subPath) subSubItems
                  subItem -> V.singleton $ itemsToNode $ V.singleton subItem
                    { item_path = prefix <> item_path subItem
                    }
                in V.concatMap (explodeSubItem $ dropPathPrefix path) subItems
              -- explode external node items with zero prefix
              ExternalNodeItem
                { item_path = Key path
                , item_node = Node
                  { node_items = subItems
                  }
                } | BS.length path == maxPrefixLength -> V.singleton $ itemsToNode subItems
              _ -> V.singleton $ itemsToNode $ V.singleton $ dropItemPathPrefix item

            -- if prefix is zero
            in if maxPrefixLength == 0
              -- just fold the value items
              then ValueItem
                { item_path = mempty
                , item_value = V.foldl1 (fold pathPrefix) $ V.map item_value currentItems
                } : nextStep
              -- else recursively merge tries
              else let
                mergedSubNode@Node
                  { node_items = mergedSubNodeItems
                  } = mergeNodes store memoStore foldFunc (pathPrefix <> Key maxPrefix) (V.concatMap packItem currentItems)
                mergedItem = case V.head mergedSubNodeItems of
                  -- do not wrap the only NodeItem into another item
                  subItem@InlineNodeItem
                    { item_path = path
                    } | V.length mergedSubNodeItems == 1 -> subItem
                    { item_path = Key maxPrefix <> path
                    }
                  subItem@ExternalNodeItem
                    { item_path = path
                    } | V.length mergedSubNodeItems == 1 -> subItem
                    { item_path = Key maxPrefix <> path
                    }
                  _ -> if isNodeBig mergedSubNode
                    then ExternalNodeItem
                      { item_path = Key maxPrefix
                      , item_node = mergedSubNode
                      }
                    else InlineNodeItem
                      { item_path = Key maxPrefix
                      , item_node = mergedSubNode
                      }
                in mergedItem : nextStep

      Nothing -> []


sortTrie :: (Store s, MemoStore ms) => s -> ms -> TransformFunc -> FoldFunc -> Trie -> Trie
sortTrie store memoStore transformFunc foldFunc = Trie . sortNode store memoStore transformFunc foldFunc mempty . unTrie

sortNode :: (Store s, MemoStore ms) => s -> ms -> TransformFunc -> FoldFunc -> Key -> Node -> Node
sortNode store memoStore transformFunc@Func
  { func_key = transformKey
  , func_func = transform
  } foldFunc@Func
  { func_key = foldKey
  } pathPrefix Node
  { node_hash = rootNodeHash
  , node_items = items
  } = memoize store memoStore (StoreKey $ BS.toShort $ BA.convert sortOpHash) sortedNode where
  sortOpHash :: C.Digest C.SHA256
  sortOpHash = C.hashlazy $ S.runPutLazy $ do
    S.putWord8 OP_SORT_NODE
    S.put transformKey
    S.put foldKey
    S.put pathPrefix
    encode rootNodeHash

  sortedNode = mergeNodes store memoStore foldFunc mempty $ V.concatMap (sortItem pathPrefix) items

  sortItem itemPathPrefix = \case
    ValueItem
      { item_path = path
      , item_value = value
      } -> V.singleton $ uncurry singletonNode $ transform (itemPathPrefix <> path) value
    InlineNodeItem
      { item_path = path
      , item_node = Node
        { node_items = subItems
        }
      } -> V.concatMap (sortItem (itemPathPrefix <> path)) subItems
    ExternalNodeItem
      { item_path = path
      , item_node = node
      } -> V.singleton $ sortNode store memoStore transformFunc foldFunc (itemPathPrefix <> path) node


data Func a = Func
  { func_key :: {-# UNPACK #-} !FuncKey
  , func_func :: !a
  }

newtype FuncKey = FuncKey BS.ShortByteString deriving (Eq, S.Serialize, IsString)

type TransformFunc = Func (Key -> Value -> (Key, Value))
type FoldFunc = Func (Key -> Value -> Value -> Value)

-- | Standard fold operation: last item takes precedence.
-- Fold key 'OP_FOLD_TO_LAST'.
foldToLast :: FoldFunc
foldToLast = Func
  { func_key = OP_FOLD_TO_LAST
  , func_func = \_k _a b -> b
  }

-- | Create trie from items.
itemsToTrie :: (Store s, MemoStore ms) => s -> ms -> V.Vector (Key, Value) -> Trie
itemsToTrie store memoStore pairs = mergeTries store memoStore foldToLast tries where
  tries = V.map (uncurry singletonTrie) pairs




-- Magic numbers for operations.

pattern OP_MERGE_NODES :: Word8
pattern OP_MERGE_NODES = 0

pattern OP_SORT_NODE :: Word8
pattern OP_SORT_NODE = 1


-- Standard fold operations.

pattern OP_FOLD_TO_LAST :: FuncKey
pattern OP_FOLD_TO_LAST = "fold_to_last"


-- Utils

-- first byte of bytestring, or Nothing if it's empty
maybeFirstByte :: Key -> Maybe Word8
maybeFirstByte (Key bytes) = if BS.null bytes
  then Nothing
  else Just $ BS.index bytes 0


-- Checking

checkTrie :: Trie -> Bool
checkTrie = checkNode . unTrie

checkNode :: Node -> Bool
checkNode = checkNode' True

checkNode' :: Bool -> Node -> Bool
checkNode' isRoot Node
  { node_items = items
  }
  -- non-zero number of items in non-root node
  =  (isRoot || not (V.null items))
  -- items must be sorted by paths, and paths must be different by first letter
  && fst (V.foldl' (\(ok, maybePrevPath) (item_path -> path) -> (ok && maybe True (`pathsOrdered` path) maybePrevPath, Just path)) (True, Nothing) items)
  -- empty-key item must be ValueItem
  -- only item in non-root node must be ValueItem
  && (if V.null items
    then True
    else case V.head items of
      InlineNodeItem
        { item_path = Key path
        } | (BS.null path || V.length items == 1 && not isRoot) -> False
      ExternalNodeItem
        { item_path = Key path
        } | (BS.null path || V.length items == 1 && not isRoot) -> False
      _ -> True
    )
  -- recurse into subnodes
  && V.all (\case
    ValueItem {} -> True
    InlineNodeItem
      { item_node = subNode
      } -> not (isNodeBig subNode) && checkNode' False subNode
    ExternalNodeItem
      { item_node = subNode
      } -> isNodeBig subNode && checkNode' False subNode
    ) items
  where
    pathsOrdered p1 p2 = maybeFirstByte p1 < maybeFirstByte p2

-- Debug printing

debugPrintTrie :: Trie -> IO ()
debugPrintTrie (Trie node) = debugPrintNode 0 node

debugPrintNode :: Int -> Node -> IO ()
debugPrintNode space Node
  { node_hash = nodeHash
  , node_items = items
  } = do
  debugPrintSpace space
  print nodeHash
  mapM_ (debugPrintItem (space + 1)) items

debugPrintItem :: Int -> Item -> IO ()
debugPrintItem space item = do
  debugPrintSpace space
  putStr $ show $ unKey $ item_path item
  putStr ": "
  case item of
    ValueItem
      { item_value = value
      } -> print value
    InlineNodeItem
      { item_node = node
      } -> do
      putStrLn "inline_node"
      debugPrintNode (space + 1) node
    ExternalNodeItem
      { item_node = node
      } -> do
      putStrLn "external_node"
      debugPrintNode (space + 1) node

debugPrintSpace :: Int -> IO ()
debugPrintSpace space = putStr $ concat $ replicate space "  "
