{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, OverloadedStrings, PatternSynonyms, ViewPatterns #-}

module MessDB.Trie
  ( Node()
  , Trie(..)
  , Key(..)
  , Value
  , FoldKey(..)
  , emptyTrie
  , singletonTrie
  , trieToItems
  , mergeTries
  , itemsToTrie
  , foldToLast
  , pattern OP_FOLD_TO_LAST
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
import Data.Maybe
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
  | NodeItem
    { item_path :: {-# UNPACK #-} !Key
    , item_node :: Node -- stored either inline or hash only
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
    NodeItem
      { item_path = path
      , item_node = Node
        { node_maybeHash = nodeMaybeHash
        , node_encoded = nodeEncoded
        }
      } -> do
      S.putWord8 1
      S.put path
      case nodeMaybeHash of
        Nothing -> do
          S.putWord8 0
          S.putLazyByteString nodeEncoded
        Just nodeHash -> do
          S.putWord8 1
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
        nodeType <- S.getWord8
        case nodeType of
          -- inline node
          0 -> do
            node <- decode
            return NodeItem
              { item_path = path
              , item_node = node
              }
          -- external node
          1 -> do
            nodeHash <- decode
            return NodeItem
              { item_path = path
              , item_node = Node
                { node_maybeHash = Just nodeHash
                , node_items = error "items: NodeItem not loaded"
                , node_encoded = error "encoded: NodeItem not loaded"
                }
              }
          _ -> fail "wrong item node type"
      _ -> fail "wrong item type"

-- | Node.
-- Node can be stored separately by hash, or inlined into parent node.
-- Basis for byte-based radix trie.
-- No two items can have the same first byte in their paths.
-- At most one item is allowed to have empty path, and it should be ValueItem.
-- Items are sorted by path.
data Node = Node
  { node_maybeHash :: Maybe StoreKey
  , node_items :: V.Vector Item -- stored
  , node_encoded :: BL.ByteString
  }

instance Encodable Node where
  encode Node
    { node_items = items
    } = encode items
  decode = itemsToNode <$> decode

instance Persistable Node where
  save store node@Node
    { node_items = items
    , node_encoded = nodeEncoded
    } = storeSave store (hashOfNode node) $ do
    V.forM_ items $ \case
      NodeItem
        { item_node = itemNode@Node
          { node_maybeHash = Just _
          }
        } -> save store itemNode
      _ -> return ()
    return nodeEncoded
  load store hash = do
    node@Node
      { node_items = items
      } <- either fail (return . ensureNodeHash) . S.runGetLazy decode =<< storeLoad store hash
    let
      calculatedHash = hashOfNode node
    unless (calculatedHash == hash) $ fail "node hash is wrong"
    let
      hydrateItems = V.map $ \item -> case item of
        ValueItem {} -> item
        NodeItem
          { item_node = subNode@Node
            { node_maybeHash = itemNodeMaybeHash
            , node_items = subItems
            }
          } -> case itemNodeMaybeHash of
          Just itemNodeHash -> item
            { item_node = unsafePerformIO $ load store itemNodeHash
            }
          Nothing -> item
            { item_node = subNode
              { node_items = hydrateItems subItems
              }
            }
    return node
      { node_items = hydrateItems items
      }

data Trie
  = Trie Node
  | EmptyTrie

itemsToNode :: V.Vector Item -> Node
itemsToNode items = finalizeNode node where
  node = Node
    { node_maybeHash = Nothing
    , node_items = items
    , node_encoded = S.runPutLazy $ encode node
    }

maxInlineNodeSize :: Int64
maxInlineNodeSize = 1024

-- | Finalize node.
-- Add encoded representation and hash if needed.
finalizeNode :: Node -> Node
finalizeNode node@Node
  { node_encoded = nodeEncoded
  } = node
  { node_maybeHash = if BL.length nodeEncoded > maxInlineNodeSize
    then Just (hashOfNode node)
    else Nothing
  }

-- | Calculate hash of node.
hashOfNode :: Node -> StoreKey
hashOfNode Node
  { node_maybeHash = maybeHash
  , node_encoded = nodeEncoded
  } = fromMaybe (StoreKey $ BS.toShort $ BA.convert h) maybeHash where
  h :: C.Digest C.SHA256
  h = C.hashlazy nodeEncoded

ensureNodeHash :: Node -> Node
ensureNodeHash node@Node
  { node_maybeHash = maybeHash
  } = case maybeHash of
  Just _ -> node
  Nothing -> node
    { node_maybeHash = Just $ hashOfNode node
    }

emptyTrie :: Trie
emptyTrie = EmptyTrie

singletonTrie :: Key -> Value -> Trie
singletonTrie key value = Trie $ singletonNode key value

singletonNode :: Key -> Value -> Node
singletonNode key value = itemsToNode $ V.singleton ValueItem
  { item_path = key
  , item_value = value
  }

trieToItems :: Trie -> [(Key, Value)]
trieToItems EmptyTrie = []
trieToItems (Trie node) = unpackNode mempty node where
  unpackNode prefix Node
    { node_items = items
    } = let
    unpackItem ValueItem
      { item_path = path
      , item_value = value
      } = [(prefix <> path, value)]
    unpackItem NodeItem
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
    case node_maybeHash node of
      -- if node has a hash
      Just nodeHash -> do
        -- persist node first
        save store node
        -- return hash
        return (Just $ unStoreKey nodeHash, Left nodeHash)
      -- otherwise node is inline
      Nothing -> return (Nothing, Right node)
  case r of
    -- hash loaded from cache, load node
    Left nodeHash -> load store $ StoreKey nodeHash
    -- normal node was built, reload it by hash
    Right (Left nodeHash) -> load store nodeHash
    -- inline node was built, use it
    Right (Right newNode) -> return newNode

-- | Merge tries.
mergeTries :: (Store s, MemoStore ms) => s -> ms -> FoldKey -> (Key -> Value -> Value -> Value) -> V.Vector Trie -> Trie
mergeTries store memoStore foldKey fold tries = if V.null nodes
  then EmptyTrie
  else Trie $ mergeNodes store memoStore foldKey fold mempty nodes
  where
    nodes = V.mapMaybe trieToNode tries
    trieToNode = \case
      Trie node -> Just node
      EmptyTrie -> Nothing

-- | Merge non-zero number of nodes.
mergeNodes :: (Store s, MemoStore ms) => s -> ms -> FoldKey -> (Key -> Value -> Value -> Value) -> Key -> V.Vector Node -> Node
mergeNodes store memoStore foldKey fold pathPrefix rootNodes = memoize store memoStore (StoreKey $ BS.toShort $ BA.convert mergeOpHash) mergedNode where
  mergeOpHash :: C.Digest C.SHA256
  mergeOpHash = C.hashlazy $ S.runPutLazy $ do
    S.putWord8 OP_MERGE_TRIES
    S.put foldKey
    S.put pathPrefix
    mapM_ (encode . hashOfNode) rootNodes

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

            -- remove prefix from item path
            dropItemPathPrefix item = item
              { item_path = Key $ BS.pack $ drop maxPrefixLength $ BS.unpack $ unKey $ item_path item
              }

            -- pack item into node, cutting of prefix
            packItem item = case item of
              -- explode node items
              NodeItem
                { item_path = Key path
                , item_node = Node
                  { node_items = subItems
                  }
                } -> itemsToNode $ V.map (\subItem -> subItem
                { item_path = Key (BS.pack $ drop maxPrefixLength $ BS.unpack path) <> item_path subItem
                }) subItems
              _ -> itemsToNode $ V.singleton $ dropItemPathPrefix item

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
                  } = mergeNodes store memoStore foldKey fold (pathPrefix <> Key maxPrefix) (V.map packItem currentItems)
                mergedItem = case V.head mergedSubNodeItems of
                  -- do not wrap the only NodeItem into another item
                  subItem@NodeItem
                    { item_path = path
                    } | V.length mergedSubNodeItems == 1 -> subItem
                    { item_path = Key maxPrefix <> path
                    }
                  _ -> NodeItem
                    { item_path = Key maxPrefix
                    , item_node = mergedSubNode
                    }
                in mergedItem : nextStep

      Nothing -> []


newtype FoldKey = FoldKey BS.ShortByteString deriving (Eq, S.Serialize, IsString)

-- | Standard fold operation: last item takes precedence.
-- Fold key 'OP_FOLD_TO_LAST'.
foldToLast :: Key -> Value -> Value -> Value
foldToLast _k _a b = b

-- | Create trie from items.
itemsToTrie :: (Store s, MemoStore ms) => s -> ms -> V.Vector (Key, Value) -> Trie
itemsToTrie store memoStore pairs = mergeTries store memoStore OP_FOLD_TO_LAST foldToLast tries where
  tries = V.map (uncurry singletonTrie) pairs




-- Magic numbers for operations.
pattern OP_MERGE_TRIES :: Word8
pattern OP_MERGE_TRIES = 0

-- Standard fold operations.
pattern OP_FOLD_TO_LAST :: FoldKey
pattern OP_FOLD_TO_LAST = "fold_to_last"


-- Utils

-- first byte of bytestring, or Nothing if it's empty
maybeFirstByte :: Key -> Maybe Word8
maybeFirstByte (Key (BS.unpack -> bytes)) = case bytes of
  x : _ -> Just x
  [] -> Nothing


-- Checking

checkTrie :: Trie -> Bool
checkTrie (Trie node) = checkNode node
checkTrie EmptyTrie = True

checkNode :: Node -> Bool
checkNode = checkNode' True

checkNode' :: Bool -> Node -> Bool
checkNode' isRoot node@Node
  { node_maybeHash = maybeHash
  , node_items = items
  }
  =  maybe True (hashOfNode node ==) maybeHash
  -- non-zero number of items
  && not (V.null items)
  -- items must be sorted by paths, and paths must be different by first letter
  && fst (V.foldl' (\(ok, maybePrevPath) (item_path -> path) -> (ok && maybe True (`pathsOrdered` path) maybePrevPath, Just path)) (True, Nothing) items)
  -- empty-key item must be ValueItem
  -- only item in non-root node must be ValueItem
  && (
    case V.head items of
      NodeItem
        { item_path = Key path
        } | (BS.null path || V.length items == 1 && not isRoot) -> False
      _ -> True
    )
  -- recurse into subnodes
  && V.all (\case
    ValueItem {} -> True
    NodeItem
      { item_node = subNode
      } -> checkNode' False subNode
    ) items
  where
    pathsOrdered p1 p2 = maybeFirstByte p1 < maybeFirstByte p2

-- Debug printing

debugPrintTrie :: Trie -> IO ()
debugPrintTrie (Trie node) = debugPrintNode 0 node
debugPrintTrie EmptyTrie = putStrLn "empty-trie"

debugPrintNode :: Int -> Node -> IO ()
debugPrintNode space Node
  { node_maybeHash = maybeHash
  , node_items = items
  } = do
  debugPrintSpace space
  print maybeHash
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
    NodeItem
      { item_node = node
      } -> do
      putStrLn "node"
      debugPrintNode (space + 1) node

debugPrintSpace :: Int -> IO ()
debugPrintSpace space = putStr $ concat $ replicate space "  "
