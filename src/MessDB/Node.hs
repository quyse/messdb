{-# LANGUAGE BangPatterns, ConstraintKinds, DefaultSignatures, GADTs, GeneralizedNewtypeDeriving, LambdaCase, ViewPatterns #-}

module MessDB.Node
  ( NodeHash(..)
  , IsKeyValue
  , IsItem(..)
  , IsTree
  , nodeHash
  , Node(..)
  , ValueItem(..)
  , NodeItem(..)
  , Tree(..)
  , itemsToTree
  , reloadNode
  , checkTree
  , sortTree
  , debugPrintTree
  ) where

import Control.Monad
import qualified Crypto.Hash as C
import qualified Data.ByteArray as BA
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import Data.Hashable(Hashable)
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Proxy
import qualified Data.Serialize as S
import qualified Data.Vector as V
import System.IO.Unsafe(unsafePerformIO)

import MessDB.Slicing
import MessDB.Store

newtype NodeHash = NodeHash StoreKey deriving (Eq, Hashable, Show)

instance Encodable NodeHash where
  encode (NodeHash (StoreKey bytes)) = S.putShortByteString bytes
  decode = NodeHash . StoreKey <$> S.getShortByteString (C.hashDigestSize (undefined :: C.SHA256))

-- | Requirements for key and value.
type IsKeyValue k v = (Ord k, Encodable k, Encodable v)

class IsItem q where
  itemMinKey :: Ord k => q k v -> k
  itemMaxKey :: Ord k => q k v -> k
  saveItem :: (Store s, IsKeyValue k v) => s -> q k v -> IO ()
  loadItem :: (Store s, IsKeyValue k v) => s -> q k v -> IO (q k v)

  itemLevel :: Proxy q -> Int

  -- | Sort operation.
  -- Transforms key-value pairs into new key and value, and merges pairs with the same key.
  sortItems :: (Store s, IsKeyValue k v, IsKeyValue k2 v2) => s -> (ValueItem k v -> ValueItem k2 v2) -> (k2 -> v2 -> v2 -> v2) -> V.Vector (q k v) -> Tree k2 v2

  -- Functions for encoding and decoding. As additional constraints are needed,
  -- it is impossible to do via Encodable requirement for the IsItem class.

  encodeItem :: IsKeyValue k v => q k v -> S.Put
  default encodeItem :: Encodable (q k v) => q k v -> S.Put
  encodeItem = encode

  decodeItem :: IsKeyValue k v => S.Get (q k v)
  default decodeItem :: Encodable (q k v) => S.Get (q k v)
  decodeItem = decode

  checkItem :: IsKeyValue k v => q k v -> Bool -> Bool
  debugPrintItem :: (IsKeyValue k v, Show k) => Int -> q k v -> IO ()

-- | Requirements for an hierarchy of nodes.
type IsTree q k v = (IsItem q, IsKeyValue k v)

-- | Node of B-tree.
data Node q k v = Node
  { node_hash :: NodeHash
  , node_minKey :: k
  , node_maxKey :: k
  , node_items :: (V.Vector (q k v))
  }

nodeHash :: IsTree q k v => Node q k v -> NodeHash
nodeHash a = NodeHash $ StoreKey $ BS.toShort $ BA.convert h where
  h :: C.Digest C.SHA256
  h = C.hash $ S.runPut $ encode a

instance IsTree q k v => Encodable (Node q k v) where
  encode Node
    { node_minKey = minKey
    , node_maxKey = maxKey
    , node_items = items
    } = do
    S.putWord8 0 -- Node magic
    encode minKey
    encode maxKey
    S.put $ V.length items
    V.mapM_ encodeItem items
  decode = do
    0 <- S.getWord8 -- Node magic
    minKey <- decode
    maxKey <- decode
    itemsCount <- S.get
    items <- V.replicateM itemsCount decodeItem
    let
      node = Node
        { node_hash = nodeHash node
        , node_minKey = minKey
        , node_maxKey = maxKey
        , node_items = items
        }
      in return node

instance IsTree q k v => Persistable (Node q k v) where
  save store node@Node
    { node_hash = NodeHash hash
    , node_items = items
    } = storeSave store hash $ do
    mapM_ (saveItem store) items
    return $ S.runPutLazy $ encode node
  load store hash = do
    node@Node
      { node_hash = NodeHash calculatedHash
      , node_items = items
      } <- either fail return . S.runGetLazy decode =<< storeLoad store hash
    unless (calculatedHash == hash) $ fail "node hash is wrong"
    return node
      { node_items = V.map (unsafePerformIO . loadItem store) items
      }

-- | Item storing actual key-value pair.
data ValueItem k v = ValueItem !k v

instance (Encodable k, Encodable v) => Encodable (ValueItem k v) where
  encode (ValueItem k v) = do
    encode k
    encode v
  decode = do
    k <- decode
    v <- decode
    return $ ValueItem k v

instance IsItem ValueItem where
  itemMinKey (ValueItem k _v) = k
  itemMaxKey (ValueItem k _v) = k
  saveItem _ _ = return ()
  loadItem _ = return
  itemLevel _ = 0
  sortItems store transform fold = treeFromMap . V.foldr' transformAndFold M.empty
    where
      transformAndFold (transform -> ValueItem k v) = M.alter (Just . maybe v (fold k v)) k
      treeFromMap = itemsToTree store . map (uncurry ValueItem) . M.toAscList

  checkItem _ _ = True
  debugPrintItem _ _ = return ()

-- | Item storing subnode.
data NodeItem q k v = NodeItem (Node q k v)

instance Encodable k => Encodable (NodeItem q k v) where
  encode (NodeItem Node
    { node_hash = h
    , node_minKey = k1
    , node_maxKey = k2
    }) = do
    encode h
    encode k1
    encode k2
  decode = do
    h <- decode
    k1 <- decode
    k2 <- decode
    return $ NodeItem Node
      { node_hash = h
      , node_minKey = k1
      , node_maxKey = k2
      , node_items = V.empty
      }

instance IsItem q => IsItem (NodeItem q) where
  itemMinKey (NodeItem Node
    { node_minKey = k
    }) = k
  itemMaxKey (NodeItem Node
    { node_maxKey = k
    }) = k
  saveItem s (NodeItem n) = save s n
  loadItem s (NodeItem Node
    { node_hash = NodeHash h
    , node_minKey = k1
    , node_maxKey = k2
    }) = do
    node@Node
      { node_hash = NodeHash nh
      , node_minKey = nk1
      , node_maxKey = nk2
      } <- load s h
    unless (h == nh && k1 == nk1 && k2 == nk2) $ fail "item does not correspond to loaded node"
    return $ NodeItem node
  itemLevel = f Proxy where
    f :: IsItem q => Proxy q -> Proxy (NodeItem q) -> Int
    f p _ = itemLevel p + 1
  sortItems store transform fold = mergeTrees store fold . V.map (\(NodeItem node) -> sortNode store transform fold node)
  checkItem (NodeItem n) = checkNode n
  debugPrintItem space (NodeItem n) = debugPrintNode space n


-- | Container for root node, hiding its concrete type.
data Tree k v where
  Tree :: IsItem q => Node q k v -> Tree k v
  EmptyTree :: Tree k v

-- | Build additional layers over sequence of nodes as needed.
closeNodes :: (Store s, IsTree q k v) => s -> [Node q k v] -> Tree k v
closeNodes store = \case
  [] -> EmptyTree
  [n] -> Tree $ reloadNode store n
  ns -> closeNodes store $ itemLayer store $ map NodeItem ns

itemLayer :: (Store s, IsTree q k v) => s -> [q k v] -> [Node q k v]
itemLayer store items = withSliceState $ \ss -> let
  f (i : is) = do
    z <- sliceBytes ss $ S.runPutLazy $ encodeItem i
    if z
      then return ([i], itemLayer store is)
      else do
        (fis, fns) <- f is
        return ((i : fis), fns)
  f [] = return ([], [])
  in do
    (is, ns) <- f items
    case is of
      [] -> return ns
      _ -> (: ns) <$> newNode store is

newNode :: (Store s, IsTree q k v) => s -> [q k v] -> IO (Node q k v)
newNode store (V.fromList -> items) = do
  save store n
  return n
  where
    k1 = itemMinKey (V.head items)
    k2 = itemMaxKey (V.last items)
    n = Node
      { node_hash = h
      , node_minKey = k1
      , node_maxKey = k2
      , node_items = items
      }
    h = nodeHash n

-- | Pack nodes to a tree.
itemsToTree :: (Store s, IsKeyValue k v) => s -> [ValueItem k v] -> Tree k v
itemsToTree store = closeNodes store . itemLayer store

-- | Load node from the store lazily, to allow GC of hierarchy.
reloadNode :: (Store s, IsTree q k v) => s -> Node q k v -> Node q k v
reloadNode store Node
  { node_hash = !(NodeHash h)
  } = unsafePerformIO $ load store h

checkTree :: IsKeyValue k v => Tree k v -> Bool
checkTree (Tree root) = checkNode root True where
checkTree EmptyTree = True

checkNode :: IsTree q k v => Node q k v -> Bool -> Bool
checkNode n@Node
  { node_hash = h
  , node_minKey = k1
  , node_maxKey = k2
  , node_items = items
  } isLast
  =  nodeHash n == h
  && not (V.null items)
  && ordered items
  && itemMinKey (V.head items) == k1
  && itemMaxKey (V.last items) == k2
  && V.ifoldl' (\ok i item -> ok && checkItem item (isLast && i >= V.length items - 1)) True items
  && sliceConsistent
  where
    ordered = snd . V.foldl' (\(maybeLastMaxKey, ok) item -> (Just $ itemMaxKey item, ok && maybe True (< itemMinKey item) maybeLastMaxKey)) (Nothing, True)
    sliceConsistent = withSliceState $ \ss -> let
      f ok i item = do
        z <- sliceBytes ss $ S.runPutLazy $ encodeItem item
        return $ ok && (if i >= V.length items - 1 then z || isLast else not z)
      in V.ifoldM' f True items

debugPrintTree :: (IsKeyValue k v, Show k) => Tree k v -> IO ()
debugPrintTree = \case
  Tree node -> debugPrintNode 0 node
  EmptyTree -> putStrLn "empty tree"

debugPrintNode :: (IsTree q k v, Show k) => Int -> Node q k v -> IO ()
debugPrintNode space n@Node
  { node_hash = NodeHash h
  , node_minKey = k1
  , node_maxKey = k2
  , node_items = items
  } = do
  debugPrintSpace space
  putStrLn $ storeKeyToString h <> (' ' : shows k1 (' ' : shows k2 (' ' : shows (BL.length $ S.runPutLazy $ encode n) "")))
  mapM_ (debugPrintItem (space + 1)) items

debugPrintSpace :: Int -> IO ()
debugPrintSpace space = putStr $ concat $ replicate space "  "
