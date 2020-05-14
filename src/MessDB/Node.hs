{-# LANGUAGE BangPatterns, ConstraintKinds, DefaultSignatures, FlexibleInstances, FunctionalDependencies, GADTs, GeneralizedNewtypeDeriving, LambdaCase, MultiParamTypeClasses, TypeFamilies, ViewPatterns #-}

module MessDB.Node
  ( Encodable(..)
  , NodeHash(..)
  , nodeHashToString
  , Store(..)
  , Persistable(..)
  , nodeHash
  , Node(..)
  , ValueItem(..)
  , Tree(..)
  , itemsToTree
  , reloadNode
  , checkTree
  , debugPrintTree
  ) where

import Control.Monad
import qualified Crypto.Hash as C
import qualified Data.ByteArray as BA
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import Data.Hashable(Hashable)
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import System.IO.Unsafe(unsafeInterleaveIO, unsafePerformIO)

import MessDB.Slicing

class Encodable a where
  -- | Encode only the object, without children.
  encode :: a -> S.Put
  default encode :: S.Serialize a => a -> S.Put
  encode = S.put

  -- | Decode only the object, without children.
  decode :: S.Get a
  default decode :: S.Serialize a => S.Get a
  decode = S.get

instance Encodable Int

newtype NodeHash = NodeHash BS.ShortByteString deriving (Eq, Hashable, Show)

instance Encodable NodeHash where
  encode (NodeHash bytes) = S.putShortByteString bytes
  decode = NodeHash <$> S.getShortByteString (C.hashDigestSize (undefined :: C.SHA256))

nodeHashToString :: NodeHash -> String
nodeHashToString (NodeHash h) = T.unpack $ T.decodeUtf8 $ BA.convertToBase BA.Base16 $ BS.fromShort h

class Store s where
  storeSave :: s -> NodeHash -> IO BL.ByteString -> IO ()
  storeLoad :: s -> NodeHash -> IO BL.ByteString

class Persistable a where
  -- | Save object, recursively including children into store.
  save :: Store s => s -> a -> IO ()
  -- | Load object with all children.
  load :: Store s => s -> NodeHash -> IO a

nodeHash :: IsItem k q => Node k q -> NodeHash
nodeHash a = NodeHash $ BS.toShort $ BA.convert h where
  h :: C.Digest C.SHA256
  h = C.hash $ S.runPut $ encode a

-- | Node of B-tree.
data Node k q = Node
  { node_hash :: NodeHash
  , node_minKey :: k
  , node_maxKey :: k
  , node_items :: (V.Vector q)
  }

instance IsItem k q => Encodable (Node k q) where
  encode Node
    { node_minKey = minKey
    , node_maxKey = maxKey
    , node_items = items
    } = do
    encode minKey
    encode maxKey
    S.put $ V.length items
    V.mapM_ encode items
  decode = do
    minKey <- decode
    maxKey <- decode
    itemsCount <- S.get
    items <- V.replicateM itemsCount decode
    let
      node = Node
        { node_hash = nodeHash node
        , node_minKey = minKey
        , node_maxKey = maxKey
        , node_items = items
        }
      in return node


instance IsItem k q => Persistable (Node k q) where
  save store node@Node
    { node_hash = hash
    , node_items = items
    } = storeSave store hash $ do
    mapM_ (saveItem store) items
    return $ S.runPutLazy $ encode node
  load store hash = do
    node@Node
      { node_hash = calculatedHash
      , node_items = items
      } <- either fail return . S.runGetLazy decode =<< storeLoad store hash
    unless (calculatedHash == hash) $ fail "node hash is wrong"
    return node
      { node_items = V.map (unsafePerformIO . loadItem store) items
      }

class (Ord k, Encodable k, Encodable q) => IsItem k q | q -> k where
  itemMinKey :: q -> k
  itemMaxKey :: q -> k
  type ItemFinalValue q :: *
  saveItem :: Store s => s -> q -> IO ()
  loadItem :: Store s => s -> q -> IO q
  checkItem :: q -> Bool -> Bool
  debugPrintItem :: Show k => Int -> q -> IO ()

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

instance (Ord k, Encodable k, Encodable v) => IsItem k (ValueItem k v) where
  itemMinKey (ValueItem k _v) = k
  itemMaxKey (ValueItem k _v) = k
  type ItemFinalValue (ValueItem k v) = v
  saveItem _ _ = return ()
  loadItem _ = return
  checkItem _ _ = True
  debugPrintItem _ _ = return ()

-- | Item storing subnode.
data NodeItem k q = NodeItem (Node k q)

instance Encodable k => Encodable (NodeItem k q) where
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

instance IsItem k q => IsItem k (NodeItem k q) where
  itemMinKey (NodeItem Node
    { node_minKey = k
    }) = k
  itemMaxKey (NodeItem Node
    { node_maxKey = k
    }) = k
  type ItemFinalValue (NodeItem k q) = ItemFinalValue q
  saveItem s (NodeItem n) = save s n
  loadItem s (NodeItem Node
    { node_hash = h
    , node_minKey = k1
    , node_maxKey = k2
    }) = do
    node@Node
      { node_hash = nh
      , node_minKey = nk1
      , node_maxKey = nk2
      } <- load s h
    unless (h == nh && k1 == nk1 && k2 == nk2) $ fail "item does not correspond to loaded node"
    return $ NodeItem node
  checkItem (NodeItem n) = checkNode n
  debugPrintItem space (NodeItem n) = debugPrintNode space n


-- | Container for root node, hiding its concrete type.
data Tree k v where
  Tree :: (IsItem k q, ItemFinalValue q ~ v) => Node k q -> Tree k v
  EmptyTree :: Tree k v

type IsKeyValue k v = (Ord k, Encodable k, Encodable v)

-- | Build additional layers over sequence of nodes as needed.
closeNodes :: (Store s, IsItem k q, ItemFinalValue q ~ v) => s -> [Node k q] -> IO (Tree k v)
closeNodes store = \case
  [] -> return EmptyTree
  [n] -> Tree <$> reloadNode store n
  ns -> closeNodes store $ itemLayer store $ map NodeItem ns

itemLayer :: (Store s, IsItem k q) => s -> [q] -> [Node k q]
itemLayer store items = withSliceState $ \ss -> let
  f (i : is) = do
    z <- sliceBytes ss $ S.runPutLazy $ encode i
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

newNode :: (Store s, IsItem k q) => s -> [q] -> IO (Node k q)
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
itemsToTree :: (Store s, IsKeyValue k v) => s -> [ValueItem k v] -> IO (Tree k v)
itemsToTree store = closeNodes store . itemLayer store

-- | Load node from the store lazily, to allow GC of hierarchy.
reloadNode :: (Store s, IsItem k q) => s -> Node k q -> IO (Node k q)
reloadNode store Node
  { node_hash = !h
  } = unsafeInterleaveIO $ load store h

checkTree :: Tree k v -> Bool
checkTree (Tree root) = checkNode root True where
checkTree EmptyTree = True

checkNode :: IsItem k q => Node k q -> Bool -> Bool
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
        z <- sliceBytes ss $ S.runPutLazy $ encode item
        return $ ok && (if i >= V.length items - 1 then z || isLast else not z)
      in V.ifoldM' f True items

debugPrintTree :: (IsKeyValue k v, Show k) => Tree k v -> IO ()
debugPrintTree = \case
  Tree node -> debugPrintNode 0 node
  EmptyTree -> putStrLn "empty tree"

debugPrintNode :: (IsItem k q, Show k) => Int -> Node k q -> IO ()
debugPrintNode space n@Node
  { node_hash = h
  , node_minKey = k1
  , node_maxKey = k2
  , node_items = items
  } = do
  debugPrintSpace space
  putStrLn $ nodeHashToString h <> (' ' : shows k1 (' ' : shows k2 (' ' : shows (BL.length $ S.runPutLazy $ encode n) "")))
  mapM_ (debugPrintItem (space + 1)) items

debugPrintSpace :: Int -> IO ()
debugPrintSpace space = putStr $ concat $ replicate space "  "
