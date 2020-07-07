{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, OverloadedLists, ViewPatterns #-}

module MessDB.Table
  ( Table(..)
  , tableHash
  , TableRef(..)
  , refTable
  , resolveTableRef
  , TableKey(..)
  , TableValue
  , TableTransformFunc(..)
  , tableTransformFunc
  , TableFoldFunc(..)
  , tableFoldFunc
  , emptyTable
  , singletonTable
  , syncTable
  , mergeTables
  , sortTable
  , rangeFilterTable
  , tableToRows
  , tableFromRows
  , tableInsert
  , printTable
  , tableFoldToLast
  , TableKeyRange(..)
  , TableKeyRangeEnd(..)
  , tableKeyRangeSingleton
  ) where

import qualified Data.Serialize as S
import qualified Data.Vector as V

import MessDB.Store
import MessDB.Table.Types
import MessDB.Trie

newtype Table k v = Table
  { unTable :: Trie
  } deriving Persistable

tableHash :: Table k v -> StoreKey
tableHash = trieHash . unTable

-- | Table ref is a reference to a table.
newtype TableRef k v = TableRef
  { unTableRef :: StoreKey
  } deriving Encodable

instance S.Serialize (TableRef k v) where
  put = encode
  get = decode

-- | Create ref to table.
refTable :: Table k v -> TableRef k v
refTable = TableRef . tableHash

-- | Load table by ref.
resolveTableRef :: Store s => s -> TableRef k v -> IO (Table k v)
resolveTableRef store = fmap Table . load store . unTableRef

newtype TableTransformFunc k v k' v' = TableTransformFunc TransformFunc

tableTransformFunc :: (TableKey k, TableValue v, TableKey k', TableValue v') => FuncKey -> (k -> v -> (k', v')) -> TableTransformFunc k v k' v'
tableTransformFunc funcKey f = TableTransformFunc Func
  { func_key = funcKey
  , func_func = \(decodeTableKey -> k) (decodeTableValue -> v) -> let
    (encodeTableKey -> k', encodeTableValue -> v') = f k v
    in (k', v')
  }

newtype TableFoldFunc k v = TableFoldFunc FoldFunc

tableFoldFunc :: (TableKey k, TableValue v) => FuncKey -> (k -> v -> v -> v) -> TableFoldFunc k v
tableFoldFunc funcKey f = TableFoldFunc Func
  { func_key = funcKey
  , func_func = \(decodeTableKey -> k) (decodeTableValue -> v1) (decodeTableValue -> v2) -> encodeTableValue (f k v1 v2)
  }

emptyTable :: Table k v
emptyTable = Table emptyTrie

singletonTable :: (TableKey k, TableValue v) => k -> v -> Table k v
singletonTable key value = Table $ singletonTrie (encodeTableKey key) (encodeTableValue value)

syncTable :: Store s => s -> Table k v -> Table k v
syncTable store (Table trie) = Table $ syncTrie store trie

mergeTables
  :: (TableKey k, TableValue v, Store s, MemoStore ms)
  => s -> ms
  -> TableFoldFunc k v
  -> V.Vector (Table k v)
  -> Table k v
mergeTables store memoStore
  (TableFoldFunc foldFunc)
  (V.map unTable -> tries)
  = Table $ mergeTries store memoStore foldFunc tries

sortTable
  :: (TableKey k, TableValue v, TableKey k', TableValue v', Store s, MemoStore ms)
  => s -> ms
  -> TableTransformFunc k v k' v'
  -> TableFoldFunc k' v'
  -> Table k v
  -> Table k' v'
sortTable store memoStore
  (TableTransformFunc transformFunc)
  (TableFoldFunc foldFunc)
  (Table trie)
  = Table $ sortTrie store memoStore transformFunc foldFunc trie

rangeFilterTable
  :: (TableKey k, Store s, MemoStore ms)
  => s -> ms
  -> TableKeyRange k
  -> Table k v
  -> Table k v
rangeFilterTable store memoStore (tableKeyRangeToKeyRange -> keyRange) (Table trie)
  = Table $ rangeFilterTrie store memoStore keyRange trie

tableToRows :: (TableKey k, TableValue v) => Table k v -> [(k, v)]
tableToRows (Table trie) = map (\(k, v) -> (decodeTableKey k, decodeTableValue v)) $ trieToItems trie

tableFromRows
  :: (TableKey k, TableValue v, Store s, MemoStore ms)
  => s -> ms
  -> [(k, v)]
  -> Table k v
tableFromRows store memoStore = tableFromTables . map (\(k, v) -> singletonTable k v) where
  tableFromTables [] = emptyTable
  tableFromTables [t] = t
  tableFromTables ts = tableFromTables $ map (mergeTables store memoStore tableFoldToLast) $ splitIntoGroups ts
  -- split into groups
  splitIntoGroups [] = []
  splitIntoGroups tables = let
    (a, b) = splitAt groupSize tables
    in V.fromList a : splitIntoGroups b
  -- number of rows/tables per group
  groupSize = 1024

tableInsert :: (Store s, MemoStore ms, TableKey k, TableValue v) => s -> ms -> k -> v -> Table k v -> Table k v
tableInsert store memoStore k v table = mergeTables store memoStore tableFoldToLast [table, (singletonTable k v)]

printTable :: (TableKey k, TableValue v, Show k, Show v) => Table k v -> IO ()
printTable = mapM_ print . tableToRows

tableFoldToLast :: TableFoldFunc k v
tableFoldToLast = TableFoldFunc foldToLast

data TableKeyRange k = TableKeyRange !(TableKeyRangeEnd k) !(TableKeyRangeEnd k)

data TableKeyRangeEnd k
  = TableKeyRangeEnd_inclusive !k
  | TableKeyRangeEnd_exclusive !k
  | TableKeyRangeEnd_infinite

tableKeyRangeSingleton :: k -> TableKeyRange k
tableKeyRangeSingleton key = TableKeyRange (TableKeyRangeEnd_inclusive key) (TableKeyRangeEnd_inclusive key)

tableKeyRangeToKeyRange :: TableKey k => TableKeyRange k -> KeyRange
tableKeyRangeToKeyRange (TableKeyRange lowerEnd upperEnd) = KeyRange (tableKeyRangeEndToKeyRangeEnd lowerEnd) (tableKeyRangeEndToKeyRangeEnd upperEnd)

tableKeyRangeEndToKeyRangeEnd :: TableKey k => TableKeyRangeEnd k -> KeyRangeEnd
tableKeyRangeEndToKeyRangeEnd = \case
  TableKeyRangeEnd_inclusive key -> KeyRangeEnd_inclusive (encodeTableKey key)
  TableKeyRangeEnd_exclusive key -> KeyRangeEnd_exclusive (encodeTableKey key)
  TableKeyRangeEnd_infinite -> KeyRangeEnd_infinite
