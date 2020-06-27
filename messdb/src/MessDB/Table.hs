{-# LANGUAGE ViewPatterns #-}

module MessDB.Table
  ( Table(..)
  , TableTransformFunc(..)
  , tableTransformFunc
  , TableFoldFunc(..)
  , tableFoldFunc
  , emptyTable
  , singletonTable
  , mergeTables
  , sortTable
  , tableToRows
  , tableFromRows
  , printTable
  , tableFoldToLast
  ) where

import qualified Data.Vector as V

import MessDB.Store
import MessDB.Table.Types
import MessDB.Trie

newtype Table k v = Table
  { unTable :: Trie
  }

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
emptyTable = Table EmptyTrie

singletonTable :: (TableKey k, TableValue v) => k -> v -> Table k v
singletonTable key value = Table $ singletonTrie (encodeTableKey key) (encodeTableValue value)

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

printTable :: (TableKey k, TableValue v, Show k, Show v) => Table k v -> IO ()
printTable = mapM_ print . tableToRows

tableFoldToLast :: TableFoldFunc k v
tableFoldToLast = TableFoldFunc foldToLast
