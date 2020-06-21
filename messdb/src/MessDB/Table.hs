{-# LANGUAGE ViewPatterns #-}

module MessDB.Table
  ( Table(..)
  , TableTransformFunc(..)
  , tableTransformFunc
  , TableFoldFunc(..)
  , tableFoldFunc
  , sortTable
  , printTable
  ) where

import MessDB.Store
import MessDB.Table.Types
import MessDB.Trie

newtype Table k v = Table Trie

newtype TableTransformFunc k v k' v' = TableTransformFunc TransformFunc

tableTransformFunc :: (TableKey k, TableValue v, TableKey k', TableValue v') => FuncKey -> (k -> v -> (k', v')) -> TableTransformFunc k v k' v'
tableTransformFunc funcKey f = TableTransformFunc Func
  { func_key = funcKey
  , func_func = \(decodeKey -> k) (decodeValue -> v) -> let
    (encodeKey -> k', encodeValue -> v') = f k v
    in (k', v')
  }

newtype TableFoldFunc k v = TableFoldFunc FoldFunc

tableFoldFunc :: (TableKey k, TableValue v) => FuncKey -> (k -> v -> v -> v) -> TableFoldFunc k v
tableFoldFunc funcKey f = TableFoldFunc Func
  { func_key = funcKey
  , func_func = \(decodeKey -> k) (decodeValue -> v1) (decodeValue -> v2) -> encodeValue (f k v1 v2)
  }

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

printTable :: (TableKey k, TableValue v, Show k, Show v) => Table k v -> IO ()
printTable (Table trie) = mapM_ printItem $ trieToItems trie where
  printItem (k, v) = putStrLn $ shows k $ ' ' : show v
