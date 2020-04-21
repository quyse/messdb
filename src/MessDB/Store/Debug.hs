module MessDB.Store.Debug
  ( DebugStore(..)
  ) where

import MessDB.Node

newtype DebugStore s = DebugStore s

instance Store s => Store (DebugStore s) where
  storeSave (DebugStore store) hash io = do
    putStrLn $ "Saving " <> hashStr <> "..."
    storeSave store hash $ do
      r <- io
      putStrLn $ "Saving " <> hashStr <> ": saved."
      return r
    where
      hashStr = nodeHashToString hash

  storeLoad (DebugStore store) hash = do
    putStrLn $ "Loading " <> hashStr <> "..."
    storeLoad store hash
    where
      hashStr = nodeHashToString hash
