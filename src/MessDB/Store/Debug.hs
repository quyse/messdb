module MessDB.Store.Debug
  ( DebugStore(..)
  ) where

import MessDB.Store

newtype DebugStore s = DebugStore s

instance Store s => Store (DebugStore s) where
  storeSave (DebugStore store) key io = do
    putStrLn $ "Saving " <> keyStr <> "..."
    storeSave store key $ do
      r <- io
      putStrLn $ "Saving " <> keyStr <> ": saved."
      return r
    where
      keyStr = storeKeyToString key

  storeLoad (DebugStore store) key = do
    putStrLn $ "Loading " <> keyStr <> "..."
    storeLoad store key
    where
      keyStr = storeKeyToString key

instance MemoStore s => MemoStore (DebugStore s) where
  memoStoreCache (DebugStore store) key io = do
    putStrLn $ "Caching " <> keyStr <> "..."
    r <- memoStoreCache store key $ do
      putStrLn $ "Caching " <> keyStr <> ": building..."
      (mr, ud) <- io
      putStrLn $ case mr of
        Just _ -> "Caching " <> keyStr <> ": built, will be stored."
        Nothing -> "Caching " <> keyStr <> ": built, will not be stored."
      return (mr, ud)
    putStrLn $ "Caching " <> keyStr <> ": done."
    return r
    where
      keyStr = storeKeyToString key
