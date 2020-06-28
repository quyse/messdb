module MessDB.Store.Stat
  ( StatStore(..)
  , newStatStoreIO
  , StoreStats(..)
  , zeroStoreStats
  , diffStoreStats
  , withStoreStats
  ) where

import Control.Concurrent.STM

import MessDB.Store

-- | Wrapper around 'Store' and/or 'MemoStore', calculating statistics.
data StatStore s = StatStore
  { statStore_store :: !s
  , statStore_statsVar :: !(TVar StoreStats)
  }

-- | Create 'StatStore'.
newStatStoreIO :: s -> IO (StatStore s)
newStatStoreIO store = do
  statsVar <- newTVarIO zeroStoreStats
  return StatStore
    { statStore_store = store
    , statStore_statsVar = statsVar
    }

data StoreStats = StoreStats
  { storeStats_loadCount :: {-# UNPACK #-} !Int
  , storeStats_saveCount :: {-# UNPACK #-} !Int
  , storeStats_saveActionCount :: {-# UNPACK #-} !Int
  , storeStats_cacheCount :: {-# UNPACK #-} !Int
  , storeStats_cacheActionCount :: {-# UNPACK #-} !Int
  , storeStats_cacheNewValueCount :: {-# UNPACK #-} !Int
  } deriving Show

zeroStoreStats :: StoreStats
zeroStoreStats = StoreStats
  { storeStats_loadCount = 0
  , storeStats_saveCount = 0
  , storeStats_saveActionCount = 0
  , storeStats_cacheCount = 0
  , storeStats_cacheActionCount = 0
  , storeStats_cacheNewValueCount = 0
  }

-- | Diffs before and after.
diffStoreStats :: StoreStats -> StoreStats -> StoreStats
diffStoreStats StoreStats
  { storeStats_loadCount = loadCountBefore
  , storeStats_saveCount = saveCountBefore
  , storeStats_saveActionCount = saveActionCountBefore
  , storeStats_cacheCount = cacheCountBefore
  , storeStats_cacheActionCount = cacheActionCountBefore
  , storeStats_cacheNewValueCount = cacheNewValueCountBefore
  } StoreStats
  { storeStats_loadCount = loadCountAfter
  , storeStats_saveCount = saveCountAfter
  , storeStats_saveActionCount = saveActionCountAfter
  , storeStats_cacheCount = cacheCountAfter
  , storeStats_cacheActionCount = cacheActionCountAfter
  , storeStats_cacheNewValueCount = cacheNewValueCountAfter
  } = StoreStats
  { storeStats_loadCount = loadCountAfter - loadCountBefore
  , storeStats_saveCount = saveCountAfter - saveCountBefore
  , storeStats_saveActionCount = saveActionCountAfter - saveActionCountBefore
  , storeStats_cacheCount = cacheCountAfter - cacheCountBefore
  , storeStats_cacheActionCount = cacheActionCountAfter - cacheActionCountBefore
  , storeStats_cacheNewValueCount = cacheNewValueCountAfter - cacheNewValueCountBefore
  }

withStoreStats :: StatStore s -> IO a -> IO (StoreStats, a)
withStoreStats StatStore
  { statStore_statsVar = statsVar
  } io = do
  statsBefore <- readTVarIO statsVar
  r <- io
  statsAfter <- readTVarIO statsVar
  return (diffStoreStats statsBefore statsAfter, r)

instance Store s => Store (StatStore s) where
  storeSave StatStore
    { statStore_store = store
    , statStore_statsVar = statsVar
    } key io = do
    atomically $ modifyTVar' statsVar $ \stats@StoreStats
      { storeStats_saveCount = saveCount
      } -> stats
      { storeStats_saveCount = saveCount + 1
      }
    storeSave store key $ do
      atomically $ modifyTVar' statsVar $ \stats@StoreStats
        { storeStats_saveActionCount = saveActionCount
        } -> stats
        { storeStats_saveActionCount = saveActionCount + 1
        }
      io

  storeLoad StatStore
    { statStore_store = store
    , statStore_statsVar = statsVar
    } key = do
    atomically $ modifyTVar' statsVar $ \stats@StoreStats
      { storeStats_loadCount = loadCount
      } -> stats
      { storeStats_loadCount = loadCount + 1
      }
    storeLoad store key

instance MemoStore s => MemoStore (StatStore s) where
  memoStoreCache StatStore
    { statStore_store = store
    , statStore_statsVar = statsVar
    } key io = do
    atomically $ modifyTVar' statsVar $ \stats@StoreStats
      { storeStats_cacheCount = cacheCount
      } -> stats
      { storeStats_cacheCount = cacheCount + 1
      }

    memoStoreCache store key $ do
      atomically $ modifyTVar' statsVar $ \stats@StoreStats
        { storeStats_cacheActionCount = cacheActionCount
        } -> stats
        { storeStats_cacheActionCount = cacheActionCount + 1
        }
      r@(maybeNewValue, _) <- io
      case maybeNewValue of
        Just _ -> atomically $ modifyTVar' statsVar $ \stats@StoreStats
          { storeStats_cacheNewValueCount = cacheNewValueCount
          } -> stats
          { storeStats_cacheNewValueCount = cacheNewValueCount + 1
          }
        Nothing -> return ()
      return r
