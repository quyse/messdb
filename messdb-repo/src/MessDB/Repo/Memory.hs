module MessDB.Repo.Memory
  ( MemoryRepo(..)
  , newMemoryRepoIO
  ) where

import Control.Concurrent.STM

import MessDB.Repo
import MessDB.Store

newtype MemoryRepo = MemoryRepo (TVar StoreKey)

newMemoryRepoIO :: StoreKey -> IO MemoryRepo
newMemoryRepoIO = fmap MemoryRepo . newTVarIO

instance RepoStore MemoryRepo where
  repoStoreGetRoot (MemoryRepo rootVar) = readTVarIO rootVar
  repoStoreSetRoot (MemoryRepo rootVar) = atomically . writeTVar rootVar
