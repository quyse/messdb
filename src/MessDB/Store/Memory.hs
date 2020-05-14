module MessDB.Store.Memory
  ( MemoryStore(..)
  , newMemoryStoreIO
  ) where

import Control.Concurrent.STM
import Control.Monad
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM

import MessDB.Node

newtype MemoryStore = MemoryStore (TVar (HM.HashMap NodeHash BL.ByteString))

newMemoryStoreIO :: IO MemoryStore
newMemoryStoreIO = MemoryStore <$> newTVarIO HM.empty

instance Store MemoryStore where
  storeSave (MemoryStore var) hash io = do
    exists <- HM.member hash <$> readTVarIO var
    unless exists $
      atomically . modifyTVar' var . HM.insert hash =<< io

  storeLoad (MemoryStore var) hash =
    maybe (fail "memory store: missing hash") return . HM.lookup hash =<< readTVarIO var
