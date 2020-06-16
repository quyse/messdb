module MessDB.Store.Memory
  ( MemoryStore(..)
  , newMemoryStoreIO
  ) where

import Control.Concurrent.STM
import Control.Monad
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import qualified Data.HashMap.Strict as HM

import MessDB.Store

newtype MemoryStore = MemoryStore (TVar (HM.HashMap StoreKey BL.ByteString))

newMemoryStoreIO :: IO MemoryStore
newMemoryStoreIO = MemoryStore <$> newTVarIO HM.empty

instance Store MemoryStore where
  storeSave (MemoryStore var) key io = do
    exists <- HM.member key <$> readTVarIO var
    unless exists $
      atomically . modifyTVar' var . HM.insert key =<< io

  storeLoad (MemoryStore var) key =
    maybe (fail "memory store: missing key") return . HM.lookup key =<< readTVarIO var

instance MemoStore MemoryStore where
  memoStoreCache (MemoryStore var) key io = do
    maybeValue <- HM.lookup key <$> readTVarIO var
    case maybeValue of
      Just value -> return $ Left $ BS.toShort $ BL.toStrict value
      Nothing -> do
        (maybeNewValue, userData) <- io
        case maybeNewValue of
          Just newValue -> atomically $ modifyTVar' var $ HM.insert key $ BL.fromStrict $ BS.fromShort newValue
          Nothing -> return ()
        return $ Right userData
