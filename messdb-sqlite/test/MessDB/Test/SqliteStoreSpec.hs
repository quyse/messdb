{-# LANGUAGE OverloadedStrings #-}

module MessDB.Test.SqliteStoreSpec
  ( spec
  ) where

import Control.Concurrent.STM
import Control.Monad
import qualified Data.ByteString.Lazy as BL
import System.IO.Temp
import Test.Hspec

import MessDB.Store
import MessDB.Store.Sqlite

spec :: Spec
spec = describe "SqliteStore" $ do
  it "simple ops" $ withTempStore $ \store -> do
    let
      key1 = StoreKey "abc"
      key2 = StoreKey "def"
      value1 = "ABC"
      value2 = "DEF"
    shouldSave store True key1 (return value1)
    shouldSave store False key1 (return value1)
    shouldSave store False key1 (return value2)
    True <- (== value1) <$> storeLoad store key1
    shouldSave store True key2 (return value2)
    True <- (== value2) <$> storeLoad store key2
    return ()

shouldSave :: Store s => s -> Bool -> StoreKey -> IO BL.ByteString -> IO ()
shouldSave store should key io = do
  calledVar <- newTVarIO False
  storeSave store key $ do
    atomically $ writeTVar calledVar True
    io
  called <- readTVarIO calledVar
  unless (called == should) $ fail $ "shouldSave failed: unexpectedly " <> if called then "saved" else "not saved"

withTempStore :: (SqliteStore -> IO a) -> IO a
withTempStore io = withSystemTempDirectory "messdb-sqlite-test-temp-store" $ \path ->
  withSqliteStore (path <> "/store.db") io
