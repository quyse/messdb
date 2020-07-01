{-# LANGUAGE OverloadedStrings #-}

module MessDB.Test.LmdbStoreSpec
  ( spec
  ) where

import Control.Concurrent.STM
import Control.Monad
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import System.IO.Temp
import Test.Hspec

import MessDB.Store
import MessDB.Store.Lmdb

spec :: Spec
spec = describe "LmdbStore" $ do
  it "simple ops" $ withTempStore $ \store -> do
    let
      key1 = StoreKey "abc"
      key2 = StoreKey "def"
    shouldSave store True key1 (return "ABC")
    shouldSave store False key1 (return "ABC")
    shouldSave store False key1 (return "DEF")
    True <- (== "ABC") <$> storeLoad store key1
    shouldSave store True key2 (return "DEF")
    True <- (== "DEF") <$> storeLoad store key2
    testMemoCache store key1 (Just "ABC") Nothing
    testMemoCache store key1 (Just "ABC") (Just "ABC")
    testMemoCache store key1 Nothing (Just "ABC")
    testMemoCache store key2 Nothing Nothing
    testMemoCache store key2 Nothing Nothing
    testMemoCache store key2 (Just "DEF") Nothing
    testMemoCache store key2 (Just "DEF") (Just "DEF")
    testMemoCache store key2 Nothing (Just "DEF")

shouldSave :: Store s => s -> Bool -> StoreKey -> IO BL.ByteString -> IO ()
shouldSave store should key io = do
  calledVar <- newTVarIO False
  storeSave store key $ do
    atomically $ writeTVar calledVar True
    io
  called <- readTVarIO calledVar
  unless (called == should) $ fail $ "shouldSave failed: unexpectedly " <> if called then "saved" else "not saved"

testMemoCache :: MemoStore s => s -> StoreKey -> Maybe BS.ShortByteString -> Maybe BS.ShortByteString -> IO ()
testMemoCache store key maybeSetValue maybeResultValue = do
  r <- memoStoreCache store key (return (maybeSetValue, ()))
  case r of
    Left resultValue -> case maybeResultValue of
      Just expectedResultValue -> unless (resultValue == expectedResultValue) $ fail "testMemoCache: wrong result"
      Nothing -> fail "testMemoCache: unexectedly returned result"
    Right () -> unless (maybeResultValue == Nothing) $ fail "testMemoCache: unexpectedly missing result"

withTempStore :: (LmdbStore -> IO a) -> IO a
withTempStore io = withSystemTempDirectory "messdb-lmdb-test-temp-store" $ \path ->
  withLmdbStore (path <> "/store.db") io
