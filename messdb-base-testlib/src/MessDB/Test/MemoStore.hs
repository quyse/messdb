{-# LANGUAGE OverloadedStrings, RankNTypes #-}

module MessDB.Test.MemoStore
  ( memoStoreTestSpec
  ) where

import Control.Monad
import qualified Data.ByteString.Short as BS
import Test.Hspec

import MessDB.Store

memoStoreTestSpec :: MemoStore ms => (forall a. (ms -> IO a) -> IO a) -> Spec
memoStoreTestSpec withMemoStore = describe "memo store test" $ do
  it "simple ops" $ withMemoStore $ \memoStore -> do
    let
      key1 = StoreKey "abc"
      key2 = StoreKey "def"
    testMemoCache memoStore key1 (Just "ABC") Nothing
    testMemoCache memoStore key1 (Just "ABC") (Just "ABC")
    testMemoCache memoStore key1 Nothing (Just "ABC")
    testMemoCache memoStore key2 Nothing Nothing
    testMemoCache memoStore key2 Nothing Nothing
    testMemoCache memoStore key2 (Just "DEF") Nothing
    testMemoCache memoStore key2 (Just "DEF") (Just "DEF")
    testMemoCache memoStore key2 Nothing (Just "DEF")

testMemoCache :: MemoStore s => s -> StoreKey -> Maybe BS.ShortByteString -> Maybe BS.ShortByteString -> IO ()
testMemoCache memoStore key maybeSetValue maybeResultValue = do
  r <- memoStoreCache memoStore key (return (maybeSetValue, ()))
  case r of
    Left resultValue -> case maybeResultValue of
      Just expectedResultValue -> unless (resultValue == expectedResultValue) $ fail "testMemoCache: wrong result"
      Nothing -> fail "testMemoCache: unexectedly returned result"
    Right () -> unless (maybeResultValue == Nothing) $ fail "testMemoCache: unexpectedly missing result"
