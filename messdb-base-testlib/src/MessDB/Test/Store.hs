{-# LANGUAGE OverloadedStrings, RankNTypes #-}

module MessDB.Test.Store
  ( storeTestSpec
  ) where

import Control.Concurrent.STM
import Control.Monad
import qualified Data.ByteString.Lazy as BL
import Test.Hspec

import MessDB.Store

storeTestSpec :: Store s => (forall a. (s -> IO a) -> IO a) -> Spec
storeTestSpec withStore = describe "store test" $ do
  it "simple ops" $ withStore $ \store -> do
    let
      key1 = StoreKey "abc"
      key2 = StoreKey "def"
    shouldSave store True key1 (return "ABC")
    shouldSave store False key1 (return "ABC")
    shouldSave store False key1 (return "DEF")
    True <- (== "ABC") <$> storeLoad store key1
    shouldSave store True key2 (return "DEF")
    True <- (== "DEF") <$> storeLoad store key2
    return ()

shouldSave :: Store s => s -> Bool -> StoreKey -> IO BL.ByteString -> IO ()
shouldSave store should key io = do
  calledVar <- newTVarIO False
  storeSave store key $ do
    atomically $ writeTVar calledVar True
    io
  called <- readTVarIO calledVar
  unless (called == should) $ fail $ "shouldSave failed: unexpectedly " <> if called then "saved" else "not saved"
