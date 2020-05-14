module MessDB.Test.NodeSpec
  ( spec
  ) where

import System.IO.Unsafe(unsafePerformIO)
import Test.Hspec

import MessDB.Node
import MessDB.Store.Memory

spec :: Spec
spec = describe "Node" $ do
  it "Tree should be correct" $ withTempStore $ \store ->
    checkTree <$> itemsToTree store [ValueItem k k | k <- [1 :: Int .. 1000000]]
  it "Unordered tree should be incorrect" $ withTempStore $ \store ->
    not . checkTree <$> itemsToTree store [ValueItem k k | k <- [1 :: Int .. 1000] ++ [1 :: Int .. 1000]]

{-# NOINLINE withTempStore #-}
withTempStore :: (MemoryStore -> IO a) -> a
withTempStore f = unsafePerformIO $ f =<< newMemoryStoreIO
