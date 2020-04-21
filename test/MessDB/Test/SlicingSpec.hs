module MessDB.Test.SlicingSpec
  ( spec
  ) where

import Control.Monad
import qualified Data.ByteString.Lazy as BL
import Test.Hspec
import Test.QuickCheck

import MessDB.Slicing

spec :: Spec
spec = describe "Slicing" $ do
  it "Rollsum depends only on last 64 bytes" $ property $ do
    a <- genBytes 1 128
    b <- genBytes 1 128
    c <- genBytes 64 64
    return $ sliceDigest (a <> c) == sliceDigest (b <> c)

genBytes :: Int -> Int -> Gen BL.ByteString
genBytes a b = do
  len <- choose (a, b)
  BL.pack <$> replicateM len arbitrary
