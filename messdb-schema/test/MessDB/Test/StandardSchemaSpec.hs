{-# LANGUAGE DataKinds, OverloadedLists, OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module MessDB.Test.StandardSchemaSpec
  ( spec
  ) where

import qualified Data.Csv as Csv
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Text as T
import Test.Hspec
import Test.QuickCheck
import Test.QuickCheck.Arbitrary.Generic

import MessDB.Table.Row

spec :: Spec
spec = describe "StandardSchema" $ do
  it "header" $ property $ Csv.headerOrder (undefined :: TestRow) == ["a", "b", "c"]
  schemaCsvSpec (Proxy :: Proxy TestRow)

schemaCsvSpec :: (Arbitrary a, Eq a, Csv.FromRecord a, Csv.ToRecord a) => Proxy a -> Spec
schemaCsvSpec p = describe "csv" $ do
  it "fromRecord is inverse of toRecord" $ property $ do
    a <- genByProxy p
    return $ Csv.runParser (Csv.parseRecord (Csv.toRecord a)) == Right a
  it "parse with header" $ property $ Csv.runParser (fromJust (csvParseRecordWithHeader ["b", "d", "a", "c"]) ["1", "2", "3", "4"]) == Right (Row 3 (Row "1" (Row 4 ())) :: TestRow)

genByProxy :: Arbitrary a => Proxy a -> Gen a
genByProxy Proxy = arbitrary

instance (Arbitrary a, Arbitrary b) => Arbitrary (Row n a b) where
  arbitrary = genericArbitrary

instance Arbitrary T.Text where
  arbitrary = T.pack <$> arbitrary

type TestRow = Row "a" Int64 (Row "b" T.Text (Row "c" Float ()))
