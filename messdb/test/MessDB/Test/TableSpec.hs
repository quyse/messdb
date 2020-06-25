{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module MessDB.Test.TableSpec
  ( spec
  ) where

import qualified Data.ByteString as B
import Data.Int
import Data.Proxy
import qualified Data.Serialize as S
import qualified Data.Text as T
import Data.Word
import GHC.Generics(Generic)
import Test.Hspec
import Test.QuickCheck
import Test.QuickCheck.Arbitrary.Generic

import MessDB.Table.Types

spec :: Spec
spec = describe "Table" $ do
  tableKeySpec "Int64" (Proxy :: Proxy Int64)
  tableKeySpec "Int32" (Proxy :: Proxy Int32)
  tableKeySpec "Int16" (Proxy :: Proxy Int16)
  tableKeySpec "Int8" (Proxy :: Proxy Int8)
  tableKeySpec "Word64" (Proxy :: Proxy Word64)
  tableKeySpec "Word32" (Proxy :: Proxy Word32)
  tableKeySpec "Word16" (Proxy :: Proxy Word16)
  tableKeySpec "Word8" (Proxy :: Proxy Word8)
  tableKeySpec "ByteString" (Proxy :: Proxy B.ByteString)
  tableKeySpec "Text" (Proxy :: Proxy T.Text)
  tableKeySpec "Float" (Proxy :: Proxy Float)
  tableKeySpec "Double" (Proxy :: Proxy Double)

tableKeySpec :: (TableKey k, Arbitrary k) => String -> Proxy k -> Spec
tableKeySpec name p = describe name $ do
  describe "scalar" $ f p arbitrary
  describe "pair" $ f2 p arbitrary
  describe "triple" $ f3 p arbitrary
  describe "struct1" $ fs1 p arbitrary
  describe "struct2" $ fs2 p arbitrary
  where
  f :: (TableKey k, Arbitrary k) => Proxy k -> Gen k -> Spec
  f Proxy gen = do
    it "decode is inverse of encode" $ property $ do
      k <- gen
      let
        Right k' = S.runGet getTableKey $ S.runPut $ putTableKey k
      return $ k == k'
    it "correct comparison" $ property $ do
      a <- gen
      b <- gen
      let
        a' = S.runPut $ putTableKey a
        b' = S.runPut $ putTableKey b
      return $ compare a b == compare a' b'

  f2 :: (TableKey k, Arbitrary k) => Proxy k -> Gen (k, k) -> Spec
  f2 Proxy = f Proxy

  f3 :: (TableKey k, Arbitrary k) => Proxy k -> Gen (k, k, k) -> Spec
  f3 Proxy = f Proxy

  fs1 :: (TableKey k, Arbitrary k) => Proxy k -> Gen (Struct1 k) -> Spec
  fs1 Proxy = f Proxy

  fs2 :: (TableKey k, Arbitrary k) => Proxy k -> Gen (Struct2 k) -> Spec
  fs2 Proxy = f Proxy

instance Arbitrary B.ByteString where
  arbitrary = B.pack <$> arbitrary

instance Arbitrary T.Text where
  arbitrary = T.pack <$> arbitrary

data Struct1 a
  = Struct1_1
    { s1_1 :: a
    }
  | Struct1_2
    { s1_1 :: a
    , s1_2 :: a
    }
  | Struct1_3
    { s1_1 :: a
    , s1_2 :: a
    , s1_3 :: a
    }
  deriving (Eq, Ord, Generic)

instance Arbitrary a => Arbitrary (Struct1 a) where
  arbitrary = genericArbitrary
instance TableKey a => TableKey (Struct1 a)

data Struct2 a
  = Struct2_1
    { s2_1 :: a
    , s2_2 :: a
    }
  | Struct2_2
    { s2_1 :: a
    , s2_2 :: a
    }
  deriving (Eq, Ord, Generic)

instance Arbitrary a => Arbitrary (Struct2 a) where
  arbitrary = genericArbitrary
instance TableKey a => TableKey (Struct2 a)
