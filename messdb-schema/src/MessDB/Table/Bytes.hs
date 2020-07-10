{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module MessDB.Table.Bytes
  ( Bytes(..)
  ) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import qualified Data.Csv as Csv
import qualified Data.Text.Encoding as T

import MessDB.Table

-- | Wrapper over ByteString, to be able to add more instances.
-- Various serializations represent it in hex encoding.
newtype Bytes = Bytes
  { unBytes :: B.ByteString
  } deriving (Eq, Ord, TableKey, TableValue, Csv.FromField, Csv.ToField, Show)

instance J.FromJSON Bytes where
  parseJSON = either fail (return . Bytes) . BA.convertFromBase BA.Base16 . T.encodeUtf8 <=< J.parseJSON

instance J.ToJSON Bytes where
  toJSON = J.toJSON . T.decodeUtf8 . BA.convertToBase BA.Base16 . unBytes
  toEncoding = J.toEncoding . T.decodeUtf8 . BA.convertToBase BA.Base16 . unBytes
