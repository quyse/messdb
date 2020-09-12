module MessDB.Store.Zlib
  ( ZlibStore(..)
  ) where

import qualified Codec.Compression.Zlib as Zlib

import MessDB.Store

newtype ZlibStore s = ZlibStore s

instance Store s => Store (ZlibStore s) where
  storeSave (ZlibStore s) key = storeSave s key . fmap Zlib.compress
  storeLoad (ZlibStore s) = fmap Zlib.decompress . storeLoad s
