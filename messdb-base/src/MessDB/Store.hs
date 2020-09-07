{-# LANGUAGE DefaultSignatures, GeneralizedNewtypeDeriving #-}

module MessDB.Store
  ( Store(..)
  , MemoStore(..)
  , Encodable(..)
  , Persistable(..)
  , StoreKey(..)
  , storeKeyToString
  ) where

import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import Data.Hashable
import qualified Data.Serialize as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V

newtype StoreKey = StoreKey
  { unStoreKey :: BS.ShortByteString
  } deriving (Eq, Hashable, S.Serialize)

instance Show StoreKey where
  show = storeKeyToString

class Store s where
  storeSave :: s -> StoreKey -> IO BL.ByteString -> IO ()
  storeLoad :: s -> StoreKey -> IO BL.ByteString

-- | Memo store is a store specialized for caching small pieces of data.
class MemoStore s where
  -- | Calls IO action only if it's missing from the store.
  -- Stores value from IO action only when Just is returned.
  -- Returns either value from store, or returned user value from IO action.
  memoStoreCache :: s -> StoreKey -> IO (Maybe BS.ShortByteString, a) -> IO (Either BS.ShortByteString a)

class Encodable a where
  -- | Encode only the object, without children.
  encode :: a -> S.Put
  default encode :: S.Serialize a => a -> S.Put
  encode = S.put

  -- | Decode only the object, without children.
  decode :: S.Get a
  default decode :: S.Serialize a => S.Get a
  decode = S.get

instance Encodable Int

instance Encodable a => Encodable (V.Vector a) where
  encode v = do
    S.putWord32le $ fromIntegral $ V.length v
    V.mapM_ encode v
  decode = do
    len <- fromIntegral <$> S.getWord32le
    V.replicateM len decode

instance Encodable StoreKey where
  encode (StoreKey bytes) = S.putShortByteString bytes
  decode = StoreKey <$> S.getShortByteString 32

class Persistable a where
  -- | Save object, recursively including children into store.
  save :: Store s => s -> a -> IO ()
  -- | Load object lazily with all children.
  load :: Store s => s -> StoreKey -> a

storeKeyToString :: StoreKey -> String
storeKeyToString = T.unpack . T.decodeUtf8 . BA.convertToBase BA.Base16 . BS.fromShort . unStoreKey
