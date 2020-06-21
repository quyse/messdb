{-# LANGUAGE ConstraintKinds, DefaultSignatures #-}

module MessDB.Table.Types
  ( TableKey(..)
  , TableValue
  , encodeKey
  , decodeKey
  , encodeValue
  , decodeValue
  ) where

import Control.Monad
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B
import qualified Data.ByteString.Short as BS
import qualified Data.ByteString.Unsafe as B
import Data.Int
import qualified Data.Serialize as S
import Data.Serialize.Text()
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word
import Foreign.ForeignPtr
import Foreign.Storable
import GHC.Float
import System.IO.Unsafe

import MessDB.Trie

-- | Serialization for keys.
-- This is similar to Serialize, but must support important additional property:
-- serialized values must be comparable as bytestrings, that is, retain the same
-- order as original values.
class (Eq k, Ord k) => TableKey k where
  putKey :: k -> S.Put
  default putKey :: S.Serialize k => k -> S.Put
  putKey = S.put
  getKey :: S.Get k
  default getKey :: S.Serialize k => S.Get k
  getKey = S.get

type TableValue = S.Serialize

putValue :: S.Serialize v => v -> S.Put
putValue = S.put

getValue :: S.Serialize v => S.Get v
getValue = S.get

encodeKey :: TableKey k => k -> Key
encodeKey = Key . BS.toShort . S.runPut . putKey

decodeKey :: TableKey k => Key -> k
decodeKey (Key bytes) = either error id $ S.runGet getKey (BS.fromShort bytes)

encodeValue :: TableValue v => v -> Value
encodeValue = S.runPut . putValue

decodeValue :: TableValue v => Value -> v
decodeValue = either error id . S.runGet getValue



-- Key types.

-- Keys are always compared as bytestrings, so we should be careful with encodings.

-- For numeric keys, big endian encoding is necessary.
-- Signed numbers must also be rebalanced into positive range, because two's complement won't work.

instance TableKey Int64 where
  putKey = S.putWord64be . fromIntegral . (+ minBound)
  getKey = (+ (-minBound)) . fromIntegral <$> S.getWord64be

instance TableKey Int32 where
  putKey = S.putWord32be . fromIntegral . (+ minBound)
  getKey = (+ (-minBound)) . fromIntegral <$> S.getWord32be

instance TableKey Int16 where
  putKey = S.putWord16be . fromIntegral . (+ minBound)
  getKey = (+ (-minBound)) . fromIntegral <$> S.getWord16be

instance TableKey Int8 where
  putKey = S.putWord8 . fromIntegral . (+ minBound)
  getKey = (+ (-minBound)) . fromIntegral <$> S.getWord8

instance TableKey Word64 where
  putKey = S.putWord64be
  getKey = S.getWord64be

instance TableKey Word32 where
  putKey = S.putWord32be
  getKey = S.getWord32be

instance TableKey Word16 where
  putKey = S.putWord16be
  getKey = S.getWord16be

instance TableKey Word8 where
  putKey = S.putWord8
  getKey = S.getWord8

-- IEEE 754 floating-point numbers are almost comparable as-is: exponent goes first (in big-endian)
-- and in biased format, significand is non-negative. The only problem is sign bit,
-- so we invert it, and also invert all other bits for negative numbers, to reverse the order.

instance TableKey Float where
  putKey = S.putWord32be . transform . castFloatToWord32 where
    transform n = n `xor` (if n `testBit` 31 then 0xFFFFFFFF else 0x80000000)
  getKey = castWord32ToFloat . transform <$> S.getWord32be where
    transform n = n `xor` (if n `testBit` 31 then 0x80000000 else 0xFFFFFFFF)

instance TableKey Double where
  putKey = S.putWord64be . transform . castDoubleToWord64 where
    transform n = n `xor` (if n `testBit` 63 then 0xFFFFFFFFFFFFFFFF else 0x8000000000000000)
  getKey = castWord64ToDouble . transform <$> S.getWord64be where
    transform n = n `xor` (if n `testBit` 63 then 0x8000000000000000 else 0xFFFFFFFFFFFFFFFF)

-- ByteString is encoded in base7 big-endian. MSB of every byte is set to 1,
-- except for the extra zero byte in the end.
instance TableKey B.ByteString where
  putKey bytes = S.putByteString $ unsafePerformIO $ B.unsafeUseAsCStringLen bytes $ \(ptr, len) -> do
    let
      newLen = len + (len + 6) `quot` 7 + 1
    fptr <- mallocForeignPtrBytes newLen
    withForeignPtr fptr $ \buf -> do
      let
        f i j = when (i < len) $ let
          n = min 7 $ len - i
          s k p = if k < n
            then do
              b <- peekByteOff ptr (i + k)
              pokeByteOff buf (j + k) $ 0x80 .|. p .|. (b `shiftR` (k + 1))
              s (k + 1) ((b .&. ((1 `shiftL` (k + 1)) - 1)) `shiftL` (6 - k))
            else
              pokeByteOff buf (j + k) $ 0x80 .|. p
          in do
            s 0 (0 :: Word8)
            f (i + 7) (j + 8)
        in f 0 0
      pokeByteOff buf (newLen - 1) (0x00 :: Word8)
    return $ B.fromForeignPtr fptr 0 newLen
  getKey = B.pack <$> bytes where
    bytes = let
      f i p = do
        b <- S.getWord8
        if b `testBit` 7
          then let
            k = i .&. 0x7
            q = (b .&. ((1 `shiftL` (7 - k)) - 1)) `shiftL` (k + 1)
            in if k > 0
              then let
                s = p .|. ((b `shiftR` (7 - k)) .&. ((1 `shiftL` k) - 1))
                in (s :) <$> f (i + 1) q
              else f (i + 1) q
          else return []
      in f 0 0x00

-- Text is simply compared by its UTF-8 representation.
instance TableKey T.Text where
  putKey = putKey . T.encodeUtf8
  getKey = T.decodeUtf8 <$> getKey

instance (TableKey a, TableKey b) => TableKey (a, b) where
  putKey (a, b) = do
    putKey a
    putKey b
  getKey = do
    a <- getKey
    b <- getKey
    return (a, b)

instance (TableKey a, TableKey b, TableKey c) => TableKey (a, b, c) where
  putKey (a, b, c) = do
    putKey a
    putKey b
    putKey c
  getKey = do
    a <- getKey
    b <- getKey
    c <- getKey
    return (a, b, c)
