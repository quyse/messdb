{-# LANGUAGE ConstraintKinds, DefaultSignatures, FlexibleContexts, FlexibleInstances, LambdaCase, TypeOperators, ViewPatterns #-}

module MessDB.Table.Types
  ( TableKey(..)
  , TableValue
  , encodeTableKey
  , decodeTableKey
  , encodeTableValue
  , decodeTableValue
  ) where

import Control.Monad
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B
import qualified Data.ByteString.Short as BS
import qualified Data.ByteString.Unsafe as B
import Data.Int
import Data.Proxy
import qualified Data.Serialize as S
import Data.Serialize.Text()
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word
import Foreign.ForeignPtr
import Foreign.Storable
import qualified GHC.Generics as G
import GHC.Float
import System.IO.Unsafe

import MessDB.Table.Row
import MessDB.Trie

-- | Serialization for keys.
-- This is similar to Serialize, but must support important additional property:
-- serialized values must be comparable as bytestrings, that is, retain the same
-- order as original values.
class (Eq k, Ord k) => TableKey k where
  putTableKey :: k -> S.Put
  default putTableKey :: (G.Generic k, GenericDatatypeTableKey (G.Rep k)) => k -> S.Put
  putTableKey = putGenericDatatypeTableKey . G.from
  getTableKey :: S.Get k
  default getTableKey :: (G.Generic k, GenericDatatypeTableKey (G.Rep k)) => S.Get k
  getTableKey = G.to <$> getGenericDatatypeTableKey

type TableValue = S.Serialize

putTableValue :: S.Serialize v => v -> S.Put
putTableValue = S.put

getTableValue :: S.Serialize v => S.Get v
getTableValue = S.get

encodeTableKey :: TableKey k => k -> Key
encodeTableKey = BS.toShort . S.runPut . putTableKey

decodeTableKey :: TableKey k => Key -> k
decodeTableKey = either error id . S.runGet getTableKey . BS.fromShort

encodeTableValue :: TableValue v => v -> Value
encodeTableValue = S.runPut . putTableValue

decodeTableValue :: TableValue v => Value -> v
decodeTableValue = either error id . S.runGet getTableValue



-- Key types.

-- Keys are always compared as bytestrings, so we should be careful with encodings.


instance TableKey () where
  putTableKey () = return ()
  getTableKey = return ()


-- For integer keys, big endian encoding is necessary.
-- Signed numbers must also be rebalanced into positive range, because two's complement won't work.

instance TableKey Int64 where
  putTableKey = S.putWord64be . fromIntegral . (+ minBound)
  getTableKey = (+ (-minBound)) . fromIntegral <$> S.getWord64be

instance TableKey Int32 where
  putTableKey = S.putWord32be . fromIntegral . (+ minBound)
  getTableKey = (+ (-minBound)) . fromIntegral <$> S.getWord32be

instance TableKey Int16 where
  putTableKey = S.putWord16be . fromIntegral . (+ minBound)
  getTableKey = (+ (-minBound)) . fromIntegral <$> S.getWord16be

instance TableKey Int8 where
  putTableKey = S.putWord8 . fromIntegral . (+ minBound)
  getTableKey = (+ (-minBound)) . fromIntegral <$> S.getWord8

instance TableKey Word64 where
  putTableKey = S.putWord64be
  getTableKey = S.getWord64be

instance TableKey Word32 where
  putTableKey = S.putWord32be
  getTableKey = S.getWord32be

instance TableKey Word16 where
  putTableKey = S.putWord16be
  getTableKey = S.getWord16be

instance TableKey Word8 where
  putTableKey = S.putWord8
  getTableKey = S.getWord8

instance (TableKey a, TableKey b) => TableKey (Row n a b) where
  putTableKey (Row a b) = do
    putTableKey a
    putTableKey b
  getTableKey = Row
    <$> getTableKey
    <*> getTableKey


-- IEEE 754 floating-point numbers are almost comparable as-is: exponent goes first (in big-endian)
-- and in biased format, significand is non-negative. The only problem is sign bit,
-- so we invert it, and also invert all other bits for negative numbers, to reverse the order.

instance TableKey Float where
  putTableKey = putTableKey . transform . castFloatToWord32 where
    transform n = n `xor` (if n `testBit` 31 then 0xFFFFFFFF else 0x80000000)
  getTableKey = castWord32ToFloat . transform <$> getTableKey where
    transform n = n `xor` (if n `testBit` 31 then 0x80000000 else 0xFFFFFFFF)

instance TableKey Double where
  putTableKey = putTableKey . transform . castDoubleToWord64 where
    transform n = n `xor` (if n `testBit` 63 then 0xFFFFFFFFFFFFFFFF else 0x8000000000000000)
  getTableKey = castWord64ToDouble . transform <$> getTableKey where
    transform n = n `xor` (if n `testBit` 63 then 0x8000000000000000 else 0xFFFFFFFFFFFFFFFF)


-- ByteString is encoded in base7 big-endian. MSB of every byte is set to 1,
-- except for the extra zero byte in the end.
instance TableKey B.ByteString where
  putTableKey bytes = S.putByteString $ unsafePerformIO $ B.unsafeUseAsCStringLen bytes $ \(ptr, len) -> do
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
  getTableKey = B.pack <$> bytes where
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
  putTableKey = putTableKey . T.encodeUtf8
  getTableKey = T.decodeUtf8 <$> getTableKey


-- Considering TableKey properies, generic tuples/structs are easy.

instance TableKey a => TableKey (Maybe a)
instance (TableKey a, TableKey b) => TableKey (a, b)
instance (TableKey a, TableKey b, TableKey c) => TableKey (a, b, c)
instance (TableKey a, TableKey b, TableKey c, TableKey d) => TableKey (a, b, c, d)

-- Implementation for Generics.

class GenericDatatypeTableKey f where
  putGenericDatatypeTableKey :: f p -> S.Put
  getGenericDatatypeTableKey :: S.Get (f p)

class GenericConstructorTableKey f where
  putGenericConstructorTableKey :: Word32 -> f p -> S.Put
  getGenericConstructorTableKey :: Word32 -> S.Get (f p)
  countGenericConstructorTableKey :: Proxy f -> Word32

class GenericSelectorTableKey f where
  putGenericSelectorTableKey :: f p -> S.Put
  getGenericSelectorTableKey :: S.Get (f p)

class GenericValueTableKey f where
  putGenericValueTableKey :: f p -> S.Put
  getGenericValueTableKey :: S.Get (f p)

instance GenericConstructorTableKey f => GenericDatatypeTableKey (G.M1 G.D c f) where
  putGenericDatatypeTableKey = putGenericConstructorTableKey 0 . G.unM1
  getGenericDatatypeTableKey = do
    i <- getTableKey
    G.M1 <$> getGenericConstructorTableKey i

instance GenericSelectorTableKey f => GenericConstructorTableKey (G.M1 G.C c f) where
  putGenericConstructorTableKey i (G.M1 k) = do
    putTableKey i
    putGenericSelectorTableKey k
  getGenericConstructorTableKey = \case
    0 -> G.M1 <$> getGenericSelectorTableKey
    _ -> fail "wrong table key index"
  countGenericConstructorTableKey Proxy = 1

instance (GenericConstructorTableKey a, GenericConstructorTableKey b) => GenericConstructorTableKey (a G.:+: b) where
  putGenericConstructorTableKey i c = case c of
    G.L1 lc -> putGenericConstructorTableKey i lc
    G.R1 rc -> putGenericConstructorTableKey (i + len Proxy c) rc
    where
      len :: GenericConstructorTableKey a => Proxy a -> (a G.:+: b) p -> Word32
      len p _ = countGenericConstructorTableKey p
  getGenericConstructorTableKey i = q Proxy where
    q :: (GenericConstructorTableKey a, GenericConstructorTableKey b) => Proxy a -> S.Get ((a G.:+: b) p)
    q (countGenericConstructorTableKey -> ll) = if i < ll
      then G.L1 <$> getGenericConstructorTableKey i
      else G.R1 <$> getGenericConstructorTableKey (i - ll)
  countGenericConstructorTableKey = q Proxy Proxy where
    q :: (GenericConstructorTableKey a, GenericConstructorTableKey b) => Proxy a -> Proxy b -> Proxy (a G.:+: b) -> Word32
    q a b Proxy = countGenericConstructorTableKey a + countGenericConstructorTableKey b

instance GenericSelectorTableKey G.U1 where
  putGenericSelectorTableKey G.U1 = return ()
  getGenericSelectorTableKey = return G.U1

instance GenericValueTableKey f => GenericSelectorTableKey (G.M1 G.S c f) where
  putGenericSelectorTableKey = putGenericValueTableKey . G.unM1
  getGenericSelectorTableKey = G.M1 <$> getGenericValueTableKey

instance (GenericSelectorTableKey a, GenericSelectorTableKey b) => GenericSelectorTableKey (a G.:*: b) where
  putGenericSelectorTableKey (a G.:*: b) = do
    putGenericSelectorTableKey a
    putGenericSelectorTableKey b
  getGenericSelectorTableKey = liftM2 (G.:*:) getGenericSelectorTableKey getGenericSelectorTableKey

instance TableKey k => GenericValueTableKey (G.K1 G.R k) where
  putGenericValueTableKey = putTableKey . G.unK1
  getGenericValueTableKey = G.K1 <$> getTableKey
