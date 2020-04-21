{-# LANGUAGE BangPatterns #-}

module MessDB.Slicing
  ( SliceState()
  , withSliceState
  , sliceBytes
  , sliceDigest
  ) where

import Control.Monad
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Unsafe as B
import Data.Word
import Foreign.C.Types
import Foreign.Marshal.Alloc
import Foreign.Ptr
import System.IO.Unsafe

newtype SliceState = SliceState (Ptr ())

withSliceState :: (SliceState -> IO a) -> a
withSliceState f = unsafePerformIO $ allocaBytes 76 $ \p -> do
  c_rollsumInit p
  f $ SliceState p

sliceBytes :: SliceState -> BL.ByteString -> IO Bool
sliceBytes (SliceState p) bytes = foldM f False $ BL.toChunks bytes where
  f !z chunk = B.unsafeUseAsCStringLen chunk $ \(chunkPtr, chunkLen) ->
    (z || ) . (> 0) <$> c_rollsumSum p chunkPtr (fromIntegral chunkLen)

sliceDigest :: BL.ByteString -> Word32
sliceDigest bytes = withSliceState $ \ss@(SliceState p) -> do
  void $ sliceBytes ss bytes
  c_rollsumDigest p

foreign import ccall unsafe "rollsumInit" c_rollsumInit :: Ptr () -> IO ()
foreign import ccall unsafe "rollsumDigest" c_rollsumDigest :: Ptr () -> IO Word32
foreign import ccall unsafe "rollsumSum" c_rollsumSum :: Ptr () -> Ptr CChar -> CSize -> IO Word8
