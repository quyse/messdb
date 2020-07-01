{-# LANGUAGE ViewPatterns #-}

module MessDB.Store.Lmdb
  ( LmdbStore()
  , withLmdbStore
  ) where

import Control.Exception
import Control.Monad
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import qualified Data.ByteString.Unsafe as B
import Foreign.C.String
import Foreign.C.Types
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils
import Foreign.Ptr
import Foreign.Storable

import MessDB.Store

newtype LmdbStore = LmdbStore
  { lmdbStore_storePtr :: Ptr C_LmdbStore
  }

data C_LmdbStore

withLmdbStore :: FilePath -> (LmdbStore -> IO a) -> IO a
withLmdbStore filePath = bracket open close where
  open = withCString filePath $ \filePathPtr -> do
    storePtr <- c_store_open filePathPtr
    when (storePtr == nullPtr) $ throwIO LmdbStoreException_lmdb
    return LmdbStore
      { lmdbStore_storePtr = storePtr
      }
  close LmdbStore
    { lmdbStore_storePtr = storePtr
    } = c_store_close storePtr

instance Store LmdbStore where
  storeSave store@LmdbStore
    { lmdbStore_storePtr = storePtr
    } (StoreKey key) io = BS.useAsCStringLen key $ \(keyPtr, keyLen) -> do
    exists <- c_store_key_exists storePtr keyPtr (fromIntegral keyLen)
    checkStore store
    unless (exists > 0) $ do
      value <- io
      B.unsafeUseAsCStringLen (BL.toStrict value) $ \(valuePtr, valueLen) -> do
        c_store_set storePtr keyPtr (fromIntegral keyLen) valuePtr (fromIntegral valueLen)
        checkStore store
  storeLoad store@LmdbStore
    { lmdbStore_storePtr = storePtr
    } (StoreKey key) = BS.useAsCStringLen key $ \(keyPtr, keyLen) ->
    alloca $ \valuePtrPtr -> alloca $ \valueSizePtr -> do
      c_store_get storePtr keyPtr (fromIntegral keyLen) valuePtrPtr valueSizePtr
      checkStore store
      valuePtr <- peek valuePtrPtr
      if valuePtr == nullPtr
        then throwIO LmdbStoreException_valueDoesNotExist
        else do
          valueSize <- fromIntegral <$> peek valueSizePtr
          BL.fromStrict <$> B.unsafePackMallocCStringLen (valuePtr, valueSize)

instance MemoStore LmdbStore where
  memoStoreCache store@LmdbStore
    { lmdbStore_storePtr = storePtr
    } (StoreKey key) io = BS.useAsCStringLen key $ \(keyPtr, keyLen) ->
    alloca $ \valuePtrPtr -> alloca $ \valueSizePtr -> do
    c_memo_store_get storePtr keyPtr (fromIntegral keyLen) valuePtrPtr valueSizePtr
    checkStore store
    valuePtr <- peek valuePtrPtr
    if valuePtr == nullPtr
      then do
        (maybeNewValue, userData) <- io
        case maybeNewValue of
          Just newValue -> BS.useAsCStringLen newValue $ \(newValuePtr, newValueLen) -> do
            c_memo_store_set storePtr keyPtr (fromIntegral keyLen) newValuePtr (fromIntegral newValueLen)
            checkStore store
          Nothing -> return ()
        return $ Right userData
      else do
        valueSize <- fromIntegral <$> peek valueSizePtr
        Left . BS.toShort <$> B.unsafePackMallocCStringLen (valuePtr, valueSize)

checkStore :: LmdbStore -> IO ()
checkStore LmdbStore
  { lmdbStore_storePtr = storePtr
  } = do
  errored <- c_store_errored storePtr
  when (errored > 0) $ throwIO LmdbStoreException_lmdb

-- | Copy into malloc'ed buffer.
createBlob :: Ptr CChar -> CInt -> IO (Ptr CChar)
createBlob ptr (fromIntegral -> size) = do
  buffer <- mallocBytes size
  copyBytes buffer ptr size
  return buffer

data LmdbStoreException
  = LmdbStoreException_lmdb
  | LmdbStoreException_valueDoesNotExist
  deriving Show

instance Exception LmdbStoreException

foreign import ccall safe "messdb_lmdb_store_open" c_store_open :: Ptr CChar -> IO (Ptr C_LmdbStore)
foreign import ccall safe "messdb_lmdb_store_close" c_store_close :: Ptr C_LmdbStore -> IO ()
foreign import ccall unsafe "messdb_lmdb_store_errored" c_store_errored :: Ptr C_LmdbStore -> IO CInt
foreign import ccall safe "messdb_lmdb_store_key_exists" c_store_key_exists :: Ptr C_LmdbStore -> Ptr CChar -> CSize -> IO CInt
foreign import ccall safe "messdb_lmdb_store_get" c_store_get :: Ptr C_LmdbStore -> Ptr CChar -> CSize -> Ptr (Ptr CChar) -> Ptr CSize -> IO ()
foreign import ccall safe "messdb_lmdb_store_set" c_store_set :: Ptr C_LmdbStore -> Ptr CChar -> CSize -> Ptr CChar -> CSize -> IO ()
foreign import ccall safe "messdb_lmdb_memo_store_get" c_memo_store_get :: Ptr C_LmdbStore -> Ptr CChar -> CSize -> Ptr (Ptr CChar) -> Ptr CSize -> IO ()
foreign import ccall safe "messdb_lmdb_memo_store_set" c_memo_store_set :: Ptr C_LmdbStore -> Ptr CChar -> CSize -> Ptr CChar -> CSize -> IO ()
foreign export ccall "messdb_lmdb_store_create_blob" createBlob :: Ptr CChar -> CInt -> IO (Ptr CChar)
