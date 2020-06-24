module MessDB.Store.Sqlite
  ( SqliteStore()
  , withSqliteStore
  ) where

import Control.Exception
import Control.Monad
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import qualified Data.ByteString.Unsafe as B
import Foreign.C.String
import Foreign.C.Types
import Foreign.Ptr
import Foreign.StablePtr

import MessDB.Store

newtype SqliteStore = SqliteStore
  { sqliteStore_storePtr :: Ptr C_SqliteStore
  }

data C_SqliteStore

withSqliteStore :: FilePath -> (SqliteStore -> IO a) -> IO a
withSqliteStore filePath = bracket open close where
  open = withCString filePath $ \filePathPtr -> do
    storePtr <- c_store_open filePathPtr
    when (storePtr == nullPtr) $ throwIO SqliteStoreException_sqlite
    return SqliteStore
      { sqliteStore_storePtr = storePtr
      }
  close SqliteStore
    { sqliteStore_storePtr = storePtr
    } = c_store_close storePtr

instance Store SqliteStore where
  storeSave store@SqliteStore
    { sqliteStore_storePtr = storePtr
    } (StoreKey key) io = BS.useAsCStringLen key $ \(keyPtr, keyLen) -> do
    exists <- c_store_key_exists storePtr keyPtr (fromIntegral keyLen)
    checkStore store
    unless (exists > 0) $ do
      value <- io
      B.unsafeUseAsCStringLen (BL.toStrict value) $ \(valuePtr, valueLen) -> c_store_set storePtr keyPtr (fromIntegral keyLen) valuePtr (fromIntegral valueLen)
      checkStore store
  storeLoad store@SqliteStore
    { sqliteStore_storePtr = storePtr
    } (StoreKey key) = BS.useAsCStringLen key $ \(keyPtr, keyLen) -> do
    valuePtr <- c_store_get storePtr keyPtr (fromIntegral keyLen)
    value <- if castStablePtrToPtr valuePtr == nullPtr
      then throwIO SqliteStoreException_valueDoesNotExist
      else do
        value <- deRefStablePtr valuePtr
        freeStablePtr valuePtr
        return value
    checkStore store
    return value

checkStore :: SqliteStore -> IO ()
checkStore SqliteStore
  { sqliteStore_storePtr = storePtr
  } = do
  errored <- c_store_errored storePtr
  when (errored > 0) $ throwIO SqliteStoreException_sqlite

createBlob :: Ptr CChar -> CInt -> IO (StablePtr BL.ByteString)
createBlob ptr size = newStablePtr . BL.fromStrict =<< B.packCStringLen (ptr, fromIntegral size)

data SqliteStoreException
  = SqliteStoreException_sqlite
  | SqliteStoreException_valueDoesNotExist
  deriving Show

instance Exception SqliteStoreException

foreign import ccall safe "messdb_sqlite_store_open" c_store_open :: Ptr CChar -> IO (Ptr C_SqliteStore)
foreign import ccall safe "messdb_sqlite_store_close" c_store_close :: Ptr C_SqliteStore -> IO ()
foreign import ccall unsafe "messdb_sqlite_store_errored" c_store_errored :: Ptr C_SqliteStore -> IO CInt
foreign import ccall safe "messdb_sqlite_store_key_exists" c_store_key_exists :: Ptr C_SqliteStore -> Ptr CChar -> CInt -> IO CInt
foreign import ccall safe "messdb_sqlite_store_get" c_store_get :: Ptr C_SqliteStore -> Ptr CChar -> CInt -> IO (StablePtr BL.ByteString)
foreign import ccall safe "messdb_sqlite_store_set" c_store_set :: Ptr C_SqliteStore -> Ptr CChar -> CInt -> Ptr CChar -> CInt -> IO ()
foreign export ccall "messdb_sqlite_store_create_blob" createBlob :: Ptr CChar -> CInt -> IO (StablePtr BL.ByteString)
