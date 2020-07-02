module MessDB.Test.LmdbStoreSpec
  ( spec
  ) where

import System.IO.Temp
import Test.Hspec

import MessDB.Store.Lmdb
import MessDB.Test.MemoStore
import MessDB.Test.Store

spec :: Spec
spec = describe "LmdbStore" $ do
  storeTestSpec withTempStore
  memoStoreTestSpec withTempStore

withTempStore :: (LmdbStore -> IO a) -> IO a
withTempStore io = withSystemTempDirectory "messdb-lmdb-test-temp-store" $ \path ->
  withLmdbStore (path <> "/store.db") io
