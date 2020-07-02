module MessDB.Test.SqliteStoreSpec
  ( spec
  ) where

import System.IO.Temp
import Test.Hspec

import MessDB.Store.Sqlite
import MessDB.Test.MemoStore
import MessDB.Test.Store

spec :: Spec
spec = describe "SqliteStore" $ do
  storeTestSpec withTempStore
  memoStoreTestSpec withTempStore

withTempStore :: (SqliteStore -> IO a) -> IO a
withTempStore io = withSystemTempDirectory "messdb-sqlite-test-temp-store" $ \path ->
  withSqliteStore (path <> "/store.db") io
