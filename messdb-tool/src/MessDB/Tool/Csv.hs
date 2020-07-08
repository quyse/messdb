{-# LANGUAGE OverloadedLists, OverloadedStrings #-}

module MessDB.Tool.Csv
  ( repoTableImportCsv
  , repoTableExportCsv
  ) where

import qualified Data.ByteString.Char8 as BC8
import qualified Data.ByteString.Lazy as BL
import qualified Data.Csv as Csv
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Vector as V
import System.Exit

import MessDB.Repo
import MessDB.Schema
import MessDB.Store
import MessDB.Table
import MessDB.Table.Row

repoTableImportCsv
  :: (IsRepo e, SchemaConstraintClass e HasCsvColumnNames, SchemaConstraintClass e HasCsvFromRecord)
  => Repo e -> RepoTableName -> BL.ByteString -> IO ()
repoTableImportCsv repo@Repo
  { repo_store = store
  , repo_memoStore = memoStore
  } tableName@(RepoTableName tableNameText) csvBytes = do
  SomeRepoTable RepoTable
    { repoTable_tableRef = tableRef
    } <- maybe (die $ "Table " <> T.unpack tableNameText <> " does not exist") return =<< loadRepoTable repo tableName
  oldTable <- resolveTableRef store tableRef
  newTable <- case
    ( constrainTable repo oldTable (Proxy :: Proxy HasCsvColumnNames)
    , constrainTable repo oldTable (Proxy :: Proxy HasCsvFromRecord)
    ) of
    (   (Just ConstrainedType, Just ConstrainedType)
      , (Just ConstrainedType, Just ConstrainedType)
      ) -> tableImportCsv store memoStore csvBytes
    _ -> die "table's schema does not support CSV serialization"

  saveRepoTable repo tableName $ SomeRepoTable $ RepoTable $ refTable $ mergeTables store memoStore tableFoldToLast [oldTable, newTable]

tableImportCsv
  ::
    ( Store s, MemoStore ms
    , TableKey k, HasCsvColumnNames k, HasCsvFromRecord k
    , TableValue v, HasCsvColumnNames v, HasCsvFromRecord v
    )
  => s -> ms -> BL.ByteString -> IO (Table k v)
tableImportCsv store memoStore csvBytes = do
  sss <- either die return $ Csv.decode Csv.NoHeader csvBytes
  let
    header = V.map BC8.unpack (V.head sss)
    records = V.toList (V.tail sss)
  table <- either die return $ Csv.runParser $ let
    Just parseRowKey = csvParseRecordWithHeader header
    Just parseRowValue = csvParseRecordWithHeader header
    parseRow record = do
      key <- parseRowKey record
      value <- parseRowValue record
      return (key, value)
    in tableFromRows store memoStore <$> mapM parseRow records
  save store table
  return table

repoTableExportCsv
  :: (IsRepo e, SchemaConstraintClass e HasCsvColumnNames, SchemaConstraintClass e Csv.ToRecord)
  => Repo e -> RepoTableName -> IO BL.ByteString
repoTableExportCsv repo@Repo
  { repo_store = store
  } tableName@(RepoTableName tableNameText) = do
  SomeRepoTable RepoTable
    { repoTable_tableRef = tableRef
    } <- maybe (die $ "Table " <> T.unpack tableNameText <> " does not exist") return =<< loadRepoTable repo tableName
  table <- resolveTableRef store tableRef
  case
    ( constrainTable repo table (Proxy :: Proxy HasCsvColumnNames)
    , constrainTable repo table (Proxy :: Proxy Csv.ToRecord)
    ) of
    (   (Just ConstrainedType, Just ConstrainedType)
      , (Just ConstrainedType, Just ConstrainedType)
      ) -> tableExportCsv table
    _ -> die "table's schema does not support CSV serialization"

tableExportCsv
  ::
    ( TableKey k, HasCsvColumnNames k, Csv.ToRecord k
    , TableValue v, HasCsvColumnNames v, Csv.ToRecord v
    )
  => Table k v -> IO BL.ByteString
tableExportCsv table = return $ Csv.encode $ map Pair $ tableToRows table

-- | Check if a 'Table' has requested constraint for both key and value, using encoding of a 'Repo'.
constrainTable
  :: ( SchemaEncoding e, SchemaTypeClass e k, SchemaTypeClass e v, SchemaConstraintClass e c)
  => Repo e -> Table k v -> Proxy c -> (Maybe (ConstrainedType e c k), Maybe (ConstrainedType e c v))
constrainTable _ _ _ = (constrainSchemaType, constrainSchemaType)

newtype Pair a b = Pair (a, b)

instance (Csv.ToRecord a, Csv.ToRecord b) => Csv.ToRecord (Pair a b) where
  toRecord (Pair (a, b)) = Csv.toRecord a <> Csv.toRecord b
