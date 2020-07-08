{-# LANGUAGE ConstraintKinds, LambdaCase, OverloadedStrings, ViewPatterns #-}

module MessDB.SQL
  ( sqlStatement
  , sqlDialect
  , SqlException(..)
  ) where

import Control.Monad.Trans.Except
import Data.Proxy
import qualified Language.SQL.SimpleSQL.Dialect as SS
import qualified Language.SQL.SimpleSQL.Syntax as SS

import MessDB.Repo
import MessDB.Schema
import MessDB.SQL.Schema
import MessDB.Table


-- | Convert SQL statement to either read-only query or writing statement.
sqlStatement :: (IsRepo e, SqlSchema e) => SS.Statement -> Except SqlException (Either (RepoQuery e) (RepoStatement e))
sqlStatement = \case
  SS.CreateTable (RepoTableName . simpleNames -> tableName) tableDefinition -> do
    (keySchema, valueSchema) <- withExcept SqlException_schema $ defineTableSchema tableDefinition
    return $ Right $ createTableStatement tableName keySchema valueSchema
  _ -> throwE SqlException_unsupportedStatement

createTableStatement :: IsRepo e => RepoTableName -> ConstrainedSchema e TableKey -> ConstrainedSchema e TableValue -> RepoStatement e
createTableStatement tableName (ConstrainedSchema keyProxy) (ConstrainedSchema valueProxy) = RepoStatement $ \store memoStore (RepoRoot tablesTable) ->
  RepoRoot $ tableInsert store memoStore tableName (SomeRepoTable RepoTable
    { repoTable_tableRef = refTable $ syncTable store $ initialTable keyProxy valueProxy
    }) tablesTable
  where
    initialTable :: Proxy k -> Proxy v -> Table k v
    initialTable Proxy Proxy = emptyTable

-- | Recommended SQL dialect.
sqlDialect :: SS.Dialect
sqlDialect = SS.postgres

data SqlException
  = SqlException_unsupportedStatement
  | SqlException_schema SqlSchemaException
  deriving Show
