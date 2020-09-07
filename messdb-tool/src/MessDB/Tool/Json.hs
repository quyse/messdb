{-# LANGUAGE OverloadedStrings #-}

module MessDB.Tool.Json
  ( repoTableExportToJsonLines
  ) where

import qualified Data.Aeson as J
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BL

import MessDB.Repo
import MessDB.Schema
import MessDB.Table
import MessDB.Table.Row

repoTableExportToJsonLines :: (IsRepo e, SchemaConstraintClass e RowToJson) => Repo e -> SomeRepoTable e -> Maybe (BL.ByteString)
repoTableExportToJsonLines repo@Repo
  { repo_store = store
  } (SomeRepoTable RepoTable
  { repoTable_tableRef = tableRef
  }) = f repo constrainSchemaType constrainSchemaType $ resolveTableRef store tableRef where
  f :: (TableKey k, TableValue v) => Repo e -> Maybe (ConstrainedType e RowToJson k) -> Maybe (ConstrainedType e RowToJson v) -> Table k v -> Maybe BL.ByteString
  f _repo m1 m2 table = case (m1, m2) of
    (Just ConstrainedType, Just ConstrainedType) -> Just $ rowsToJsonLines $ tableToRows table
    _ -> Nothing

-- | Convert list of rows to a JSON per line.
rowsToJsonLines :: (RowToJson k, RowToJson v) => [(k, v)] -> BL.ByteString
rowsToJsonLines = BB.toLazyByteString . mconcat . map rowToJsonLine

rowToJsonLine :: (RowToJson k, RowToJson v) => (k, v) -> BB.Builder
rowToJsonLine (k, v) = J.fromEncoding (J.toEncoding (J.Object $ rowToJson k <> rowToJson v)) <> "\n"
