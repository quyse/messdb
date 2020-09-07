{-# LANGUAGE LambdaCase, OverloadedLists, ViewPatterns #-}

module Main(main) where

import Control.Monad
import Control.Monad.Trans.Except
import qualified Data.Aeson as J
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC8
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TL
import qualified Options.Applicative as O
import qualified Language.SQL.SimpleSQL.Parse as SS
import qualified Language.SQL.SimpleSQL.Pretty as SS
import System.Exit
import System.IO

import MessDB.Repo
import MessDB.Schema
import MessDB.Schema.Standard
import MessDB.SQL
import MessDB.Store.Sqlite
import MessDB.Table.Row
import MessDB.Tool.Csv
import MessDB.Tool.Json

data Options = Options
  { options_dbPath :: String
  , options_command :: OptionCommand
  }

data OptionCommand
  = OptionCommand_sql
    { optionCommand_commands :: [String]
    , optionCommand_files :: [String]
    , optionCommand_printParsedSQL :: Bool
    }
  | OptionCommand_printSchema
    { optionCommand_table :: String
    }
  | OptionCommand_importCsv
    { optionCommand_table :: String
    , optionCommand_file :: String
    }
  | OptionCommand_exportCsv
    { optionCommand_table :: String
    , optionCommand_file :: String
    }
  | OptionCommand_exportJson
    { optionCommand_table :: String
    , optionCommand_file :: String
    }

main :: IO ()
main = run =<< O.execParser parser where
  parser = O.info (O.helper <*> opts)
    (  O.fullDesc
    <> O.progDesc "Command-line tool for MessDB"
    <> O.header "messdb-tool"
    )

  opts = Options
    <$> O.strOption
      (  O.long "db"
      <> O.short 'd'
      <> O.metavar "DB"
      <> O.help "Path to database"
      )
    <*> O.subparser
      (  O.command "sql"
        (  O.info
          (O.helper <*> (OptionCommand_sql
            <$> O.many (O.strOption
              (  O.long "command"
              <> O.short 'c'
              <> O.metavar "SQL"
              <> O.help "SQL statement or query"
              ))
            <*> O.many (O.strOption
              (  O.long "file"
              <> O.short 'f'
              <> O.metavar "SQL_FILE"
              <> O.help ".sql file to run"
              ))
            <*> O.switch
              (  O.long "print-parsed-sql"
              <> O.help "Pretty-print all SQL queries to verify that queries are parsed correctly"
              )
          )) (O.fullDesc <> O.progDesc "Run SQL statements or queries")
        )
      <> O.command "print-schema"
        (  O.info
          (O.helper <*> (OptionCommand_printSchema
            <$> O.strOption
              (  O.long "table"
              <> O.short 't'
              <> O.metavar "TABLE"
              <> O.help "Table to import into"
              )
          )) (O.fullDesc <> O.progDesc "Print table schema")
        )
      <> O.command "import-csv"
        (  O.info
          (O.helper <*> (OptionCommand_importCsv
            <$> O.strOption
              (  O.long "table"
              <> O.short 't'
              <> O.metavar "TABLE"
              <> O.help "Table to import into"
              )
            <*> O.strArgument
              (  O.value "-" <> O.showDefault
              <> O.metavar "CSV_FILE"
              <> O.help ".csv file to import, '-' for stdin"
              )
          )) (O.fullDesc <> O.progDesc "Import CSV file into table")
        )
      <> O.command "export-csv"
        (  O.info
          (O.helper <*> (OptionCommand_exportCsv
            <$> O.strOption
              (  O.long "table"
              <> O.short 't'
              <> O.metavar "TABLE"
              <> O.help "Table to export from"
              )
            <*> O.strArgument
              (  O.value "-" <> O.showDefault
              <> O.metavar "CSV_FILE"
              <> O.help ".csv file name to export to, '-' for stdout"
              )
          )) (O.fullDesc <> O.progDesc "Export table as CSV")
        )
      <> O.command "export-json"
        (  O.info
          (O.helper <*> (OptionCommand_exportJson
            <$> O.strOption
              (  O.long "table"
              <> O.short 't'
              <> O.metavar "TABLE"
              <> O.help "Table to export from"
              )
            <*> O.strArgument
              (  O.value "-" <> O.showDefault
              <> O.metavar "JSON_FILE"
              <> O.help ".json file name to export to, '-' for stdout"
              )
          )) (O.fullDesc <> O.progDesc "Export table as CSV")
        )
      )

  run Options
    { options_dbPath = dbPath
    , options_command = command
    } = withSqliteStore dbPath $ \store -> runCommand Repo
    { repo_store = store
    , repo_memoStore = store
    , repo_repoStore = store
    } command

  runCommand :: Repo StandardSchema -> OptionCommand -> IO ()
  runCommand repo = \case
    OptionCommand_sql
      { optionCommand_commands = sqlCommands
      , optionCommand_files = sqlFiles
      , optionCommand_printParsedSQL = printParsedSQL
      } -> do
      -- SQL commands
      let
        commandSqlTexts = zip ["command#" <> show i | i <- [(1 :: Int)..]] sqlCommands

      -- load SQL files
      fileSqlTexts <- forM sqlFiles $ \sqlFile -> do
        sqlText <- TL.readFile sqlFile
        return (sqlFile, TL.unpack sqlText)

      -- parse SQL commands and files
      parsedSqlTexts <- fmap concat . forM (commandSqlTexts <> fileSqlTexts) $ \(scriptName, sqlText) -> case SS.parseStatements sqlDialect scriptName Nothing sqlText of
        Left SS.ParseError
          { SS.peFormattedError = e
          } -> die e
        Right statements -> do
          when printParsedSQL $ do
            hPutStrLn stderr $ scriptName <> ":"
            hPutStrLn stderr $ SS.prettyStatements sqlDialect statements
          return statements

      queriesAndStatements <- forM parsedSqlTexts $ \parsedSqlText -> case runExcept (sqlStatement parsedSqlText) of
        Left e -> die $ show e <> ": " <> SS.prettyStatement sqlDialect parsedSqlText
        Right eitherQueryOrStatement -> return eitherQueryOrStatement

      forM_ queriesAndStatements $ \case
        Left repoQuery -> BLC8.putStrLn =<< repoTableExportToJsonLinesOrDie repo =<< runRepoQuery repo repoQuery
        Right repoStatement -> runRepoStatement repo repoStatement

    OptionCommand_printSchema
      { optionCommand_table = RepoTableName . T.pack -> tableName
      } -> do
      (ConstrainedSchema keyProxy, ConstrainedSchema valueProxy) <- repoTableSchema <$> loadTableOrDie repo tableName
      printJson $ encodeSchema keyProxy `asProxyTypeOf` repo
      printJson $ encodeSchema valueProxy `asProxyTypeOf` repo

    OptionCommand_importCsv
      { optionCommand_table = RepoTableName . T.pack -> tableName
      , optionCommand_file = csvFile
      } -> repoTableImportCsv repo tableName =<< BL.readFile csvFile

    OptionCommand_exportCsv
      { optionCommand_table = RepoTableName . T.pack -> tableName
      , optionCommand_file = csvFile
      } -> BL.writeFile csvFile =<< repoTableExportCsv repo tableName

    OptionCommand_exportJson
      { optionCommand_table = RepoTableName . T.pack -> tableName
      , optionCommand_file = csvFile
      } -> BL.writeFile csvFile =<< repoTableExportToJsonLinesOrDie repo =<< loadTableOrDie repo tableName

loadTableOrDie :: IsRepo e => Repo e -> RepoTableName -> IO (SomeRepoTable e)
loadTableOrDie repo tableName@(RepoTableName tableNameText) =
  maybe (die $ "table " <> T.unpack tableNameText <> " not found") return
  =<< loadRepoTable repo tableName

repoTableExportToJsonLinesOrDie :: (IsRepo e, SchemaConstraintClass e RowToJson) => Repo e -> SomeRepoTable e -> IO BL.ByteString
repoTableExportToJsonLinesOrDie repo table = maybe (die "table schema does not support JSON encoding") return $ repoTableExportToJsonLines repo table

printJson :: J.ToJSON a => a -> IO ()
printJson = BLC8.putStrLn . J.encode
