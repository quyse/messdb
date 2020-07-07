{-# LANGUAGE DeriveGeneric, GADTs, GeneralizedNewtypeDeriving, RankNTypes, ScopedTypeVariables, UndecidableInstances #-}

module MessDB.Repo
  ( RepoRoot(..)
  , RepoTable(..)
  , SomeRepoTable(..)
  , RepoTableName(..)
  , RepoStore(..)
  , RepoQuery(..)
  , RepoStatement(..)
  , loadRepoTable
  , saveRepoTable
  , runRepoStatement
  ) where

import Control.Exception
import Data.Proxy
import qualified Data.Serialize as S
import qualified Data.Text as T
import GHC.Generics(Generic)

import MessDB.Schema
import MessDB.Store
import MessDB.Table

-- | Repo root contains a table of tables.
newtype RepoRoot e = RepoRoot (Table RepoTableName (SomeRepoTable e))

-- | Table in repository.
data RepoTable e k v = RepoTable
  { repoTable_tableRef :: {-# UNPACK #-} !(TableRef k v)
  } deriving Generic

instance S.Serialize (RepoTable e k v)

-- | Table in repository hiding key/value types.
data SomeRepoTable e where
  SomeRepoTable :: (TableKey k, S.Serialize v, SchemaTypeClass e k, SchemaTypeClass e v) => RepoTable e k v -> SomeRepoTable e

-- Serialization of SomeRepoTable stores key/value types.
instance (SchemaEncoding e, SchemaConstraintClass e TableKey, SchemaConstraintClass e TableValue) => S.Serialize (SomeRepoTable e) where
  put (SomeRepoTable (repoTable :: RepoTable e k v)) = do
    S.put (encodeSchema (Proxy :: Proxy k) :: e)
    S.put (encodeSchema (Proxy :: Proxy v) :: e)
    S.put repoTable
  get = do
    Just (ConstrainedSchema keyProxy :: ConstrainedSchema e TableKey) <- constrainSchema . decodeSchema <$> S.get
    Just (ConstrainedSchema valueProxy :: ConstrainedSchema e TableValue) <- constrainSchema . decodeSchema <$> S.get
    let
      getRepoTable :: Proxy k -> Proxy v -> S.Get (RepoTable e k v)
      getRepoTable Proxy Proxy = S.get
    SomeRepoTable <$> getRepoTable keyProxy valueProxy


-- | Table name in repository.
newtype RepoTableName = RepoTableName T.Text deriving (Eq, Ord, TableKey)

-- | Repo store keeps repo root.
class RepoStore s where
  repoStoreGetRoot :: s -> IO StoreKey
  repoStoreSetRoot :: s -> StoreKey -> IO ()

-- | Type of read-only query to repo.
newtype RepoQuery e = RepoQuery (forall s ms. (Store s, MemoStore ms) => s -> ms -> RepoRoot e -> SomeRepoTable e)
-- | Type of a statement to repo.
newtype RepoStatement e = RepoStatement (forall s ms. (Store s, MemoStore ms) => s -> ms -> RepoRoot e -> RepoRoot e)

-- | Load repo root.
loadRepoRoot :: (Store s, RepoStore rs) => s -> rs -> IO (RepoRoot e)
loadRepoRoot store repoStore = fmap RepoRoot . either (\SomeException {} -> return emptyTable) (load store) =<< try (repoStoreGetRoot repoStore)

-- | Save repo root.
saveRepoRoot :: (Store s, RepoStore rs) => s -> rs -> RepoRoot e -> IO ()
saveRepoRoot store repoStore (RepoRoot rootTable) = do
  save store rootTable
  repoStoreSetRoot repoStore (tableHash rootTable)

-- | Load repo table by name.
loadRepoTable
  ::
    ( Store s, MemoStore ms, RepoStore rs
    , SchemaEncoding e, SchemaConstraintClass e TableKey, SchemaConstraintClass e TableValue
    )
  => s -> ms -> rs -> RepoTableName -> IO (Maybe (SomeRepoTable e))
loadRepoTable store memoStore repoStore tableName = do
  RepoRoot rootTable <- loadRepoRoot store repoStore
  case tableToRows $ rangeFilterTable store memoStore (tableKeyRangeSingleton tableName) rootTable of
    [(_, table)] -> return $ Just table
    [] -> return Nothing
    _ -> fail "loadRepoTable: impossible"

-- | Save repo table by name.
-- Table must be saved already.
saveRepoTable
  ::
    ( Store s, MemoStore ms, RepoStore rs
    , SchemaEncoding e, SchemaConstraintClass e TableKey, SchemaConstraintClass e TableValue
    )
  => s -> ms -> rs -> RepoTableName -> SomeRepoTable e -> IO ()
saveRepoTable s ms rs tableName table =
  runRepoStatement s ms rs $ RepoStatement $ \store memoStore (RepoRoot rootTable) ->
    RepoRoot $ tableInsert store memoStore tableName table rootTable

runRepoStatement :: (Store s, MemoStore ms, RepoStore rs) => s -> ms -> rs -> RepoStatement e -> IO ()
runRepoStatement store memoStore repoStore (RepoStatement f) =
  saveRepoRoot store repoStore . f store memoStore =<< loadRepoRoot store repoStore
