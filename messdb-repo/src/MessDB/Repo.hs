{-# LANGUAGE ConstraintKinds, DeriveGeneric, GADTs, GeneralizedNewtypeDeriving, RankNTypes, ScopedTypeVariables, UndecidableInstances #-}

module MessDB.Repo
  ( Repo(..)
  , IsRepo
  , RepoRoot(..)
  , RepoTable(..)
  , SomeRepoTable(..)
  , RepoTableName(..)
  , RepoStore(..)
  , RepoQuery(..)
  , RepoStatement(..)
  , getRepoTable
  , loadRepoTable
  , saveRepoTable
  , runRepoQuery
  , runRepoStatement
  , repoTableSchema
  ) where

import Control.Exception
import Data.Proxy
import qualified Data.Serialize as S
import qualified Data.Text as T
import GHC.Generics(Generic)

import MessDB.Schema
import MessDB.Store
import MessDB.Table

-- | Repo combines all what's needed for repo operations.
data Repo e where
  Repo :: (Store s, MemoStore ms, RepoStore rs) =>
    { repo_store :: s
    , repo_memoStore :: ms
    , repo_repoStore :: rs
    } -> Repo e

-- | Constraint for useful repos.
type IsRepo e = (SchemaEncoding e, SchemaConstraintClass e TableKey, SchemaConstraintClass e TableValue)

-- | Repo root contains a table of tables.
newtype RepoRoot e = RepoRoot (Table RepoTableName (SomeRepoTable e))

-- | Table in repository.
data RepoTable e k v = RepoTable
  { repoTable_tableRef :: {-# UNPACK #-} !(TableRef k v)
  } deriving Generic

instance S.Serialize (RepoTable e k v)

-- | Table in repository hiding key/value types.
data SomeRepoTable e where
  SomeRepoTable :: (TableKey k, TableValue v, SchemaTypeClass e k, SchemaTypeClass e v) => RepoTable e k v -> SomeRepoTable e

-- Serialization of SomeRepoTable stores key/value types.
instance IsRepo e => S.Serialize (SomeRepoTable e) where
  put (SomeRepoTable (repoTable :: RepoTable e k v)) = do
    S.put (encodeSchema (Proxy :: Proxy k) :: e)
    S.put (encodeSchema (Proxy :: Proxy v) :: e)
    S.put repoTable
  get = do
    Just (ConstrainedSchema keyProxy :: ConstrainedSchema e TableKey) <- constrainSchema . decodeSchema <$> S.get
    Just (ConstrainedSchema valueProxy :: ConstrainedSchema e TableValue) <- constrainSchema . decodeSchema <$> S.get
    let
      getTable :: Proxy k -> Proxy v -> S.Get (RepoTable e k v)
      getTable Proxy Proxy = S.get
    SomeRepoTable <$> getTable keyProxy valueProxy


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
loadRepoRoot :: Repo e -> IO (RepoRoot e)
loadRepoRoot Repo
  { repo_store = store
  , repo_repoStore = repoStore
  } = fmap RepoRoot . either (\SomeException {} -> return emptyTable) (load store) =<< try (repoStoreGetRoot repoStore)

-- | Save repo root.
saveRepoRoot :: Repo e -> RepoRoot e -> IO ()
saveRepoRoot Repo
  { repo_store = store
  , repo_repoStore = repoStore
  } (RepoRoot rootTable) = do
  save store rootTable
  repoStoreSetRoot repoStore (tableHash rootTable)

-- | Get repo table by name from root.
getRepoTable :: (IsRepo e, Store s, MemoStore ms) => s -> ms -> RepoRoot e -> RepoTableName -> Maybe (SomeRepoTable e)
getRepoTable store memoStore (RepoRoot rootTable) tableName = case tableToRows $ rangeFilterTable store memoStore (tableKeyRangeSingleton tableName) rootTable of
  [(_, table)] -> Just table
  _ -> Nothing

-- | Load repo table by name.
loadRepoTable :: IsRepo e => Repo e -> RepoTableName -> IO (Maybe (SomeRepoTable e))
loadRepoTable repo@Repo
  { repo_store = store
  , repo_memoStore = memoStore
  } tableName = flip (getRepoTable store memoStore) tableName <$> loadRepoRoot repo

-- | Save repo table by name.
-- Table must be saved already.
saveRepoTable :: IsRepo e => Repo e -> RepoTableName -> SomeRepoTable e -> IO ()
saveRepoTable repo tableName table =
  runRepoStatement repo $ RepoStatement $ \store memoStore (RepoRoot rootTable) ->
    RepoRoot $ tableInsert store memoStore tableName table rootTable

runRepoQuery :: Repo e -> RepoQuery e -> IO (SomeRepoTable e)
runRepoQuery repo@Repo
  { repo_store = store
  , repo_memoStore = memoStore
  } (RepoQuery f) = f store memoStore <$> loadRepoRoot repo

runRepoStatement :: Repo e -> RepoStatement e -> IO ()
runRepoStatement repo@Repo
  { repo_store = store
  , repo_memoStore = memoStore
  } (RepoStatement f) = saveRepoRoot repo . f store memoStore =<< loadRepoRoot repo

repoTableSchema :: IsRepo e => SomeRepoTable e -> (ConstrainedSchema e TableKey, ConstrainedSchema e TableValue)
repoTableSchema (SomeRepoTable repoTable) = f Proxy Proxy repoTable where
  f :: (IsRepo e, TableKey k, TableValue v, SchemaTypeClass e k, SchemaTypeClass e v) => Proxy k -> Proxy v -> RepoTable e k v -> (ConstrainedSchema e TableKey, ConstrainedSchema e TableValue)
  f pk pv _ = (ConstrainedSchema pk, ConstrainedSchema pv)
