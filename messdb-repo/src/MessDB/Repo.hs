{-# LANGUAGE DeriveGeneric, GADTs, GeneralizedNewtypeDeriving, RankNTypes, ScopedTypeVariables, UndecidableInstances #-}

module MessDB.Repo
  ( RepoRoot(..)
  , RepoTable(..)
  , SomeRepoTable(..)
  , RepoTableName(..)
  , RepoStore(..)
  , RepoQuery(..)
  , RepoStatement(..)
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
import MessDB.Trie

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
instance (SchemaEncoding e, SchemaConstraintClass e TableKey, SchemaConstraintClass e S.Serialize) => S.Serialize (SomeRepoTable e) where
  put (SomeRepoTable (repoTable :: RepoTable e k v)) = do
    S.put (encodeSchema (Proxy :: Proxy k) :: e)
    S.put (encodeSchema (Proxy :: Proxy v) :: e)
    S.put repoTable
  get = do
    Just (ConstrainedSchema keyProxy :: ConstrainedSchema e TableKey) <- constrainSchema . decodeSchema <$> S.get
    Just (ConstrainedSchema valueProxy :: ConstrainedSchema e S.Serialize) <- constrainSchema . decodeSchema <$> S.get
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

runRepoStatement :: (Store s, MemoStore ms, RepoStore rs) => s -> ms -> rs -> RepoStatement e -> IO ()
runRepoStatement store memoStore repoStore (RepoStatement f) = do
  rootTrie <- either (\SomeException {} -> return emptyTrie) (load store) =<< try (repoStoreGetRoot repoStore)
  let
    RepoRoot (Table newRootTrie) = f store memoStore (RepoRoot (Table rootTrie))
  save store newRootTrie
  repoStoreSetRoot repoStore (trieHash newRootTrie)
