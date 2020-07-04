{-# LANGUAGE GADTs, GeneralizedNewtypeDeriving, RankNTypes, ScopedTypeVariables, UndecidableInstances #-}

module MessDB.Repo
  ( RepoRoot(..)
  , RepoTable(..)
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

import MessDB.Schema
import MessDB.Store
import MessDB.Table
import MessDB.Trie

-- | Repo root contains a table of tables.
newtype RepoRoot e = RepoRoot (Table RepoTableName (RepoTable e))

-- | Table in repository.
data RepoTable e where
  RepoTable :: (TableKey k, S.Serialize v, SchemaTypeClass e k, SchemaTypeClass e v) => TableRef k v -> RepoTable e

instance (SchemaEncoding e, SchemaConstraintClass e TableKey, SchemaConstraintClass e S.Serialize) => S.Serialize (RepoTable e) where
  put (RepoTable (tableRef :: TableRef k v)) = do
    S.put (encodeSchema (Proxy :: Proxy k) :: e)
    S.put (encodeSchema (Proxy :: Proxy v) :: e)
    encode tableRef
  get = do
    Just (ConstrainedSchema keyProxy :: ConstrainedSchema e TableKey) <- constrainSchema . decodeSchema <$> S.get
    Just (ConstrainedSchema valueProxy :: ConstrainedSchema e S.Serialize) <- constrainSchema . decodeSchema <$> S.get
    let
      decodeTableRef :: Proxy k -> Proxy v -> S.Get (TableRef k v)
      decodeTableRef Proxy Proxy = decode
    tableRef <- decodeTableRef keyProxy valueProxy
    return $ RepoTable tableRef


-- | Table name in repository.
newtype RepoTableName = RepoTableName T.Text deriving (Eq, Ord, TableKey)

-- | Repo store keeps repo root.
class RepoStore s where
  repoStoreGetRoot :: s -> IO StoreKey
  repoStoreSetRoot :: s -> StoreKey -> IO ()

-- | Type of read-only query to repo.
newtype RepoQuery e = RepoQuery (forall s ms. (Store s, MemoStore ms) => s -> ms -> RepoRoot e -> RepoTable e)
-- | Type of a statement to repo.
newtype RepoStatement e = RepoStatement (forall s ms. (Store s, MemoStore ms) => s -> ms -> RepoRoot e -> RepoRoot e)

runRepoStatement :: (Store s, MemoStore ms, RepoStore rs) => s -> ms -> rs -> RepoStatement e -> IO ()
runRepoStatement store memoStore repoStore (RepoStatement f) = do
  rootTrie <- either (\SomeException {} -> return emptyTrie) (load store) =<< try (repoStoreGetRoot repoStore)
  let
    RepoRoot (Table newRootTrie) = f store memoStore (RepoRoot (Table rootTrie))
  save store newRootTrie
  repoStoreSetRoot repoStore (trieHash newRootTrie)
