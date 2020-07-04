{-# LANGUAGE GADTs, GeneralizedNewtypeDeriving, RankNTypes, ScopedTypeVariables, UndecidableInstances #-}

module MessDB.Repo
  ( RepoRoot(..)
  , RepoTable(..)
  , RepoTableName(..)
  , RepoStore(..)
  , RepoQuery(..)
  , RepoStatement(..)
  ) where

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
    maybeKeySchema :: Maybe (Schema e TableKey) <- decodeSchema <$> (S.get :: S.Get e)
    maybeValueSchema :: Maybe (Schema e S.Serialize) <- decodeSchema <$> (S.get :: S.Get e)
    case (maybeKeySchema, maybeValueSchema) of
      (   Just (Schema (Proxy :: Proxy k) :: Schema e TableKey)
        , Just (Schema (Proxy :: Proxy v) :: Schema e S.Serialize)
        ) -> do
        tableRef :: TableRef k v <- decode
        return (RepoTable tableRef :: RepoTable e)
      _ -> fail "invalid schema"


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
