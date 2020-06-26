{-# LANGUAGE ConstraintKinds, GADTs, TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}

module MessDB.Schema
  ( Schema(..)
  , ConstrainedType(..)
  , SchemaEncoding(..)
  , TableSchema(..)
  ) where

import Data.Kind
import Data.Proxy
import qualified Data.Serialize as S

import MessDB.Table.Types

-- | Schema represents a type with particular encoding and additional constraint.
-- In other words, it contains a witness that this type can be encoded, and conforms to the constraint.
data Schema e c where
  Schema :: (SchemaTypeClass e a, c a) => Proxy a -> Schema e c

-- | Witness of a constraint for a type.
data ConstrainedType e c a where
  ConstrainedType :: c a => ConstrainedType e c a

-- | Schema encoding is a specific encoding supporting a set of types.
class S.Serialize e => SchemaEncoding e where
  -- | Class of all types supported by this encoding.
  -- Any type of this class can be encoded.
  type SchemaTypeClass e :: * -> Constraint
  -- | Class of constraints supported by this encoding.
  -- Types can optionally support constraints of these class.
  type SchemaConstraintClass e :: (* -> Constraint) -> Constraint

  -- | Encode supported type.
  encodeSchema :: SchemaTypeClass e a => Proxy a -> e
  -- | Decode supported type with additional constraint.
  -- Returns 'Nothing' if decoded type doesn't support that constraint.
  decodeSchema :: SchemaConstraintClass e c => e -> Maybe (Schema e c)
  -- | Try to get another constraint for a known type.
  constrainSchemaType :: (SchemaTypeClass e a, SchemaConstraintClass e c) => Maybe (ConstrainedType e c a)

-- | Schema of a table.
data TableSchema e = TableSchema
  { tableSchema_keySchema :: !(Schema e TableKey)
  , tableSchema_valueSchema :: !(Schema e S.Serialize)
  }
