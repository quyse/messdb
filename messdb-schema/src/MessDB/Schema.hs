{-# LANGUAGE ConstraintKinds, GADTs, TypeFamilies #-}

module MessDB.Schema
  ( Schema(..)
  , ConstrainedType(..)
  , ConstrainedSchema(..)
  , SchemaEncoding(..)
  , constrainSchema
  ) where

import Data.Kind
import Data.Proxy
import qualified Data.Serialize as S

-- | Schema keeps a witness of a type which can be encoded with particular encoding.
data Schema e where
  Schema :: SchemaTypeClass e a => Proxy a -> Schema e

-- | Witness of a constraint for a type.
data ConstrainedType e c a where
  ConstrainedType :: (SchemaConstraintClass e c, c a) => ConstrainedType e c a

-- | Schema with constraint.
data ConstrainedSchema e c where
  ConstrainedSchema :: (SchemaTypeClass e a, SchemaConstraintClass e c, c a) => Proxy a -> ConstrainedSchema e c

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
  -- | Decode supported type.
  decodeSchema :: e -> Schema e
  -- | Try to get another constraint for a known type.
  constrainSchemaType :: (SchemaTypeClass e a, SchemaConstraintClass e c) => Maybe (ConstrainedType e c a)

-- | Get constrained schema with specified constraint.
constrainSchema :: (SchemaEncoding e, SchemaConstraintClass e c) => Schema e -> Maybe (ConstrainedSchema e c)
constrainSchema (Schema p) = f p <$> constrainSchemaType where
  f :: SchemaTypeClass e a => Proxy a -> ConstrainedType e c a -> ConstrainedSchema e c
  f pt ConstrainedType = ConstrainedSchema pt
