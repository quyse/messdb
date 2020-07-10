{-# LANGUAGE ConstraintKinds, DefaultSignatures, DeriveGeneric, LambdaCase, ScopedTypeVariables, TemplateHaskell, TypeFamilies, TypeOperators, ViewPatterns #-}

module MessDB.Schema.Standard
  ( StandardSchema(..)
  ) where

import Control.Monad
import qualified Data.Csv as Csv
import Data.Int
import Data.Kind
import Data.Proxy
import qualified Data.Serialize as S
import qualified Data.Text as T
import Data.Typeable
import Data.Word
import GHC.Generics(Generic)
import GHC.TypeLits

import MessDB.Schema
import MessDB.Schema.Util
import MessDB.Table.Bytes
import MessDB.Table.Row
import MessDB.Table.Types

-- | Representation of some standard set of types.
data StandardSchema
  = StandardSchema_Empty
  | StandardSchema_Int64
  | StandardSchema_Int32
  | StandardSchema_Int16
  | StandardSchema_Int8
  | StandardSchema_Word64
  | StandardSchema_Word32
  | StandardSchema_Word16
  | StandardSchema_Word8
  | StandardSchema_Float
  | StandardSchema_Double
  | StandardSchema_Bytes
  | StandardSchema_Text
  | StandardSchema_Maybe !StandardSchema
  | StandardSchema_Tuple2 !StandardSchema !StandardSchema
  | StandardSchema_Tuple3 !StandardSchema !StandardSchema !StandardSchema
  | StandardSchema_Tuple4 !StandardSchema !StandardSchema !StandardSchema !StandardSchema
  | StandardSchema_Row !T.Text !StandardSchema !StandardSchema
  deriving Generic

genJsonSerialize ''StandardSchema

instance SchemaEncoding StandardSchema where
  type SchemaTypeClass StandardSchema = StandardSchemaType
  type SchemaConstraintClass StandardSchema = StandardSchemaConstraint

  encodeSchema = standardSchema

  decodeSchema = \case
    StandardSchema_Empty -> Schema (Proxy :: Proxy ())
    StandardSchema_Int64 -> Schema (Proxy :: Proxy Int64)
    StandardSchema_Int32 -> Schema (Proxy :: Proxy Int32)
    StandardSchema_Int16 -> Schema (Proxy :: Proxy Int16)
    StandardSchema_Int8 -> Schema (Proxy :: Proxy Int8)
    StandardSchema_Word64 -> Schema (Proxy :: Proxy Word64)
    StandardSchema_Word32 -> Schema (Proxy :: Proxy Word32)
    StandardSchema_Word16 -> Schema (Proxy :: Proxy Word16)
    StandardSchema_Word8 -> Schema (Proxy :: Proxy Word8)
    StandardSchema_Float -> Schema (Proxy :: Proxy Float)
    StandardSchema_Double -> Schema (Proxy :: Proxy Double)
    StandardSchema_Bytes -> Schema (Proxy :: Proxy Bytes)
    StandardSchema_Text -> Schema (Proxy :: Proxy T.Text)
    StandardSchema_Maybe e -> case decodeSchema e of
      Schema p -> Schema $ liftM Just p
    StandardSchema_Tuple2 e1 e2 -> case (decodeSchema e1, decodeSchema e2) of
      (Schema p1, Schema p2) -> Schema $ liftM2 ((,)) p1 p2
    StandardSchema_Tuple3 e1 e2 e3 -> case (decodeSchema e1, decodeSchema e2, decodeSchema e3) of
      (Schema p1, Schema p2, Schema p3) -> Schema $ liftM3 ((,,)) p1 p2 p3
    StandardSchema_Tuple4 e1 e2 e3 e4 -> case (decodeSchema e1, decodeSchema e2, decodeSchema e3, decodeSchema e4) of
      (Schema p1, Schema p2, Schema p3, Schema p4) -> Schema $ liftM4 ((,,,)) p1 p2 p3 p4
    StandardSchema_Row n ea eb -> case (someSymbolVal (T.unpack n), decodeSchema ea, decodeSchema eb) of
      (SomeSymbol (Proxy :: Proxy pn), Schema (Proxy :: Proxy pa), Schema (Proxy :: Proxy pb)) -> Schema (Proxy :: Proxy (Row pn pa pb))

  constrainSchemaType = constrainStandardSchemaType

class (Typeable a, TableKey a, TableValue a, Show a) => StandardSchemaType a where
  standardSchema :: Proxy a -> StandardSchema

  hasCsvFromToField :: Maybe (ConstrainedType StandardSchema Csv.FromField a, ConstrainedType StandardSchema Csv.ToField a)
  default hasCsvFromToField :: (Csv.FromField a, Csv.ToField a) => Maybe (ConstrainedType StandardSchema Csv.FromField a, ConstrainedType StandardSchema Csv.ToField a)
  hasCsvFromToField = Just (ConstrainedType, ConstrainedType)

  hasCsvDefaultOrdered :: Maybe (ConstrainedType StandardSchema Csv.DefaultOrdered a)
  hasCsvDefaultOrdered = Nothing

  hasCsvFromToRecord :: Maybe (ConstrainedType StandardSchema Csv.FromRecord a, ConstrainedType StandardSchema Csv.ToRecord a)
  hasCsvFromToRecord = Nothing

  hasHasCsvColumnNames :: Maybe (ConstrainedType StandardSchema HasCsvColumnNames a)
  hasHasCsvColumnNames = Nothing

  hasHasCsvFromToRecord :: Maybe (ConstrainedType StandardSchema HasCsvFromRecord a, ConstrainedType StandardSchema HasCsvToRecord a)
  hasHasCsvFromToRecord = Nothing

instance StandardSchemaType () where
  standardSchema Proxy = StandardSchema_Empty
  hasCsvFromToField = Nothing
  hasHasCsvColumnNames = Just ConstrainedType
  hasHasCsvFromToRecord = Just (ConstrainedType, ConstrainedType)
instance StandardSchemaType Int64 where
  standardSchema Proxy = StandardSchema_Int64
instance StandardSchemaType Int32 where
  standardSchema Proxy = StandardSchema_Int32
instance StandardSchemaType Int16 where
  standardSchema Proxy = StandardSchema_Int16
instance StandardSchemaType Int8 where
  standardSchema Proxy = StandardSchema_Int8
instance StandardSchemaType Word64 where
  standardSchema Proxy = StandardSchema_Word64
instance StandardSchemaType Word32 where
  standardSchema Proxy = StandardSchema_Word32
instance StandardSchemaType Word16 where
  standardSchema Proxy = StandardSchema_Word16
instance StandardSchemaType Word8 where
  standardSchema Proxy = StandardSchema_Word8
instance StandardSchemaType Float where
  standardSchema Proxy = StandardSchema_Float
instance StandardSchemaType Double where
  standardSchema Proxy = StandardSchema_Double
instance StandardSchemaType Bytes where
  standardSchema Proxy = StandardSchema_Bytes
instance StandardSchemaType T.Text where
  standardSchema Proxy = StandardSchema_Text
instance StandardSchemaType a => StandardSchemaType (Maybe a) where
  standardSchema Proxy = StandardSchema_Maybe (standardSchema (Proxy :: Proxy a))
  hasCsvFromToField = f hasCsvFromToField where
    f :: Maybe (ConstrainedType StandardSchema Csv.FromField a, ConstrainedType StandardSchema Csv.ToField a) -> Maybe (ConstrainedType StandardSchema Csv.FromField (Maybe a), ConstrainedType StandardSchema Csv.ToField (Maybe a))
    f = \case
      Just (ConstrainedType, ConstrainedType) -> Just (ConstrainedType, ConstrainedType)
      Nothing -> Nothing
instance (StandardSchemaType a, StandardSchemaType b) => StandardSchemaType (a, b) where
  standardSchema Proxy = StandardSchema_Tuple2
    (standardSchema (Proxy :: Proxy a))
    (standardSchema (Proxy :: Proxy b))
  hasCsvFromToField = Nothing
instance (StandardSchemaType a, StandardSchemaType b, StandardSchemaType c) => StandardSchemaType (a, b, c) where
  standardSchema Proxy = StandardSchema_Tuple3
    (standardSchema (Proxy :: Proxy a))
    (standardSchema (Proxy :: Proxy b))
    (standardSchema (Proxy :: Proxy c))
  hasCsvFromToField = Nothing
instance (StandardSchemaType a, StandardSchemaType b, StandardSchemaType c, StandardSchemaType d) => StandardSchemaType (a, b, c, d) where
  standardSchema Proxy = StandardSchema_Tuple4
    (standardSchema (Proxy :: Proxy a))
    (standardSchema (Proxy :: Proxy b))
    (standardSchema (Proxy :: Proxy c))
    (standardSchema (Proxy :: Proxy d))
  hasCsvFromToField = Nothing
instance (KnownSymbol n, StandardSchemaType a, StandardSchemaType b) => StandardSchemaType (Row n a b) where
  standardSchema Proxy = StandardSchema_Row
    (T.pack (symbolVal (Proxy :: Proxy n)))
    (standardSchema (Proxy :: Proxy a))
    (standardSchema (Proxy :: Proxy b))
  hasCsvFromToField = Nothing
  hasCsvDefaultOrdered = f hasHasCsvColumnNames where
    f :: Maybe (ConstrainedType StandardSchema HasCsvColumnNames b) -> Maybe (ConstrainedType StandardSchema Csv.DefaultOrdered (Row n a b))
    f = \case
      Just ConstrainedType -> Just ConstrainedType
      Nothing -> Nothing
  hasCsvFromToRecord = f (hasCsvFromToField, hasHasCsvFromToRecord) where
    f ::
      ( Maybe (ConstrainedType StandardSchema Csv.FromField a, ConstrainedType StandardSchema Csv.ToField a)
      , Maybe (ConstrainedType StandardSchema HasCsvFromRecord b, ConstrainedType StandardSchema HasCsvToRecord b)
      )
      -> Maybe (ConstrainedType StandardSchema Csv.FromRecord (Row n a b), ConstrainedType StandardSchema Csv.ToRecord (Row n a b))
    f = \case
      (Just (ConstrainedType, ConstrainedType), Just (ConstrainedType, ConstrainedType)) -> Just (ConstrainedType, ConstrainedType)
      _ -> Nothing
  hasHasCsvColumnNames = f hasHasCsvColumnNames where
    f :: Maybe (ConstrainedType StandardSchema HasCsvColumnNames b) -> Maybe (ConstrainedType StandardSchema HasCsvColumnNames (Row n a b))
    f = \case
      Just ConstrainedType -> Just ConstrainedType
      Nothing -> Nothing
  hasHasCsvFromToRecord = f (hasCsvFromToField, hasHasCsvFromToRecord) where
    f ::
      ( Maybe (ConstrainedType StandardSchema Csv.FromField a, ConstrainedType StandardSchema Csv.ToField a)
      , Maybe (ConstrainedType StandardSchema HasCsvFromRecord b, ConstrainedType StandardSchema HasCsvToRecord b)
      )
      -> Maybe (ConstrainedType StandardSchema HasCsvFromRecord (Row n a b), ConstrainedType StandardSchema HasCsvToRecord (Row n a b))
    f = \case
      (Just (ConstrainedType, ConstrainedType), Just (ConstrainedType, ConstrainedType)) -> Just (ConstrainedType, ConstrainedType)
      _ -> Nothing

class StandardSchemaConstraint (c :: * -> Constraint) where
  constrainStandardSchemaType :: StandardSchemaType a => Maybe (ConstrainedType StandardSchema c a)

instance StandardSchemaConstraint Typeable where
  -- All standard types support 'Typeable'.
  constrainStandardSchemaType = Just ConstrainedType

instance StandardSchemaConstraint TableKey where
  -- All standard types support 'TableKey'.
  constrainStandardSchemaType = Just ConstrainedType

instance StandardSchemaConstraint S.Serialize where
  -- All standard types support 'S.Serialize'.
  constrainStandardSchemaType = Just ConstrainedType

instance StandardSchemaConstraint Show where
  -- All standard types support 'Show'.
  constrainStandardSchemaType = Just ConstrainedType

-- CSV instances are optional.
instance StandardSchemaConstraint Csv.FromField where
  constrainStandardSchemaType = fst <$> hasCsvFromToField
instance StandardSchemaConstraint Csv.ToField where
  constrainStandardSchemaType = snd <$> hasCsvFromToField
instance StandardSchemaConstraint Csv.DefaultOrdered where
  constrainStandardSchemaType = hasCsvDefaultOrdered
instance StandardSchemaConstraint Csv.FromRecord where
  constrainStandardSchemaType = fst <$> hasCsvFromToRecord
instance StandardSchemaConstraint Csv.ToRecord where
  constrainStandardSchemaType = snd <$> hasCsvFromToRecord
instance StandardSchemaConstraint HasCsvColumnNames where
  constrainStandardSchemaType = hasHasCsvColumnNames
instance StandardSchemaConstraint HasCsvFromRecord where
  constrainStandardSchemaType = fst <$> hasHasCsvFromToRecord
instance StandardSchemaConstraint HasCsvToRecord where
  constrainStandardSchemaType = snd <$> hasHasCsvFromToRecord
