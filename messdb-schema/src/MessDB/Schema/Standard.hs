{-# LANGUAGE ConstraintKinds, DeriveGeneric, LambdaCase, ScopedTypeVariables, TypeFamilies, TypeOperators, ViewPatterns #-}

module MessDB.Schema.Standard
  ( StandardSchema(..)
  ) where

import Control.Monad
import qualified Data.ByteString as B
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
  | StandardSchema_ByteString
  | StandardSchema_Text
  | StandardSchema_Maybe !StandardSchema
  | StandardSchema_Tuple2 !StandardSchema !StandardSchema
  | StandardSchema_Tuple3 !StandardSchema !StandardSchema !StandardSchema
  | StandardSchema_Tuple4 !StandardSchema !StandardSchema !StandardSchema !StandardSchema
  | StandardSchema_Row !T.Text !StandardSchema !StandardSchema
  deriving Generic

instance S.Serialize StandardSchema

instance SchemaEncoding StandardSchema where
  type SchemaTypeClass StandardSchema = StandardSchemaType
  type SchemaConstraintClass StandardSchema = StandardSchemaConstraint

  encodeSchema = standardSchema

  decodeSchema = decode where
    -- monomorphic
    decode = \case
      StandardSchema_Empty -> packWithConstraint (Proxy :: Proxy ())
      StandardSchema_Int64 -> packWithConstraint (Proxy :: Proxy Int64)
      StandardSchema_Int32 -> packWithConstraint (Proxy :: Proxy Int32)
      StandardSchema_Int16 -> packWithConstraint (Proxy :: Proxy Int16)
      StandardSchema_Int8 -> packWithConstraint (Proxy :: Proxy Int8)
      StandardSchema_Word64 -> packWithConstraint (Proxy :: Proxy Word64)
      StandardSchema_Word32 -> packWithConstraint (Proxy :: Proxy Word32)
      StandardSchema_Word16 -> packWithConstraint (Proxy :: Proxy Word16)
      StandardSchema_Word8 -> packWithConstraint (Proxy :: Proxy Word8)
      StandardSchema_Float -> packWithConstraint (Proxy :: Proxy Float)
      StandardSchema_Double -> packWithConstraint (Proxy :: Proxy Double)
      StandardSchema_ByteString -> packWithConstraint (Proxy :: Proxy B.ByteString)
      StandardSchema_Text -> packWithConstraint (Proxy :: Proxy T.Text)
      StandardSchema_Maybe e -> do
        Schema p <- decode e
        packWithConstraint $ liftM Just p
      StandardSchema_Tuple2 e1 e2 -> do
        Schema p1 <- decode e1
        Schema p2 <- decode e2
        packWithConstraint $ liftM2 ((,)) p1 p2
      StandardSchema_Tuple3 e1 e2 e3 -> do
        Schema p1 <- decode e1
        Schema p2 <- decode e2
        Schema p3 <- decode e3
        packWithConstraint $ liftM3 ((,,)) p1 p2 p3
      StandardSchema_Tuple4 e1 e2 e3 e4 -> do
        Schema p1 <- decode e1
        Schema p2 <- decode e2
        Schema p3 <- decode e3
        Schema p4 <- decode e4
        packWithConstraint $ liftM4 ((,,,)) p1 p2 p3 p4
      StandardSchema_Row n ea eb -> do
        SomeSymbol (Proxy :: Proxy pn) <- pure (someSymbolVal (T.unpack n))
        Schema (Proxy :: Proxy pa) <- decode ea
        Schema (Proxy :: Proxy pb) <- decode eb
        packWithConstraint (Proxy :: Proxy (Row pn pa pb))

  constrainSchemaType = f Proxy Proxy where
    recode :: (StandardSchemaType a, StandardSchemaConstraint c) => Proxy a -> Proxy c -> Maybe (Schema StandardSchema c)
    recode a Proxy = decodeSchema $ encodeSchema a
    f :: (StandardSchemaType a, StandardSchemaConstraint c) => Proxy a -> Proxy c -> Maybe (ConstrainedType StandardSchema c a)
    f a c = case recode a c of
      Just (Schema a') -> let
        g :: (Typeable a, Typeable a') => Proxy a -> Proxy a' -> Maybe (a :~: a')
        g Proxy Proxy = eqT
        in case g a a' of
          Just Refl -> Just ConstrainedType
          Nothing -> Nothing
      Nothing -> Nothing

class (Typeable t, TableKey t, S.Serialize t) => StandardSchemaType t where
  standardSchema :: Proxy t -> StandardSchema

instance StandardSchemaType () where
  standardSchema Proxy = StandardSchema_Empty
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
instance StandardSchemaType B.ByteString where
  standardSchema Proxy = StandardSchema_ByteString
instance StandardSchemaType T.Text where
  standardSchema Proxy = StandardSchema_Text
instance StandardSchemaType a => StandardSchemaType (Maybe a) where
  standardSchema Proxy = StandardSchema_Maybe (standardSchema (Proxy :: Proxy a))
instance (StandardSchemaType a, StandardSchemaType b) => StandardSchemaType (a, b) where
  standardSchema Proxy = StandardSchema_Tuple2
    (standardSchema (Proxy :: Proxy a))
    (standardSchema (Proxy :: Proxy b))
instance (StandardSchemaType a, StandardSchemaType b, StandardSchemaType c) => StandardSchemaType (a, b, c) where
  standardSchema Proxy = StandardSchema_Tuple3
    (standardSchema (Proxy :: Proxy a))
    (standardSchema (Proxy :: Proxy b))
    (standardSchema (Proxy :: Proxy c))
instance (StandardSchemaType a, StandardSchemaType b, StandardSchemaType c, StandardSchemaType d) => StandardSchemaType (a, b, c, d) where
  standardSchema Proxy = StandardSchema_Tuple4
    (standardSchema (Proxy :: Proxy a))
    (standardSchema (Proxy :: Proxy b))
    (standardSchema (Proxy :: Proxy c))
    (standardSchema (Proxy :: Proxy d))
instance (KnownSymbol n, StandardSchemaType a, StandardSchemaType b) => StandardSchemaType (Row n a b) where
  standardSchema Proxy = StandardSchema_Row
    (T.pack (symbolVal (Proxy :: Proxy n)))
    (standardSchema (Proxy :: Proxy a))
    (standardSchema (Proxy :: Proxy b))

class StandardSchemaConstraint (c :: * -> Constraint) where
  packWithConstraint :: StandardSchemaType t => Proxy t -> Maybe (Schema StandardSchema c)

instance StandardSchemaConstraint Typeable where
  -- All standard types support 'Typeable'.
  packWithConstraint = Just . Schema

instance StandardSchemaConstraint TableKey where
  -- All standard types support 'TableKey'.
  packWithConstraint = Just . Schema

instance StandardSchemaConstraint S.Serialize where
  -- All standard types support 'S.Serialize'.
  packWithConstraint = Just . Schema
