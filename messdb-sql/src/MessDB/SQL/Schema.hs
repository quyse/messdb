{-# LANGUAGE DataKinds, LambdaCase, OverloadedStrings, TupleSections, ViewPatterns #-}

module MessDB.SQL.Schema
  ( SqlSchema(..)
  , simpleName
  , simpleNames
  , SqlSchemaException(..)
  ) where

import Control.Monad
import Control.Monad.Trans.Except
import Data.Foldable
import Data.Int
import Data.Maybe
import qualified Data.Text as T
import Data.Typeable
import GHC.TypeLits
import qualified Language.SQL.SimpleSQL.Syntax as SS

import MessDB.Schema
import MessDB.Schema.Standard
import MessDB.Table
import MessDB.Table.Bytes
import MessDB.Table.Row

class SqlSchema e where
  -- | Construct schemas for key and value from SimpleSQL description of table.
  defineTableSchema :: [SS.TableElement] -> Except SqlSchemaException (ConstrainedSchema e TableKey, ConstrainedSchema e TableValue)

  defineColumnSchema :: SS.ColumnDef -> Except SqlSchemaException ([(T.Text, ConstrainedSchema e TableKey)], [(T.Text, ConstrainedSchema e TableValue)])

  defineFieldType :: SS.TypeName -> Except SqlSchemaException (Schema e)


instance SqlSchema StandardSchema where
  defineTableSchema = g . mconcat <=< mapM f where
    f = \case
      SS.TableColumnDef columnDef -> defineColumnSchema columnDef
      SS.TableConstraintDef _maybeName tableConstraint -> throwE $ SqlSchemaException_unsupportedTableConstraint tableConstraint
    g (keyFields, valueFields) =
      liftM2 (,) (rowSchemaFromFieldSchemas keyFields) (rowSchemaFromFieldSchemas valueFields)

  defineColumnSchema (SS.ColumnDef (simpleName -> fieldName) fieldTypeName maybeDefaultClause constraints) = do
    when (isJust maybeDefaultClause) $ throwE SqlSchemaException_unsupportedColumnDefaultClause
    ColumnConstraints
      { cs_primaryKey = isPrimaryKey
      , cs_notNull = isNotNull
      } <- foldConstraints constraints
    fieldSchema <- (if isNotNull then id else nullableSchema) <$> defineFieldType fieldTypeName
    -- return (fieldName, isPrimaryKey, fieldSchema)
    if isPrimaryKey
      then do
        keySchema <- maybe (throwE SqlSchemaException_unsupportedKeySchema) pure $ constrainSchema fieldSchema
        return ([(fieldName, keySchema)], [])
      else do
        valueSchema <- maybe (throwE SqlSchemaException_unsupportedValueSchema) pure $ constrainSchema fieldSchema
        return ([], [(fieldName, valueSchema)])

  defineFieldType t = case t of
    SS.TypeName (simpleNames -> name) -> case T.toLower name of
      "smallint" -> pure $ Schema (Proxy :: Proxy Int16)
      "int2" -> pure $ Schema (Proxy :: Proxy Int16)
      "int16" -> pure $ Schema (Proxy :: Proxy Int16)
      "integer" -> pure $ Schema (Proxy :: Proxy Int32)
      "int" -> pure $ Schema (Proxy :: Proxy Int32)
      "int4" -> pure $ Schema (Proxy :: Proxy Int32)
      "int32" -> pure $ Schema (Proxy :: Proxy Int32)
      "bigint" -> pure $ Schema (Proxy :: Proxy Int64)
      "int8" -> pure $ Schema (Proxy :: Proxy Int64)
      "int64" -> pure $ Schema (Proxy :: Proxy Int64)
      "real" -> pure $ Schema (Proxy :: Proxy Float)
      "double precision" -> pure $ Schema (Proxy :: Proxy Double)
      "bytes" -> pure $ Schema (Proxy :: Proxy Bytes)
      "text" -> pure $ Schema (Proxy :: Proxy T.Text)
      _ -> throwE $ SqlSchemaException_unsupportedType t
    _ -> throwE $ SqlSchemaException_unsupportedType t


rowSchemaFromFieldSchemas
  :: SchemaConstraintClass StandardSchema c
  => [(T.Text, ConstrainedSchema StandardSchema c)]
  -> Except SqlSchemaException (ConstrainedSchema StandardSchema c)
rowSchemaFromFieldSchemas
  ((someSymbolVal . T.unpack -> SomeSymbol fieldNameProxy, ConstrainedSchema typeProxy) : restFields) = do
  ConstrainedSchema restFieldsProxy <- rowSchemaFromFieldSchemas restFields
  maybe (throwE SqlSchemaException_unsupportedRowSchema) pure $
    constrainSchema $ Schema $ f fieldNameProxy typeProxy restFieldsProxy
  where
    f :: KnownSymbol n => Proxy n -> Proxy a -> Proxy b -> Proxy (Row n a b)
    f Proxy Proxy Proxy = Proxy
rowSchemaFromFieldSchemas [] = maybe (throwE SqlSchemaException_unsupportedRowSchema) pure $
  constrainSchema $ Schema (Proxy :: Proxy ())


data ColumnConstraints = ColumnConstraints
  { cs_primaryKey :: !Bool
  , cs_notNull :: !Bool
  }

foldConstraints :: [SS.ColConstraintDef] -> Except SqlSchemaException ColumnConstraints
foldConstraints = foldlM f ColumnConstraints
  { cs_primaryKey = False
  , cs_notNull = False
  } where
  f cs ccd@(SS.ColConstraintDef _ c) = case c of
    SS.ColNotNullConstraint -> return cs
      { cs_notNull = True
      }
    SS.ColPrimaryKeyConstraint -> return cs
      { cs_primaryKey = True
      }
    _ -> throwE $ SqlSchemaException_unsupportedColumnConstraint ccd

nullableSchema :: Schema StandardSchema -> Schema StandardSchema
nullableSchema (Schema p) = Schema (Just <$> p)

-- | Get simple name.
simpleName :: SS.Name -> T.Text
simpleName (SS.Name _ n) = T.pack n
-- | Get simple namespaced name.
simpleNames :: [SS.Name] -> T.Text
simpleNames = T.intercalate "." . map simpleName

data SqlSchemaException
  = SqlSchemaException_unsupportedType !SS.TypeName
  | SqlSchemaException_unsupportedKeySchema
  | SqlSchemaException_unsupportedValueSchema
  | SqlSchemaException_unsupportedRowSchema
  | SqlSchemaException_unsupportedTableConstraint !SS.TableConstraint
  | SqlSchemaException_unsupportedColumnConstraint !SS.ColConstraintDef
  | SqlSchemaException_unsupportedColumnDefaultClause
  deriving Show
