{-# LANGUAGE DataKinds, DeriveGeneric, KindSignatures, LambdaCase #-}

module MessDB.Table.Row
  ( Row(..)
  , RowFromJson(..)
  , RowToJson(..)
  , HasCsvColumnNames(..)
  , HasCsvFromRecord(..)
  , HasCsvToRecord(..)
  , csvParseRecordWithHeader
  ) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Csv as Csv
import qualified Data.HashMap.Strict as HM
import Data.Proxy
import qualified Data.Serialize as S
import Data.String
import qualified Data.Vector as V
import GHC.Generics(Generic)
import GHC.TypeLits

-- | Row is a structure with named fields.
data Row (n :: Symbol) a b = Row !a !b deriving (Eq, Ord, Generic, Show)

instance (S.Serialize a, S.Serialize b) => S.Serialize (Row n a b) where
  put (Row a b) = do
    S.put a
    S.put b
  get = Row
    <$> S.get
    <*> S.get


-- JSON instances

-- Actual Aeson instances

instance (KnownSymbol n, J.FromJSON a, RowFromJson b) => J.FromJSON (Row n a b) where
  parseJSON = J.withObject "row" rowParseJson

instance (KnownSymbol n, J.ToJSON a, RowToJson b) => J.ToJSON (Row n a b) where
  toJSON = J.Object . rowToJson
  toEncoding = J.toEncoding . rowToJson -- does not use efficiency of 'J.Encoding' yet

-- JSON helper classes and instances

class RowFromJson a where
  rowParseJson :: J.Object -> J.Parser a
instance (KnownSymbol n, J.FromJSON a, RowFromJson b) => RowFromJson (Row n a b) where
  rowParseJson = f Proxy where
    f :: (KnownSymbol n, J.FromJSON a, RowFromJson b) => Proxy n -> J.Object -> J.Parser (Row n a b)
    f pn fields = do
      a <- fields J..: (fromString $ symbolVal pn)
      Row a <$> rowParseJson fields
instance RowFromJson () where
  rowParseJson _ = return ()

class RowToJson a where
  rowToJson :: a -> J.Object
instance (KnownSymbol n, J.ToJSON a, RowToJson b) => RowToJson (Row n a b) where
  rowToJson = f Proxy where
    f :: (KnownSymbol n, J.ToJSON a, RowToJson b) => Proxy n -> Row n a b -> J.Object
    f pn (Row a b) = HM.insert (fromString $ symbolVal pn) (J.toJSON a) (rowToJson b)
instance RowToJson () where
  rowToJson () = HM.empty


-- CSV instances

-- Actual Cassava instances

instance (KnownSymbol n, HasCsvColumnNames b) => Csv.DefaultOrdered (Row n a b) where
  headerOrder = f Proxy where
    f :: (KnownSymbol n, HasCsvColumnNames b) => Proxy (Row n a b) -> Row n a b -> Csv.Header
    f p _ = V.fromList $ map fromString $ csvColumnNames p

instance (Csv.FromField a, HasCsvFromRecord b) => Csv.FromRecord (Row n a b) where
  parseRecord = csvParseRecord . V.toList

instance (Csv.ToField a, HasCsvToRecord b) => Csv.ToRecord (Row n a b) where
  toRecord = V.fromList . csvToRecord


-- CSV helper classes and instances

class HasCsvColumnNames a where
  csvColumnNames :: Proxy a -> [String]
instance (KnownSymbol n, HasCsvColumnNames b) => HasCsvColumnNames (Row n a b) where
  csvColumnNames = f Proxy Proxy where
    f :: (KnownSymbol n, HasCsvColumnNames b) => Proxy n -> Proxy b -> Proxy (Row n a b) -> [String]
    f pn pb Proxy = symbolVal pn : csvColumnNames pb
instance HasCsvColumnNames () where
  csvColumnNames Proxy = []

class HasCsvFromRecord a where
  csvParseRecord :: [Csv.Field] -> Csv.Parser a
instance (Csv.FromField a, HasCsvFromRecord b) => HasCsvFromRecord (Row n a b) where
  csvParseRecord = \case
    (field : restFields) -> do
      a <- Csv.parseField field
      Row a <$> csvParseRecord restFields
    [] -> mempty -- fail because of not enough fields
instance HasCsvFromRecord () where
  csvParseRecord = \case
    [] -> return ()
    _ -> mempty -- fail because of extra fields

class HasCsvToRecord a where
  csvToRecord :: a -> [Csv.Field]
instance (Csv.ToField a, HasCsvToRecord b) => HasCsvToRecord (Row n a b) where
  csvToRecord (Row a b) = Csv.toField a : csvToRecord b
instance HasCsvToRecord () where
  csvToRecord () = []

-- | Parse record given field order.
-- More efficient than 'Csv.parseNamedRecord'.
csvParseRecordWithHeader :: (HasCsvColumnNames a, HasCsvFromRecord a) => V.Vector String -> Maybe (V.Vector Csv.Field -> Csv.Parser a)
csvParseRecordWithHeader = f Proxy where
  f :: (HasCsvColumnNames a, HasCsvFromRecord a) => Proxy a -> V.Vector String -> Maybe (V.Vector Csv.Field -> Csv.Parser a)
  f p fieldOrder = do
    columns <- mapM (`V.elemIndex` fieldOrder) (csvColumnNames p)
    return $ \fields -> csvParseRecord $ map (fields V.!) columns
