{-# LANGUAGE DataKinds, DeriveGeneric, FlexibleInstances, GADTs, KindSignatures, LambdaCase, MultiParamTypeClasses, TypeOperators #-}

module MessDB.Table.Row
  ( Row(..)
  , SomeRowField(..)
  , IsRow(..)
  , HasRowField(..)
  , HasCsvColumnNames(..)
  , HasCsvFromRecord(..)
  , HasCsvToRecord(..)
  , csvParseRecordWithHeader
  ) where

import qualified Data.Csv as Csv
import Data.Proxy
import qualified Data.Serialize as S
import Data.String
import Data.Typeable
import qualified Data.Vector as V
import GHC.Generics(Generic)
import GHC.TypeLits

-- | Row is a structure with named fields.
data Row (n :: Symbol) a b = Row !a !b deriving (Eq, Ord, Generic, Show)

-- | Witness for row having a field with particular name.
data SomeRowField n r where
  SomeRowField :: HasRowField n a r => Proxy a -> SomeRowField n r

class IsRow r where
  tryGetSomeRowField :: KnownSymbol n => Proxy n -> Proxy r -> Maybe (SomeRowField n r)

instance KnownSymbol n => IsRow (Row n a b) where
  tryGetSomeRowField pn pr = case p pn pr of
    Just Refl -> Just (q Proxy pn pr)
    Nothing -> Nothing
    where
      p :: (KnownSymbol n, KnownSymbol n') => Proxy n' -> Proxy (Row n a b) -> Maybe (n' :~: n)
      p Proxy Proxy = eqT
      q :: Proxy a -> Proxy n -> Proxy (Row n a b) -> SomeRowField n (Row n a b)
      q pf Proxy Proxy = SomeRowField pf

class HasRowField (n :: Symbol) a r where
  getRowField :: Proxy n -> r -> a
  setRowField :: Proxy n -> a -> r -> r

instance {-# OVERLAPS #-} HasRowField n a (Row n a b) where
  getRowField Proxy (Row a _b) = a
  setRowField Proxy a (Row _a b) = Row a b

instance {-# OVERLAPS #-} HasRowField n f b => HasRowField n f (Row m a b) where
  getRowField n (Row _a b) = getRowField n b
  setRowField n v (Row a b) = Row a (setRowField n v b)

instance (S.Serialize a, S.Serialize r) => S.Serialize (Row n a r) where
  put (Row a b) = do
    S.put a
    S.put b
  get = Row
    <$> S.get
    <*> S.get


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
