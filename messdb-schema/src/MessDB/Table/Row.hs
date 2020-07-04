{-# LANGUAGE DataKinds, FlexibleInstances, GADTs, KindSignatures, MultiParamTypeClasses, TypeOperators #-}

module MessDB.Table.Row
  ( Row(..)
  , SomeRowField(..)
  , IsRow(..)
  , HasRowField(..)
  ) where

import Data.Proxy
import qualified Data.Serialize as S
import Data.Typeable

import GHC.TypeLits

-- | Row is a structure with named fields.
data Row (n :: Symbol) a b = Row !a !b deriving (Eq, Ord)

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
