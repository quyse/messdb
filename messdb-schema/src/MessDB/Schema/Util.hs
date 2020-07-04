{-# LANGUAGE LambdaCase, TemplateHaskell #-}

module MessDB.Schema.Util
  ( genJsonSerialize
  ) where

import qualified Data.Aeson as J
import qualified Data.Serialize as S
import Language.Haskell.TH

-- | Generate 'J.FromJSON', 'J.toJSON', and 'S.Serialize' instances for given type.
-- 'S.Serialize' instance uses JSON representation, to be more stable to future changes to the type.
genJsonSerialize :: Name -> Q [Dec]
genJsonSerialize typeName = [d|
  instance J.FromJSON $(conT typeName) where
    parseJSON = J.genericParseJSON jsonOptions
  instance J.ToJSON $(conT typeName) where
    toJSON = J.genericToJSON jsonOptions
    toEncoding = J.genericToEncoding jsonOptions
  instance S.Serialize $(conT typeName) where
    put = S.put . J.encode
    get = maybe (fail $(litE $ stringL $ "wrong JSON for " <> nameBase typeName)) return . J.decode =<< S.get
  |]

jsonOptions :: J.Options
jsonOptions = J.defaultOptions
  { J.fieldLabelModifier = stripBeforeUnderscore
  , J.constructorTagModifier = stripBeforeUnderscore
  , J.sumEncoding = J.TaggedObject
    { J.tagFieldName = "type"
    , J.contentsFieldName = "data"
    }
  } where
  stripBeforeUnderscore = \case
    (x : xs) -> case x of
      '_' -> xs
      _ -> stripBeforeUnderscore xs
    [] -> []
