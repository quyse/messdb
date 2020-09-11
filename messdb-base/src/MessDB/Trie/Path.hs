{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, ViewPatterns #-}

module MessDB.Trie.Path
  ( Path()
  , pathPack
  , pathUnpack
  , pathLength
  , pathNull
  , pathMaybeFirstByte
  , bytesToPath
  , bytesFromPath
  , PathRange(..)
  , PathRangeEnd(..)
  , pathRangeIncludes
  , PathRangeRelation(..)
  , pathPrefixRangeRelation
  ) where

import Data.Bits
import qualified Data.ByteString.Short as BS
import Data.List
import Data.Maybe
import qualified Data.Serialize as S
import Data.Word

-- | Path to trie node.
-- Currently stored as a bytestring, but every byte keeps only 4 bits.
newtype Path = Path
  { unPath :: BS.ShortByteString
  } deriving (Eq, Ord, Semigroup, Monoid, S.Serialize, Show)

pathPack :: [Word8] -> Path
pathPack = Path . BS.pack

pathUnpack :: Path -> [Word8]
pathUnpack = BS.unpack . unPath

pathLength :: Path -> Int
pathLength = BS.length . unPath

pathNull :: Path -> Bool
pathNull = BS.null . unPath

-- | First byte of path, or Nothing if it's empty.
pathMaybeFirstByte :: Path -> Maybe Word8
pathMaybeFirstByte (Path bytes) = if BS.null bytes
  then Nothing
  else Just $ BS.index bytes 0

bytesToPath :: BS.ShortByteString -> Path
bytesToPath = Path . BS.pack . splitBytes . BS.unpack where
  splitBytes (x : xs) = (x `shiftR` 4) : (x .&. 0x0f) : splitBytes xs
  splitBytes [] = []

bytesFromPath :: Path -> BS.ShortByteString
bytesFromPath = BS.pack . joinBytes . BS.unpack . unPath where
  joinBytes (x1 : x0 : xs) = ((x1 `shiftL` 4) .|. x0) : joinBytes xs
  joinBytes [] = []
  joinBytes _ = error "not even half-byte string"

-- | Range of paths.
data PathRange = PathRange !PathRangeEnd !PathRangeEnd

instance S.Serialize PathRange where
  put (PathRange a b) = do
    S.put a
    S.put b
  get = do
    a <- S.get
    b <- S.get
    return $ PathRange a b

-- | End of path range.
data PathRangeEnd
  = PathRangeEnd_inclusive {-# UNPACK #-} !Path
  | PathRangeEnd_exclusive {-# UNPACK #-} !Path
  | PathRangeEnd_infinite

instance S.Serialize PathRangeEnd where
  put = \case
    PathRangeEnd_inclusive k -> do
      S.putWord8 0
      S.put k
    PathRangeEnd_exclusive k -> do
      S.putWord8 1
      S.put k
    PathRangeEnd_infinite ->
      S.putWord8 2
  get = do
    f <- S.getWord8
    case f of
      0 -> PathRangeEnd_inclusive <$> S.get
      1 -> PathRangeEnd_exclusive <$> S.get
      2 -> return PathRangeEnd_infinite
      _ -> fail "wrong path range end type"

-- | Calculate relation between single item and range.
pathRangeIncludes :: Path -> PathRange -> Bool
pathRangeIncludes (Path path) (PathRange lowerEnd upperEnd) = inLower && inUpper where
  inLower = case lowerEnd of
    PathRangeEnd_inclusive (Path end) -> path >= end
    PathRangeEnd_exclusive (Path end) -> path > end
    PathRangeEnd_infinite -> True
  inUpper = case upperEnd of
    PathRangeEnd_inclusive (Path end) -> path <= end
    PathRangeEnd_exclusive (Path end) -> path < end
    PathRangeEnd_infinite -> True

-- | Relation between something and path range.
data PathRangeRelation
  = PathRangeRelation_in
  | PathRangeRelation_out
  | PathRangeRelation_intersects
  deriving Eq

-- | Calculate relation between prefix and range.
pathPrefixRangeRelation :: Path -> PathRange -> PathRangeRelation
pathPrefixRangeRelation (Path prefix) (PathRange lowerEnd upperEnd) = let
  isPrefixInLower = case lowerEnd of
    PathRangeEnd_inclusive (Path end) -> prefix >= end
    PathRangeEnd_exclusive (Path end) -> prefix > end
    PathRangeEnd_infinite -> True
  isPrefixInUpper = case upperEnd of
    PathRangeEnd_inclusive (Path end) -> isPrefixLessUpper end
    PathRangeEnd_exclusive (Path end) -> isPrefixLessUpper end
    PathRangeEnd_infinite -> True
  isPrefixIn = isPrefixInLower && isPrefixInUpper

  isPrefixOutLower = case lowerEnd of
    PathRangeEnd_inclusive (Path end) -> isPrefixLessUpper end
    PathRangeEnd_exclusive (Path end) -> isPrefixLessUpper end
    PathRangeEnd_infinite -> False
  isPrefixOutUpper = case upperEnd of
    PathRangeEnd_inclusive (Path end) -> prefix > end
    PathRangeEnd_exclusive (Path end) -> prefix >= end
    PathRangeEnd_infinite -> False
  isPrefixOut = isPrefixOutLower || isPrefixOutUpper

  isPrefixLessUpper end = prefix < end && (BS.length prefix >= BS.length end || isNothing (stripPrefix (BS.unpack prefix) (BS.unpack end)))

  in if isPrefixIn
    then PathRangeRelation_in
    else if isPrefixOut
      then PathRangeRelation_out
      else PathRangeRelation_intersects
