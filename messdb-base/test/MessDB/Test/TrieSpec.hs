{-# LANGUAGE OverloadedLists, OverloadedStrings #-}

module MessDB.Test.TrieSpec
  ( spec
  ) where

import Control.Monad
import qualified Data.ByteString as B
import qualified Data.ByteString.Short as BS
import Data.Char
import qualified Data.Map.Lazy as M
import qualified Data.Vector as V
import Data.Word
import System.IO.Unsafe(unsafePerformIO)
import Test.Hspec
import Test.QuickCheck

import MessDB.Store.Memory
import MessDB.Trie

spec :: Spec
spec = describe "Trie" $ do
  describe "Trie correctness" $ do
    it "Empty trie" $ checkTrie emptyTrie
    it "Singleton trie" $ checkTrie $ singletonTrie "abc" "def"
    it "Random trie 1" $ property $ checkTrie <$> arbitraryTrie (0, 3) 3
    it "Random trie 2" $ property $ checkTrie <$> arbitraryTrie (50, 100) 3
    it "Random trie 3" $ property $ checkTrie <$> arbitraryTrie (100, 200) 3
    it "Random trie 4" $ property $ checkTrie <$> arbitraryTrie (0, 1000) 26

  describe "Trie merge" $ do
    it "Merge zero tries" $ checkTrie $ mergeTries testMemoryStore testMemoryStore foldToLast []
    it "Merge 2 empty tries" $ checkTrie $ mergeTries testMemoryStore testMemoryStore foldToLast [emptyTrie, emptyTrie]
    it "Merge 3 empty tries" $ checkTrie $ mergeTries testMemoryStore testMemoryStore foldToLast [emptyTrie, emptyTrie, emptyTrie]
    it "Merge with itself" $ property $ do
      trie <- arbitraryTrie (0, 100) 3
      mergeCount <- choose (0, 50)
      return $ checkTrie $ mergeTries testMemoryStore testMemoryStore foldToLast (V.replicate mergeCount trie)

  describe "Trie sort" $ do
    it "Random trie 1" $ property $ checkTrie . checkedTrieSort testTransform foldToLast <$> arbitraryTrie (0, 3) 3
    it "Random trie 2" $ property $ checkTrie . checkedTrieSort testTransform foldToLast <$> arbitraryTrie (50, 100) 3
    it "Random trie 3" $ property $ checkTrie . checkedTrieSort testTransform foldToLast <$> arbitraryTrie (100, 200) 3
    it "Random trie 4" $ property $ checkTrie . checkedTrieSort testTransform foldToLast <$> arbitraryTrie (0, 100) 26
    it "Random trie 5" $ property $ checkTrie . checkedTrieSort testTransform foldToLast <$> arbitraryTrie (100, 1000) 26

  describe "Range functions" $ do
    describe "keyRangeIncludes" $ do
      it "Infinite" $ property $ keyRangeIncludes (Key "abc") (KeyRange KeyRangeEnd_infinite KeyRangeEnd_infinite)
      it "Inclusive 1" $ property $ keyRangeIncludes (Key "abc") (KeyRange (KeyRangeEnd_inclusive "a") (KeyRangeEnd_inclusive "b"))
      it "Inclusive 2" $ property $ keyRangeIncludes (Key "abc") (KeyRange (KeyRangeEnd_inclusive "abc") (KeyRangeEnd_inclusive "abc"))
      it "Exclusive fail 1" $ property $ not $ keyRangeIncludes (Key "abc") (KeyRange (KeyRangeEnd_exclusive "abc") (KeyRangeEnd_inclusive "b"))
      it "Exclusive fail 2" $ property $ not $ keyRangeIncludes (Key "abc") (KeyRange (KeyRangeEnd_inclusive "abc") (KeyRangeEnd_exclusive "abc"))
    describe "keyPrefixRangeRelation" $ do
      it "1" $ property $ keyPrefixRangeRelation (Key "") (KeyRange KeyRangeEnd_infinite KeyRangeEnd_infinite) == KeyRangeRelation_in
      it "2" $ property $ keyPrefixRangeRelation (Key "") (KeyRange (KeyRangeEnd_inclusive "") KeyRangeEnd_infinite) == KeyRangeRelation_in
      it "3" $ property $ keyPrefixRangeRelation (Key "") (KeyRange (KeyRangeEnd_exclusive "") KeyRangeEnd_infinite) == KeyRangeRelation_intersects
      it "4" $ property $ keyPrefixRangeRelation (Key "") (KeyRange (KeyRangeEnd_inclusive "") (KeyRangeEnd_inclusive "abc")) == KeyRangeRelation_intersects
      it "5" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange KeyRangeEnd_infinite KeyRangeEnd_infinite) == KeyRangeRelation_in
      it "6" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange (KeyRangeEnd_inclusive "") KeyRangeEnd_infinite) == KeyRangeRelation_in
      it "7" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange (KeyRangeEnd_inclusive "abc") KeyRangeEnd_infinite) == KeyRangeRelation_in
      it "8" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange (KeyRangeEnd_exclusive "abc") KeyRangeEnd_infinite) == KeyRangeRelation_intersects
      it "9" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange (KeyRangeEnd_inclusive "abc") (KeyRangeEnd_inclusive "abd")) == KeyRangeRelation_in
      it "10" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange (KeyRangeEnd_inclusive "abc") (KeyRangeEnd_exclusive "abca")) == KeyRangeRelation_intersects
      it "11" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange (KeyRangeEnd_inclusive "abc") (KeyRangeEnd_inclusive "abc")) == KeyRangeRelation_intersects
      it "12" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange KeyRangeEnd_infinite (KeyRangeEnd_inclusive "abc")) == KeyRangeRelation_intersects
      it "13" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange KeyRangeEnd_infinite (KeyRangeEnd_exclusive "abc")) == KeyRangeRelation_out
      it "14" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange (KeyRangeEnd_inclusive "abca") KeyRangeEnd_infinite) == KeyRangeRelation_intersects
      it "15" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange (KeyRangeEnd_exclusive "abc") KeyRangeEnd_infinite) == KeyRangeRelation_intersects
      it "16" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange (KeyRangeEnd_inclusive "abd") KeyRangeEnd_infinite) == KeyRangeRelation_out
      it "17" $ property $ keyPrefixRangeRelation (Key "abc") (KeyRange (KeyRangeEnd_exclusive "abd") KeyRangeEnd_infinite) == KeyRangeRelation_out

  describe "Trie range filter" $ do
    it "Random trie 1" $ property $ checkTrie <$> liftM2 checkedTrieRange (arbitraryTrie (0, 3) 3) (arbitraryRange 3)
    it "Random trie 2" $ property $ checkTrie <$> liftM2 checkedTrieRange (arbitraryTrie (0, 10) 3) (arbitraryRange 3)
    it "Random trie 3" $ property $ checkTrie <$> liftM2 checkedTrieRange (arbitraryTrie (0, 30) 3) (arbitraryRange 3)
    it "Random trie 4" $ property $ checkTrie <$> liftM2 checkedTrieRange (arbitraryTrie (50, 100) 3) (arbitraryRange 3)
    it "Random trie 5" $ property $ checkTrie <$> liftM2 checkedTrieRange (arbitraryTrie (50, 100) 26) (arbitraryRange 26)

checkedTrieSort :: TransformFunc -> FoldFunc -> Trie -> Trie
checkedTrieSort transformFunc@Func
  { func_func = transform
  } foldFunc@Func
  { func_func = fold
  } trie = let
  items = V.fromList $ trieToItems trie
  sortedItems = M.toAscList $ V.foldl (\m (k, v) -> M.alter (maybe (Just v) (\v' -> Just $ fold k v' v)) k m) M.empty $ V.map (uncurry transform) items
  in printFailedTrie sortedItems $ checkedTrieItems sortedItems $ sortTrie testMemoryStore testMemoryStore transformFunc foldFunc trie

checkedTrieRange :: Trie -> KeyRange -> Trie
checkedTrieRange trie range = let
  items = trieToItems trie
  filteredItems = filter (\(k, _v) -> keyRangeIncludes k range) items
  in printFailedTrie filteredItems $ checkedTrieItems filteredItems $ rangeFilterTrie testMemoryStore testMemoryStore range trie

testTransform :: TransformFunc
testTransform = Func
  { func_key = "test_transform"
  , func_func = \(Key k) v -> (Key $ BS.toShort $ B.reverse (BS.fromShort k) <> B.reverse v, v)
  }

checkedTrieItems :: [(Key, Value)] -> Trie -> Trie
checkedTrieItems items trie = if items == trieToItems trie
  then trie
  else unsafePerformIO $ do
    putStrLn "BEGIN checked trie expected items"
    print items
    putStrLn "BEGIN checked trie real items"
    print $ trieToItems trie
    putStrLn "BEGIN checked trie"
    debugPrintTrie trie
    putStrLn "END checked trie"
    fail "checkedTrieItems: mismatch"

arbitraryTrie :: (Int, Int) -> Int -> Gen Trie
arbitraryTrie itemsCountRange alphabetSize = do
  itemsCount <- choose itemsCountRange
  items <- V.fromList <$> vectorOf itemsCount (arbitraryPair alphabetSize)
  let
    uniqueItems = M.toAscList $ V.foldl (\m (k, v) -> M.insert k v m) M.empty items
  return $ printFailedTrie items $ checkedTrieItems uniqueItems $ itemsToTrie testMemoryStore testMemoryStore items

arbitraryPair :: Int -> Gen (Key, Value)
arbitraryPair alphabetSize = do
  k <- Key . BS.pack <$> listOf (arbitraryByte alphabetSize)
  v <- B.pack <$> listOf (arbitraryByte alphabetSize)
  return (k, v)

arbitraryByte :: Int -> Gen Word8
arbitraryByte alphabetSize = choose (fromIntegral $ ord 'a', fromIntegral $ ord 'a' + alphabetSize - 1)

arbitraryRange :: Int -> Gen KeyRange
arbitraryRange alphabetSize = liftM2 KeyRange arbitraryRangeEnd arbitraryRangeEnd where
  arbitraryRangeEnd = do
    endType <- choose (-1, 9)
    if endType < (0 :: Int)
      then return KeyRangeEnd_infinite
      else
        (if endType `rem` 2 == 0 then KeyRangeEnd_inclusive else KeyRangeEnd_exclusive)
          . Key . BS.pack <$> listOf (arbitraryByte alphabetSize)

printFailedTrie :: Show a => a -> Trie -> Trie
printFailedTrie a trie = if checkTrie trie
  then trie
  else unsafePerformIO $ do
    putStrLn "BEGIN failed tree"
    print a
    putStrLn "BEGIN failed tree itself"
    debugPrintTrie trie
    putStrLn "END failed tree"
    return trie

{-# NOINLINE testMemoryStore #-}
testMemoryStore :: MemoryStore
testMemoryStore = unsafePerformIO newMemoryStoreIO
