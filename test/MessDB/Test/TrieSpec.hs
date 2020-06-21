{-# LANGUAGE OverloadedLists, OverloadedStrings #-}

module MessDB.Test.TrieSpec
  ( spec
  ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Short as BS
import Data.Char
import qualified Data.Map.Lazy as M
import qualified Data.Vector as V
import Data.Word
import System.IO.Unsafe(unsafePerformIO)
import Test.Hspec
import Test.QuickCheck

import MessDB.Trie

import MessDB.Test.Lib

spec :: Spec
spec = describe "Trie" $ do
  it "Empty trie" $ checkTrie emptyTrie
  it "Singleton trie" $ checkTrie $ singletonTrie "abc" "def"
  it "Random trie 1" $ property $ checkTrie <$> arbitraryTrie (0, 3) 3
  it "Random trie 2" $ property $ checkTrie <$> arbitraryTrie (50, 100) 3
  it "Random trie 3" $ property $ checkTrie <$> arbitraryTrie (100, 200) 3
  it "Random trie 4" $ property $ checkTrie <$> arbitraryTrie (0, 1000) 26
  it "Merge zero tries" $ checkTrie $ mergeTries testMemoryStore testMemoryStore foldToLast []
  it "Merge with itself" $ property $ do
    trie <- arbitraryTrie (0, 100) 3
    mergeCount <- choose (0, 50)
    return $ checkTrie $ mergeTries testMemoryStore testMemoryStore foldToLast (V.replicate mergeCount trie)
  it "Sort random trie 1" $ property $ checkTrie . checkedTrieSort transformValueToKey foldToLast <$> arbitraryTrie (0, 3) 3
  it "Sort random trie 2" $ property $ checkTrie . checkedTrieSort transformValueToKey foldToLast <$> arbitraryTrie (50, 100) 3
  it "Sort random trie 3" $ property $ checkTrie . checkedTrieSort transformValueToKey foldToLast <$> arbitraryTrie (100, 200) 3
  it "Sort random trie 4" $ property $ checkTrie . checkedTrieSort transformValueToKey foldToLast <$> arbitraryTrie (0, 100) 26
  it "Sort random trie 5" $ property $ checkTrie . checkedTrieSort transformValueToKey foldToLast <$> arbitraryTrie (100, 1000) 26

checkedTrieSort :: TransformFunc -> FoldFunc -> Trie -> Trie
checkedTrieSort transformFunc@Func
  { func_func = transform
  } foldFunc@Func
  { func_func = fold
  } trie = let
  items = V.fromList $ trieToItems trie
  sortedItems = M.toAscList $ V.foldl (\m (k, v) -> M.alter (maybe (Just v) (\v' -> Just $ fold k v' v)) k m) M.empty $ V.map (uncurry transform) items
  in printFailedTrie sortedItems $ checkedTrieItems sortedItems $ sortTrie testMemoryStore testMemoryStore transformFunc foldFunc trie

transformValueToKey :: TransformFunc
transformValueToKey = Func
  { func_key = "value_to_key"
  , func_func = \_k v -> (Key $ BS.toShort v, v)
  }

checkedTrieItems :: [(Key, Value)] -> Trie -> Trie
checkedTrieItems items trie = if items == trieToItems trie
  then trie
  else unsafePerformIO $ do
    putStrLn "BEGIN checked trie items"
    print items
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
