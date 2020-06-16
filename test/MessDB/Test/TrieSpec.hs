{-# LANGUAGE OverloadedLists, OverloadedStrings #-}

module MessDB.Test.TrieSpec
  ( spec
  ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Short as BS
import Data.Char
import qualified Data.Vector as V
import Data.Word
import System.IO.Unsafe(unsafePerformIO)
import Test.Hspec
import Test.QuickCheck

import MessDB.Trie
import MessDB.Store.Memory

spec :: Spec
spec = describe "Trie" $ do
  it "Empty trie" $ checkTrie emptyTrie
  it "Singleton trie" $ checkTrie $ singletonTrie "abc" "def"
  it "Random trie 1" $ property $ checkTrie <$> arbitraryTrie (0, 3) 3
  it "Random trie 2" $ property $ checkTrie <$> arbitraryTrie (0, 100) 26
  it "Random trie 3" $ property $ checkTrie <$> arbitraryTrie (100, 1000) 26
  it "Merge zero tries" $ checkTrie $ mergeTries store store OP_FOLD_TO_LAST foldToLast []
  it "Merge with itself" $ property $ do
    trie <- arbitraryTrie (0, 3) 3
    mergeCount <- choose (0, 5)
    return $ checkTrie $ mergeTries store store OP_FOLD_TO_LAST foldToLast (V.replicate mergeCount trie)

arbitraryTrie :: (Int, Int) -> Int -> Gen Trie
arbitraryTrie itemsCountRange alphabetSize = do
  itemsCount <- choose itemsCountRange
  items <- V.fromList <$> vectorOf itemsCount (arbitraryPair alphabetSize)
  return $ printFailedTree items $ itemsToTrie store store items

arbitraryPair :: Int -> Gen (BS.ShortByteString, B.ByteString)
arbitraryPair alphabetSize = do
  k <- BS.pack <$> listOf (arbitraryByte alphabetSize)
  v <- B.pack <$> listOf (arbitraryByte alphabetSize)
  return (k, v)

arbitraryByte :: Int -> Gen Word8
arbitraryByte alphabetSize = choose (fromIntegral $ ord 'a', fromIntegral $ ord 'a' + alphabetSize - 1)

{-# NOINLINE store #-}
store :: MemoryStore
store = unsafePerformIO newMemoryStoreIO

printFailedTree :: Show a => a -> Trie -> Trie
printFailedTree a trie = if checkTrie trie
  then trie
  else unsafePerformIO $ do
    putStrLn "BEGIN failed tree"
    print a
    putStrLn "BEGIN failed tree itself"
    debugPrintTrie trie
    putStrLn "END failed tree"
    return trie
