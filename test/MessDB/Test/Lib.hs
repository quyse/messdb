module MessDB.Test.Lib
  ( testMemoryStore
  ) where

import System.IO.Unsafe(unsafePerformIO)

import MessDB.Store.Memory

{-# NOINLINE testMemoryStore #-}
testMemoryStore :: MemoryStore
testMemoryStore = unsafePerformIO newMemoryStoreIO
