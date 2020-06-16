module MessDB.Store.File
  ( FileStore(..)
  ) where

import Control.Exception
import Control.Monad
import qualified Data.ByteString.Lazy as BL
import System.Directory
import System.IO

import MessDB.Store

newtype FileStore = FileStore String

instance Store FileStore where
  storeSave (FileStore pathPrefix) key io = do
    exists <- handle (\SomeException {} -> return False) $ withFile path ReadMode $ const $ return True
    unless exists $ do
      BL.writeFile tmpPath =<< io
      renameFile tmpPath path
    where
      path = pathPrefix <> storeKeyToString key
      tmpPath = path <> ".tmp"

  storeLoad (FileStore pathPrefix) key = BL.readFile path
    where
      path = pathPrefix <> storeKeyToString key
