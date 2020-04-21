module MessDB.Store.File
  ( FileStore(..)
  ) where

import Control.Exception
import Control.Monad
import qualified Data.ByteString.Lazy as BL
import System.Directory
import System.IO

import MessDB.Node

newtype FileStore = FileStore String

instance Store FileStore where
  storeSave (FileStore pathPrefix) hash io = do
    exists <- handle (\SomeException {} -> return False) $ withFile path ReadMode $ const $ return True
    unless exists $ do
      BL.writeFile tmpPath =<< io
      renameFile tmpPath path
    where
      path = pathPrefix <> nodeHashToString hash
      tmpPath = path <> ".tmp"

  storeLoad (FileStore pathPrefix) hash = BL.readFile path
    where
      path = pathPrefix <> nodeHashToString hash
