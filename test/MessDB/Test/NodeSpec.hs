module MessDB.Test.NodeSpec
  ( spec
  ) where

import Test.Hspec

import MessDB.Node

spec :: Spec
spec = describe "Node" $ do
  it "Tree should be correct" $
    checkTree $ itemsToTree $ [ValueItem k k | k <- [1 :: Int .. 1000000]]
  it "Unordered tree should be incorrect" $
    not $ checkTree $ itemsToTree $ [ValueItem k k | k <- [1 :: Int .. 1000] ++ [1 :: Int .. 1000]]
