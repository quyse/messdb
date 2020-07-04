{ nixpkgs ? import <nixpkgs> {} }:
rec {
  inherit (import ./default.nix { inherit nixpkgs; }) packages;
  touch = {
    inherit (packages)
      messdb-base
      messdb-schema
      messdb-store-lmdb
      messdb-store-sqlite
    ;
  };
}
