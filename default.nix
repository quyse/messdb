{ pkgs ? import <nixpkgs>
}:
rec {
  packages = pkgs.haskellPackages.override {
    overrides = with pkgs.haskell.lib; let
      sourceOverrides = packageSourceOverrides {
        messdb-base = ./messdb-base;
        messdb-base-testlib = ./messdb-base-testlib;
        messdb-repo = ./messdb-repo;
        messdb-schema = ./messdb-schema;
        messdb-sql = ./messdb-sql;
        messdb-store-lmdb = ./messdb-store-lmdb;
        messdb-store-sqlite = ./messdb-store-sqlite;
        messdb-store-zlib = ./messdb-store-zlib;
        messdb-tool = ./messdb-tool;
      };

      deps = self: super: {
        messdb-store-lmdb = overrideCabal super.messdb-store-lmdb (attrs: {
          librarySystemDepends = [ pkgs.lmdb ];
        });
        messdb-store-sqlite = overrideCabal super.messdb-store-sqlite (attrs: {
          librarySystemDepends = [ pkgs.sqlite ];
        });
        simple-sql-parser = doJailbreak super.simple-sql-parser;
      };

    in pkgs.lib.composeExtensions sourceOverrides deps;
  };

  touch = {
    inherit (packages)
      messdb-base
      messdb-schema
      messdb-store-lmdb
      messdb-store-sqlite
      messdb-tool
    ;
  };
}
