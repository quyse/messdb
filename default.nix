{ nixpkgs }:
rec {
  packages = nixpkgs.haskellPackages.override {
    overrides = with nixpkgs.haskell.lib; let
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
          librarySystemDepends = [ nixpkgs.lmdb ];
        });
        messdb-store-sqlite = overrideCabal super.messdb-store-sqlite (attrs: {
          librarySystemDepends = [ nixpkgs.sqlite ];
        });
      };

    in nixpkgs.lib.composeExtensions sourceOverrides deps;
  };
}
