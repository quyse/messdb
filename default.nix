{ nixpkgs }:
rec {
  packages = nixpkgs.haskellPackages.override {
    overrides = with nixpkgs.haskell.lib; let
      sourceOverrides = packageSourceOverrides {
        messdb-base = ./messdb-base;
        messdb-base-testlib = ./messdb-base-testlib;
        messdb-schema = ./messdb-schema;
        messdb-store-lmdb = ./messdb-store-lmdb;
        messdb-store-sqlite = ./messdb-store-sqlite;
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
