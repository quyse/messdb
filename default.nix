{ nixpkgs }:
rec {
  packages = nixpkgs.haskellPackages.override {
    overrides = with nixpkgs.haskell.lib; let
      sourceOverrides = packageSourceOverrides {
        messdb-base = ./messdb-base;
        messdb-lmdb = ./messdb-lmdb;
        messdb-schema = ./messdb-schema;
        messdb-sqlite = ./messdb-sqlite;
      };

      deps = self: super: {
        messdb-lmdb = overrideCabal super.messdb-lmdb (attrs: {
          librarySystemDepends = [ nixpkgs.lmdb ];
        });
        messdb-sqlite = overrideCabal super.messdb-sqlite (attrs: {
          librarySystemDepends = [ nixpkgs.sqlite ];
        });
      };

    in nixpkgs.lib.composeExtensions sourceOverrides deps;
  };
}
