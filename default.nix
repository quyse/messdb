{ nixpkgs }:
rec {
  packages = nixpkgs.haskellPackages.override {
    overrides = nixpkgs.haskell.lib.packageSourceOverrides {
      messdb = ./messdb;
      messdb-schema = ./messdb-schema;
      messdb-sqlite = ./messdb-sqlite;
    };
  };
}
