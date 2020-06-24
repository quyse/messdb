{ nixpkgs }:
rec {
  packages = nixpkgs.haskellPackages.override {
    overrides = nixpkgs.haskell.lib.packageSourceOverrides {
      messdb = ./messdb;
      messdb-sqlite = ./messdb-sqlite;
    };
  };
}
