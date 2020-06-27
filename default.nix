{ nixpkgs }:
rec {
  packages = nixpkgs.haskellPackages.override {
    overrides = let
      sourceOverrides = nixpkgs.haskell.lib.packageSourceOverrides {
        messdb = ./messdb;
        messdb-schema = ./messdb-schema;
        messdb-sqlite = ./messdb-sqlite;
      };

      deps = self: super: with nixpkgs.haskell.lib; {
        messdb-sqlite = overrideCabal super.messdb-sqlite (attrs: {
          librarySystemDepends = [ nixpkgs.sqlite ];
        });
      };

    in nixpkgs.lib.composeExtensions sourceOverrides deps;
  };
}
