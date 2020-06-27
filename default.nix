{ nixpkgs }:
rec {
  packages = nixpkgs.haskellPackages.override {
    overrides = with nixpkgs.haskell.lib; let
      sourceOverrides = packageSourceOverrides {
        messdb = ./messdb;
        messdb-schema = ./messdb-schema;
        messdb-sqlite = ./messdb-sqlite;
      };

      deps = self: super: {
        messdb-sqlite = overrideCabal super.messdb-sqlite (attrs: {
          librarySystemDepends = [ nixpkgs.sqlite ];
        });
      };

    in nixpkgs.lib.composeExtensions sourceOverrides deps;
  };
}
