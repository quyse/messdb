{ nixpkgs }:
rec {
  packages = nixpkgs.haskellPackages.override {
    overrides = nixpkgs.haskell.lib.packageSourceOverrides {
      messdb = ./.;
    };
  };
}
