# MessDB - Nix-like approach for materialized views

This is a work-in-progress proof of concept of a database.
It borrows the immutable deterministic storage idea from the [Noms database](https://github.com/attic-labs/noms), and tries to mix it with ideas of [Nix/NixOS](https://nixos.org/) in order to deliver cacheable computations on data (incrementally updated materialized views).
