{ description = "Development environment for renku-graph";

  # Install direnv and run `direnv allow` to automatically drop into
  # this environment when entering the directory in your shell.
  # Alternatively, run `nix develop` to drop into a bash shell.
  #
  # Look for packages here:
  # https://search.nixos.org/packages?channel=22.05

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-22.05";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, utils, ... }:
    utils.lib.eachDefaultSystem (system:
      let
        overlays = import ./nix/overlays.nix;
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            overlays.jena
            overlays.sbt
            overlays.postgres-fg
          ];
        };
      in
        { devShell = pkgs.mkShell {
            buildInputs = with pkgs;
              [ postgresql_12
                postgres-fg
                apache-jena-fuseki
                openjdk17
                sbt
              ];

            JAVA_HOME = "${pkgs.openjdk17}/lib/openjdk";
          };
        }
    );
}
