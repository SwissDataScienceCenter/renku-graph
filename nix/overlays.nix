{
  # Fix the version of Jena explicitely (nixpkgs-22.05 comes with 4.3.1)
  jena = self: super: {
    apache-jena-fuseki = super.apache-jena-fuseki.overrideAttrs (old: rec {
      version = "4.6.1";
      src = super.fetchurl {
        url = "https://dlcdn.apache.org/jena/binaries/apache-jena-fuseki-${version}.tar.gz";
        sha256 = "sha256-LUaNpYcegMxd7WX3DCvgb2Hxsre4EHp7GkASzJfwaM0=";
      };
    });
  };

  # Make sure sbt is run with jdk17 (which is currently the default)
  sbt = self: super: {
    sbt = super.sbt.override { jre = super.openjdk17; };
  };

  # A simple startup script for postgresql running in foreground.
  postgres-fg = self: super: {
    postgres-fg = self.writeShellScriptBin "postgres-fg" ''
      data_dir="$1"
      port="''${2:-5432}"

      if [ -z "$data_dir" ]; then
          echo "A data directory is required!"
          exit 1
      fi

      if ! [ -f "$data_dir/PG_VERSION" ]; then
          echo "Initialize postgres cluster…"
          mkdir -p "$data_dir"
          chmod -R 700 "$data_dir"
          ${self.postgresql_14}/bin/pg_ctl init -D "$data_dir"
      fi

      ${self.postgresql_14}/bin/postgres -D "$data_dir" -k /tmp -h localhost -p $port
    '';
  };
}
