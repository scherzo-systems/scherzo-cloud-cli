{
  buildIdentity ? "unknown",
  cacert,
  craneLib,
  git,
  jq,
  lib,
  version,
}:

let
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../.config/nextest.toml
      ../Cargo.lock
      ../Cargo.toml
      ../docs/workflow-v1.md
      ../examples
      ../schemas
      ../src
      ../tests
    ];
  };

  commonArgs = {
    pname = "scherzo-cloud";
    inherit src;
    strictDeps = true;
  };

  # The dependency-only build is keyed on the crate manifests alone. The
  # rolling version and build identity change on every commit and must not
  # reach this derivation, or the cached dependency artifacts would be
  # invalidated by every source change.
  cargoArtifacts = craneLib.buildDepsOnly (
    commonArgs
    // {
      version = "0.0.0-deps";
    }
  );

  testArgs = commonArgs // {
    inherit cargoArtifacts version;
    nativeBuildInputs = [ git ];
    env = {
      SCHERZO_CLOUD_BUILD_IDENTITY = buildIdentity;
      SCHERZO_CLOUD_VERSION = version;
      SSL_CERT_FILE = "${cacert}/etc/ssl/certs/ca-bundle.crt";
    };
  };

  nextestTests = craneLib.cargoNextest (
    testArgs
    // {
      cargoExtraArgs = "--locked --workspace --all-targets --all-features";
      doInstallCargoArtifacts = false;
    }
  );
in
craneLib.buildPackage (
  commonArgs
  // {
    inherit cargoArtifacts version;
    doCheck = false;

    nativeBuildInputs = [ jq ];

    env = testArgs.env;

    # A direct production package build must first realize the nextest
    # derivation. The test gate cannot be bypassed through the package output.
    preBuild = ''
      test -d ${nextestTests}
    '';

    passthru = {
      inherit cargoArtifacts nextestTests;
    };

    postInstall = ''
      expected="scherzo-cloud ${version}"
      for invocation in "version" "--version"; do
        actual="$($out/bin/scherzo-cloud "$invocation")"
        if [ "$actual" != "$expected" ]; then
          echo "unexpected version output for $invocation: $actual" >&2
          echo "expected: $expected" >&2
          exit 1
        fi
      done

      json="$($out/bin/scherzo-cloud version --json)"
      if ! printf '%s\n' "$json" | jq --exit-status \
        --arg buildIdentity ${lib.escapeShellArg buildIdentity} \
        --arg executablePath "$out/bin/scherzo-cloud" \
        --arg version ${lib.escapeShellArg version} \
        '. == {
          "schemaVersion": 1,
          "command": "scherzo-cloud",
          "version": $version,
          "executablePath": $executablePath,
          "buildIdentity": $buildIdentity
        }' >/dev/null; then
        echo "unexpected JSON version output: $json" >&2
        exit 1
      fi
    '';

    meta = {
      description = "Command-line interface and runner for Scherzo Cloud";
      license = lib.licenses.asl20;
      mainProgram = "scherzo-cloud";
      platforms = lib.platforms.unix;
    };
  }
)
