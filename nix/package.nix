{
  buildIdentity ? "unknown",
  cacert,
  jq,
  lib,
  rustPlatform,
  version,
}:

rustPlatform.buildRustPackage {
  pname = "scherzo-cloud";
  inherit version;

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../Cargo.lock
      ../Cargo.toml
      ../src
      ../tests
    ];
  };

  cargoLock.lockFile = ../Cargo.lock;

  nativeBuildInputs = [ jq ];

  env = {
    SCHERZO_CLOUD_BUILD_IDENTITY = buildIdentity;
    SCHERZO_CLOUD_VERSION = version;
    SSL_CERT_FILE = "${cacert}/etc/ssl/certs/ca-bundle.crt";
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
