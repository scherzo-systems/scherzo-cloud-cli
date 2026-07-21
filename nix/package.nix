{
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

  env.SCHERZO_CLOUD_VERSION = version;

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
  '';

  meta = {
    description = "Command-line interface and runner for Scherzo Cloud";
    license = lib.licenses.asl20;
    mainProgram = "scherzo-cloud";
    platforms = lib.platforms.unix;
  };
}
