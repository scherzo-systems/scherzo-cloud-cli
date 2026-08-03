{ inputs, pkgs, ... }:

let
  manifest = builtins.fromTOML (builtins.readFile ./Cargo.toml);
  declaredRustVersion = manifest.package.rust-version;
  rustVersion =
    if builtins.match "[0-9]+\\.[0-9]+" declaredRustVersion != null then
      "${declaredRustVersion}.0"
    else
      declaredRustVersion;
  rustPackages = pkgs.extend inputs.rust-overlay.overlays.default;
  msrvToolchain = rustPackages.rust-bin.stable.${rustVersion}.minimal;
in
{
  # This environment deliberately duplicates the private repository's Rust and
  # structural-check tools because the exported CLI tree must validate without
  # importing or reading its parent repository.
  languages.rust = {
    enable = true;
    channel = "stable";
    version = rustVersion;
  };
  env.SCHERZO_MSRV_CARGO = "${msrvToolchain}/bin/cargo";
  env.SCHERZO_MSRV_RUSTC = "${msrvToolchain}/bin/rustc";

  packages = [
    pkgs.actionlint
    pkgs.ast-grep
    pkgs.git
    pkgs.jq
    pkgs.nodejs_24
  ];

  enterTest = ''
    ./scripts/check
  '';
}
