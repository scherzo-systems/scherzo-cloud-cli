{ pkgs, ... }:

{
  languages.rust.enable = true;

  packages = [
    pkgs.actionlint
    pkgs.ast-grep
    pkgs.jq
  ];

  enterTest = ''
    ./scripts/check
  '';
}
