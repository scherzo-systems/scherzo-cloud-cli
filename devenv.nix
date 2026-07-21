{ pkgs, ... }:

{
  languages.rust.enable = true;

  packages = [ pkgs.actionlint ];

  enterTest = ''
    ./scripts/check
  '';
}
