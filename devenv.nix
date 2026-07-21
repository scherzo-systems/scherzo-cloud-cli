{ ... }:

{
  languages.rust.enable = true;

  enterTest = ''
    ./scripts/check
  '';
}
