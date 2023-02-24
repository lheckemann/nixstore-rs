{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-22.11";
  };

  outputs = { self, nixpkgs }: {
    devShell.x86_64-linux = with nixpkgs.legacyPackages.x86_64-linux;
      mkShell {
        buildInputs = [cargo rustfmt rustc cargo-edit];
      };
  };
}
