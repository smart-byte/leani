#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 INSTALLATION_DIRECTORY" >&2
  exit 2
fi

version=0.9.2
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=aarch64-apple-darwin; checksum=ae72f0df0c399a1e96336f696fa55b1b28679fd725632eba8cf8e4568467cc3e ;;
  Linux-aarch64) platform=aarch64-unknown-linux-musl; checksum=af5169282fb6f84e13471493f405437e43ac517744c9ae12fbe2cdf0a6f0e5a8 ;;
  Linux-x86_64) platform=x86_64-unknown-linux-musl; checksum=9099a59e820c38a68b9d65f300662a567d56562f9a10f6aa4c7e86c17c2566af ;;
  *) echo "use cargo install cargo-about --version $version --locked on this platform" >&2; exit 1 ;;
esac

work_directory=$(mktemp -d)
trap 'rm -rf "$work_directory"' EXIT HUP INT TERM
package="cargo-about-$version-$platform"
curl --fail --silent --show-error --location \
  "https://github.com/EmbarkStudios/cargo-about/releases/download/$version/$package.tar.gz" \
  --output "$work_directory/$package.tar.gz"
if command -v sha256sum >/dev/null 2>&1; then
  printf '%s  %s\n' "$checksum" "$work_directory/$package.tar.gz" | sha256sum --check
else
  printf '%s  %s\n' "$checksum" "$work_directory/$package.tar.gz" | shasum -a 256 --check
fi
tar -xzf "$work_directory/$package.tar.gz" -C "$work_directory"
mkdir -p "$1"
install -m 755 "$work_directory/$package/cargo-about" "$1/cargo-about"
