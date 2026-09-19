#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <installation-directory>" >&2
  exit 2
fi

version=8.30.1
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=darwin_arm64; checksum=b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5 ;;
  Darwin-x86_64) platform=darwin_x64; checksum=dfe101a4db2255fc85120ac7f3d25e4342c3c20cf749f2c20a18081af1952709 ;;
  Linux-aarch64) platform=linux_arm64; checksum=e4a487ee7ccd7d3a7f7ec08657610aa3606637dab924210b3aee62570fb4b080 ;;
  Linux-x86_64) platform=linux_x64; checksum=551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb ;;
  *) echo "unsupported secret-scanner platform" >&2; exit 1 ;;
esac

work_directory=$(mktemp -d)
trap 'rm -rf "$work_directory"' EXIT HUP INT TERM
archive="gitleaks_${version}_${platform}.tar.gz"
curl --fail --silent --show-error --location \
  "https://github.com/gitleaks/gitleaks/releases/download/v${version}/${archive}" \
  --output "$work_directory/$archive"
printf '%s  %s\n' "$checksum" "$work_directory/$archive" | shasum -a 256 --check
tar -xzf "$work_directory/$archive" -C "$work_directory" gitleaks
mkdir -p "$1"
install -m 755 "$work_directory/gitleaks" "$1/gitleaks"
