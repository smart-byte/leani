#!/bin/sh
set -eu

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <v-version> <owner/repository> <SHA256SUMS> <output.rb>" >&2
  exit 2
fi

version=$1
repository=$2
checksums=$3
output=$4

case "$version" in
  v[0-9]*.[0-9]*.[0-9]*) ;;
  *) echo "version must be a v-prefixed SemVer" >&2; exit 2 ;;
esac

case "$repository" in
  */*) ;;
  *) echo "repository must be owner/name" >&2; exit 2 ;;
esac

checksum() {
  filename=$1
  value=$(awk -v filename="$filename" '$2 == filename { print $1 }' "$checksums")
  if [ -z "$value" ]; then
    echo "missing checksum for $filename" >&2
    exit 1
  fi
  printf '%s' "$value"
}

linux_x86="leani-${version}-x86_64-unknown-linux-gnu.tar.gz"
linux_arm="leani-${version}-aarch64-unknown-linux-gnu.tar.gz"
macos_x86="leani-${version}-x86_64-apple-darwin.tar.gz"
macos_arm="leani-${version}-aarch64-apple-darwin.tar.gz"

sed \
  -e "s|@TAG@|$version|g" \
  -e "s|@REPOSITORY@|$repository|g" \
  -e "s|@LINUX_X86_SHA@|$(checksum "$linux_x86")|g" \
  -e "s|@LINUX_ARM_SHA@|$(checksum "$linux_arm")|g" \
  -e "s|@MACOS_X86_SHA@|$(checksum "$macos_x86")|g" \
  -e "s|@MACOS_ARM_SHA@|$(checksum "$macos_arm")|g" \
  packaging/homebrew/leani.rb.template > "$output"
