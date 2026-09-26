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
  awk -v filename="$1" '$2 == filename { print $1 }' "$checksums"
}

# Each checksum is assigned and checked before rendering. A failure inside a
# command substitution in sed's arguments would not stop the script.
require_sha256() {
  value=$2
  case "$value" in
    '' | *[!0-9a-f]*) ;;
    *) if [ "${#value}" -eq 64 ]; then return 0; fi ;;
  esac
  echo "missing or invalid SHA-256 checksum for $1" >&2
  exit 1
}

linux_x86="leani-${version}-x86_64-unknown-linux-gnu.tar.gz"
linux_arm="leani-${version}-aarch64-unknown-linux-gnu.tar.gz"
macos_x86="leani-${version}-x86_64-apple-darwin.tar.gz"
macos_arm="leani-${version}-aarch64-apple-darwin.tar.gz"

linux_x86_sha=$(checksum "$linux_x86")
require_sha256 "$linux_x86" "$linux_x86_sha"
linux_arm_sha=$(checksum "$linux_arm")
require_sha256 "$linux_arm" "$linux_arm_sha"
macos_x86_sha=$(checksum "$macos_x86")
require_sha256 "$macos_x86" "$macos_x86_sha"
macos_arm_sha=$(checksum "$macos_arm")
require_sha256 "$macos_arm" "$macos_arm_sha"

sed \
  -e "s|@TAG@|$version|g" \
  -e "s|@REPOSITORY@|$repository|g" \
  -e "s|@LINUX_X86_SHA@|$linux_x86_sha|g" \
  -e "s|@LINUX_ARM_SHA@|$linux_arm_sha|g" \
  -e "s|@MACOS_X86_SHA@|$macos_x86_sha|g" \
  -e "s|@MACOS_ARM_SHA@|$macos_arm_sha|g" \
  packaging/homebrew/leani.rb.template > "$output"
