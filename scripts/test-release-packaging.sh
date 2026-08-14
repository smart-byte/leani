#!/bin/sh
set -eu

work_directory=$(mktemp -d)
trap 'rm -rf "$work_directory"' EXIT HUP INT TERM
checksums="$work_directory/SHA256SUMS"
formula="$work_directory/leani.rb"
sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

for target in \
  x86_64-unknown-linux-gnu \
  aarch64-unknown-linux-gnu \
  x86_64-apple-darwin \
  aarch64-apple-darwin
do
  printf '%s  leani-v0.1.0-%s.tar.gz\n' "$sha" "$target" >> "$checksums"
done

scripts/render-homebrew-formula.sh \
  v0.1.0 \
  smart-byte/leani \
  "$checksums" \
  "$formula"

if grep -q '@[A-Z_]*@' "$formula"; then
  echo "rendered formula contains an unresolved placeholder" >&2
  exit 1
fi
ruby -c "$formula"
