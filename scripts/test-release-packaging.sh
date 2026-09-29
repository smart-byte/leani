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

render() {
  scripts/render-homebrew-formula.sh v0.1.0 smart-byte/leani "$1" "$2"
}

render "$checksums" "$formula"

if grep -q '@[A-Z_]*@' "$formula"; then
  echo "rendered formula contains an unresolved placeholder" >&2
  exit 1
fi
if [ "$(grep -c "sha256 \"$sha\"" "$formula")" -ne 4 ]; then
  echo "rendered formula must carry all four archive checksums" >&2
  exit 1
fi
ruby -c "$formula"

# A missing, duplicated, or malformed checksum must fail before a formula exists.
reject_checksums() {
  rejected="$work_directory/rejected.rb"
  if render "$work_directory/$2" "$rejected" 2>/dev/null; then
    echo "Homebrew renderer accepted $1" >&2
    exit 1
  fi
  if [ -e "$rejected" ]; then
    echo "Homebrew renderer wrote a formula despite $1" >&2
    exit 1
  fi
}

grep -v aarch64-apple-darwin "$checksums" > "$work_directory/missing"
reject_checksums "a missing checksum" missing
cat "$checksums" "$checksums" > "$work_directory/duplicate"
reject_checksums "a duplicated checksum" duplicate
sed '1s/^a//' "$checksums" > "$work_directory/short"
reject_checksums "a short checksum" short
sed '1s/^a/g/' "$checksums" > "$work_directory/non-hex"
reject_checksums "a non-hexadecimal checksum" non-hex
echo "Homebrew formula rendering rejects 4 invalid checksum files"

# The glibc floor check reads versioned symbols through objdump -T.
tools="$work_directory/bin"
mkdir "$tools"
cat > "$tools/objdump" <<'EOF'
#!/bin/sh
[ "$1" = -T ] || exit 2
cat "$2"
EOF
chmod +x "$tools/objdump"
symbols="$work_directory/leani.symbols"
cat > "$symbols" <<'EOF'
leani:     file format elf64-x86-64

DYNAMIC SYMBOL TABLE:
0000000000000000      DF *UND*	0000000000000000 (GLIBC_2.2.5) free
0000000000000000      DF *UND*	0000000000000000 (GLIBC_2.4)  __stack_chk_fail
0000000000000000      DF *UND*	0000000000000000 (GLIBC_2.34) __libc_start_main
0000000000000000      DF *UND*	0000000000000000  GLIBC_2.39  pidfd_getpid
0000000000000000      DO *UND*	0000000000000000 (GLIBC_PRIVATE) _dl_argv
EOF
grep -v 'GLIBC_[0-9]' "$symbols" > "$work_directory/unversioned.symbols"

check_glibc() {
  PATH="$tools:$PATH" ruby scripts/check-glibc-floor.rb "$@"
}

check_glibc "$symbols" 2.39
if check_glibc "$symbols" 2.38 2>/dev/null; then
  echo "glibc floor check accepted a binary above its floor" >&2
  exit 1
fi
if check_glibc "$work_directory/unversioned.symbols" 2.39 2>/dev/null; then
  echo "glibc floor check accepted a binary without GLIBC_ symbol versions" >&2
  exit 1
fi
echo "glibc floor check rejects 2 invalid binaries"

# The container's Rust image must match rust-toolchain.toml.
digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
printf '[toolchain]\nchannel = "1.97.0"\n' > "$work_directory/rust-toolchain.toml"
printf '[toolchain]\nchannel = "stable"\n' > "$work_directory/floating-toolchain.toml"
printf 'FROM rust:1.97.0-bookworm@%s AS builder\nFROM debian:bookworm-slim@%s\n' "$digest" "$digest" \
  > "$work_directory/matching.Dockerfile"
printf 'FROM rust:1.98.0-bookworm@%s AS builder\nFROM debian:bookworm-slim@%s\n' "$digest" "$digest" \
  > "$work_directory/newer.Dockerfile"
printf 'FROM rust:1.97.0-bookworm@%s AS builder\nFROM rust:1.98.0-slim@%s AS tools\n' "$digest" "$digest" \
  > "$work_directory/mixed.Dockerfile"
printf 'FROM debian:bookworm-slim@%s\n' "$digest" > "$work_directory/no-rust.Dockerfile"

ruby scripts/check-container-toolchain.rb \
  "$work_directory/matching.Dockerfile" "$work_directory/rust-toolchain.toml"
reject_toolchain() {
  if ruby scripts/check-container-toolchain.rb "$work_directory/$2" "$work_directory/$3" 2>/dev/null; then
    echo "container toolchain check accepted $1" >&2
    exit 1
  fi
}
reject_toolchain "a newer Rust image" newer.Dockerfile rust-toolchain.toml
reject_toolchain "a second, mismatched Rust stage" mixed.Dockerfile rust-toolchain.toml
reject_toolchain "a Dockerfile without a Rust stage" no-rust.Dockerfile rust-toolchain.toml
reject_toolchain "a floating toolchain channel" matching.Dockerfile floating-toolchain.toml
echo "container toolchain check rejects 4 mismatches"

# Documented SDK installs must pin the SDK package's exact version.
printf '{ "name": "@smart-byte/leani-sdk", "version": "1.2.3-rc.4" }\n' > "$work_directory/package.json"
printf '```bash\nbun add @smart-byte/leani-sdk@1.2.3-rc.4\n```\n' > "$work_directory/pinned.md"
printf 'Install it with `npm install @smart-byte/leani-sdk@1.2.3-rc.4`.\n' > "$work_directory/prose.md"
printf 'bun add @smart-byte/leani-sdk\n' > "$work_directory/unpinned.md"
printf 'bun add @smart-byte/leani-sdk@1.2.3-rc.3\n' > "$work_directory/stale.md"
printf 'bun add @smart-byte/leani-sdk@^1.2.3-rc.4\n' > "$work_directory/range.md"
printf 'npm i @smart-byte/leani-sdk@next\n' > "$work_directory/tag.md"
printf 'Import `@smart-byte/leani-sdk` after installing it.\n' > "$work_directory/no-install.md"

ruby scripts/check-sdk-pins.rb \
  "$work_directory/package.json" "$work_directory/pinned.md" "$work_directory/prose.md"
reject_pins() {
  if ruby scripts/check-sdk-pins.rb "$work_directory/package.json" "$work_directory/$2" 2>/dev/null; then
    echo "SDK pin check accepted $1" >&2
    exit 1
  fi
}
reject_pins "an install without a version" unpinned.md
reject_pins "another version" stale.md
reject_pins "a version range" range.md
reject_pins "a distribution tag" tag.md
reject_pins "documentation without an install" no-install.md
echo "SDK pin check rejects 5 unpinned installs"
