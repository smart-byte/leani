#!/bin/sh
# Build the example Wasm processors and refresh the prebuilt copies in
# crates/processor-wasm/modules, which tests run against and the node embeds
# as templates; `--check` fails when a committed module is stale. The build
# runs in the Dockerfile's Rust image for linux/amd64 with the checkout at
# /leani: rustc's output differs between host architectures, and Cargo hashes
# the guest SDK's absolute path into every module.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
modules="$root/crates/processor-wasm/modules"
image=$(sed -n 's/^FROM \(rust:[^ ]*\).*/\1/p' "$root/Dockerfile" | head -n 1)
built=$(mktemp -d)
trap 'rm -rf "$built"' EXIT

# Outside the checkout, rustup uses the image's toolchain, which
# scripts/check-container-toolchain.rb keeps equal to rust-toolchain.toml.
docker run --rm --platform linux/amd64 -v "$root:/leani:ro" -w / "$image" sh -c '
  rustup target add wasm32-unknown-unknown >&2 &&
  RUSTFLAGS="-C link-arg=-zstack-size=262144" cargo build \
    --manifest-path /leani/examples/wasm-processors/Cargo.toml --release --locked \
    --target wasm32-unknown-unknown --target-dir /tmp/target >&2 &&
  cd /tmp/target/wasm32-unknown-unknown/release && tar -cf - *.wasm' >"$built/modules.tar"
tar -xf "$built/modules.tar" -C "$built"

status=0
for name in abi_probe block_gas erc20_transfers event_decoder token_volume; do
  if [ "${1:-}" = "--check" ]; then
    if ! cmp -s "$built/$name.wasm" "$modules/$name.wasm"; then
      echo "stale: crates/processor-wasm/modules/$name.wasm" >&2
      status=1
    fi
  else
    cp "$built/$name.wasm" "$modules/$name.wasm"
  fi
done
exit "$status"
