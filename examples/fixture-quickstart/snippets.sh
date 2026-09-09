# These are independently rendered documentation snippets, not one runnable
# script: `leani serve` intentionally remains in the foreground.

# docs:start source-install
git clone https://github.com/smart-byte/leani.git
cd leani
cargo build --locked -p leani
export LEANI_SOURCE="$PWD"
export PATH="$LEANI_SOURCE/target/debug:$PATH"
leani --version
# docs:end source-install

# docs:start offline-quickstart
LEANI_FIXTURE_DIR="$(mktemp -d)"
leani e2e fixture \
  --data-dir "$LEANI_FIXTURE_DIR" \
  --blocks 128 \
  --report "$LEANI_FIXTURE_DIR/report.json"
# docs:end offline-quickstart

# docs:start bounded-mainnet-quickstart
LEANI_HISTORY_DIR="$(mktemp -d)"
cp "$LEANI_SOURCE/config/modes/windowed.toml" "$LEANI_HISTORY_DIR/leani.toml"
cd "$LEANI_HISTORY_DIR"
leani doctor --json
leani backfill \
  --processor uniswap-v2-sync-30d --from 17000000 --to 17000999
leani serve
# docs:end bounded-mainnet-quickstart

# Run this in a second terminal after `serve` starts.
# docs:start bounded-mainnet-query
curl -s \
  'http://127.0.0.1:8080/v1/processors/uniswap-v2-sync-30d/collections/uniswap_v2.sync_hourly/entities?limit=3'
curl -s \
  'http://127.0.0.1:8080/v1/processors/uniswap-v2-sync-30d/status'
# docs:end bounded-mainnet-query

# docs:start live-block-first-run
leani subscribe blocks --once
# docs:end live-block-first-run

# docs:start live-block-node
mkdir leani-blocks && cd leani-blocks
leani init blocks
leani serve
# In another terminal in this directory:
# leani subscribe blocks --format json
# docs:end live-block-node
