# These are independently rendered documentation snippets, not one runnable
# script: `leani serve` intentionally remains in the foreground.

# docs:start offline-quickstart
brew install leani-dev/tap/leani
mkdir -p /tmp/leani-quickstart
leani e2e fixture \
  --data-dir /tmp/leani-quickstart \
  --blocks 128 \
  --report /tmp/leani-quickstart/report.json
# docs:end offline-quickstart

# docs:start bounded-mainnet-quickstart
mkdir -p leani-quickstart && cd leani-quickstart
LEANI_RELEASE="${LEANI_RELEASE:-v0.1.0}"
curl -fsSLo leani.toml \
  "https://raw.githubusercontent.com/smart-byte/leani/${LEANI_RELEASE}/config/modes/windowed.toml"
leani doctor --json
leani backfill \
  --processor uniswap-v2-sync-30d --from 10000835 --to 10001834
leani serve
# docs:end bounded-mainnet-quickstart

# Run this in a second terminal after `serve` starts.
# docs:start bounded-mainnet-query
curl -s \
  'http://127.0.0.1:8080/v1/processors/uniswap-v2-sync-30d/collections/uniswap_v2.sync_hourly/entities?limit=3'
curl -s \
  'http://127.0.0.1:8080/v1/processors/uniswap-v2-sync-30d/status'
# docs:end bounded-mainnet-query
