#!/usr/bin/env bash
set -euo pipefail

# docs:start offline-quickstart
brew install leani-dev/tap/leani
mkdir -p /tmp/leani-quickstart
leani e2e fixture \
  --data-dir /tmp/leani-quickstart \
  --blocks 128 \
  --report /tmp/leani-quickstart/report.json
# docs:end offline-quickstart

# docs:start bounded-mainnet-quickstart
LEANI_RELEASE="${LEANI_RELEASE:-v0.1.0}"
curl -fsSLo windowed.toml \
  "https://raw.githubusercontent.com/smart-byte/leani/${LEANI_RELEASE}/config/modes/windowed.toml"
leani --config windowed.toml doctor --json
leani --config windowed.toml backfill \
  --processor evm-events --from 10000835 --to 10001834
leani --config windowed.toml serve
# docs:end bounded-mainnet-quickstart
