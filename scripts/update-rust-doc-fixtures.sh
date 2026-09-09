#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repository_root"
LEANI_UPDATE_DOC_FIXTURES=1 cargo test --locked -p leani --test docs_fixtures --test showcase_contracts
