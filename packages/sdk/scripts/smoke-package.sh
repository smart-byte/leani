#!/bin/sh
set -eu

package_directory=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
work_directory=$(mktemp -d)
trap 'rm -rf "$work_directory"' EXIT HUP INT TERM
npm_cache="$work_directory/npm-cache"
mkdir "$npm_cache"

cd "$package_directory"
# `npm publish --dry-run` runs prepublishOnly with npm_config_dry_run=true.
# The nested package smoke must still create a real local tarball.
npm_config_cache="$npm_cache" npm_config_dry_run=false \
  npm pack --pack-destination "$work_directory" >/dev/null
tarball=$(find "$work_directory" -name '*.tgz' -print -quit)
if [ -z "$tarball" ]; then
  echo "npm pack did not produce a tarball" >&2
  exit 1
fi

mkdir "$work_directory/application"
cd "$work_directory/application"
npm_config_cache="$npm_cache" npm_config_dry_run=false npm init --yes >/dev/null
npm_config_cache="$npm_cache" npm_config_dry_run=false \
  npm install --ignore-scripts "$tarball" >/dev/null
node --input-type=module -e 'import("@leani/sdk").then((sdk) => { if (typeof sdk.createLeaniClient !== "function") process.exit(1); })'
node --input-type=module -e 'import("@leani/sdk/backfill").then((sdk) => { if (typeof sdk.createBackfillSubscriptionClient !== "function") process.exit(1); })'
bun --eval 'import { createLeaniClient } from "@leani/sdk"; if (typeof createLeaniClient !== "function") process.exit(1);'
bun --eval 'import { createBackfillSubscriptionClient } from "@leani/sdk/backfill"; if (typeof createBackfillSubscriptionClient !== "function") process.exit(1);'
