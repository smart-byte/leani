#!/bin/sh
set -eu

package_directory=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
work_directory=$(mktemp -d)
trap 'rm -rf "$work_directory"' EXIT HUP INT TERM
npm_cache="$work_directory/npm-cache"
mkdir "$npm_cache"

cd "$package_directory"
SDK_PACKAGE=$(node -p 'require("./package.json").name')
export SDK_PACKAGE
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
# The packed SDK_VERSION must be the packed manifest's version.
node --input-type=module -e '
  import { createRequire } from "node:module";
  const require = createRequire(`${process.cwd()}/`);
  const { version } = require(`${process.env.SDK_PACKAGE}/package.json`);
  const sdk = await import(process.env.SDK_PACKAGE);
  if (typeof sdk.createLeaniClient !== "function") process.exit(1);
  if (sdk.SDK_VERSION !== version) {
    console.error(`packed SDK_VERSION ${sdk.SDK_VERSION} is not package version ${version}`);
    process.exit(1);
  }
'
node --input-type=module -e 'import(`${process.env.SDK_PACKAGE}/backfill`).then((sdk) => { if (typeof sdk.createBackfillSubscriptionClient !== "function") process.exit(1); })'
# Resolvers without an "import" condition fall back to "default".
node -e 'require.resolve(process.env.SDK_PACKAGE); require.resolve(`${process.env.SDK_PACKAGE}/backfill`);'
bun --eval 'const { createLeaniClient } = await import(process.env.SDK_PACKAGE); if (typeof createLeaniClient !== "function") process.exit(1);'
bun --eval 'const { createBackfillSubscriptionClient } = await import(`${process.env.SDK_PACKAGE}/backfill`); if (typeof createBackfillSubscriptionClient !== "function") process.exit(1);'
