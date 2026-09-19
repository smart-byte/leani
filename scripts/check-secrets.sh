#!/bin/sh
set -eu

script_directory=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
scanner="$script_directory/../.private/tools/bin/gitleaks"
if [ ! -x "$scanner" ]; then
  scanner=$(command -v gitleaks || true)
fi
if [ -z "$scanner" ] || [ ! -x "$scanner" ]; then
  echo "Install the credential scanner with scripts/install-gitleaks.sh .private/tools/bin before pushing." >&2
  exit 1
fi
if [ "$("$scanner" version)" != "8.30.1" ]; then
  echo "Publication requires Gitleaks 8.30.1; run scripts/install-gitleaks.sh .private/tools/bin." >&2
  exit 1
fi

if [ "$#" -eq 0 ]; then set -- HEAD; fi
commits=""
for ref in "$@"; do
  commit=$(git rev-parse --verify --end-of-options "$ref^{commit}")
  commits="${commits:+$commits }$commit"
done
scan_log=$(mktemp)
trap 'rm -f "$scan_log"' EXIT HUP INT TERM
status=0
repository=$(git rev-parse --show-toplevel)
"$scanner" git "$repository" --log-opts="$commits" --redact --no-banner --no-color \
  --config "$script_directory/../.gitleaks.toml" --ignore-gitleaks-allow \
  --gitleaks-ignore-path /dev/null > "$scan_log" 2>&1 || status=$?
cat "$scan_log"
# This pinned scanner can report a Git error but return success. Treat any
# scanner error as a failed check, never as a clean credential scan.
if [ "$status" -ne 0 ] || grep -q ' ERR ' "$scan_log"; then
  exit 1
fi
