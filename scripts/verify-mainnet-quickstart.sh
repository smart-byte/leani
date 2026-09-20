#!/usr/bin/env bash
# Manual network check, deliberately separate from deterministic CI.
set -euo pipefail
repository_root="$(cd "$(dirname "$0")/.." && pwd)"
leani_binary="${LEANI_BIN:-${repository_root}/target/debug/leani}"
verification_dir="${LEANI_VERIFY_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/leani-mainnet-proof.XXXXXX")}"
mkdir -p "$verification_dir"
verification_dir="$(cd "$verification_dir" && pwd)"
export LEANI_VERIFY_API_PORT="${LEANI_VERIFY_API_PORT:-18180}"
export LEANI_VERIFY_RPC_PORT="${LEANI_VERIFY_RPC_PORT:-18545}"
export LEANI_VERIFY_WS_PORT="${LEANI_VERIFY_WS_PORT:-18546}"
config_path="$verification_dir/quickstart.toml"
printf '{"status":"running"}\n' > "$verification_dir/report.json"
python3 - "$repository_root" "$verification_dir" "$config_path" <<'PY'
from pathlib import Path
import json, os, sys
root, output, config = map(Path, sys.argv[1:])
source = (root / 'config/modes/windowed.toml').read_text()
source = source.replace('data_dir = "./data/windowed"', f'data_dir = {json.dumps(str(output / "data"))}')
for original, variable in [(8080, 'LEANI_VERIFY_API_PORT'), (8545, 'LEANI_VERIFY_RPC_PORT'), (8546, 'LEANI_VERIFY_WS_PORT')]:
    port = int(os.environ[variable])
    assert 0 < port < 65536, f'{variable} must be a valid port'
    source = source.replace(f'127.0.0.1:{original}', f'127.0.0.1:{port}')
config.write_text(source)
PY
"$leani_binary" --config "$config_path" doctor --json > "$verification_dir/doctor.json"
python3 - "$leani_binary" "$config_path" "$verification_dir" "${LEANI_VERIFY_TIMEOUT_SECONDS:-600}" <<'PYBACKFILL'
from pathlib import Path
import json, subprocess, sys, time
binary, config, output, timeout = sys.argv[1:]
root = Path(output)
timeout = int(timeout)
assert timeout > 0, 'LEANI_VERIFY_TIMEOUT_SECONDS must be positive'
began = time.monotonic()
with (root / 'backfill.json').open('w') as stdout, (root / 'backfill.log').open('w') as stderr:
    process = subprocess.Popen([binary, '--config', config, 'backfill', '--processor',
                                'uniswap-v2-sync-30d', '--from', '17000000', '--to', '17000999'],
                               stdout=stdout, stderr=stderr)
    timed_out = False
    try:
        code = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        process.terminate()
        try:
            code = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            code = process.wait()
attempt = dict(exitCode=code, timedOut=timed_out, elapsedSeconds=round(time.monotonic()-began, 3))
(root / 'backfill-attempt.json').write_text(json.dumps(attempt, indent=2) + '\n')
if timed_out or code:
    (root / 'report.json').write_text(json.dumps(dict(status='failed', backfill=attempt), indent=2) + '\n')
    raise SystemExit(f'Backfill failed; see {root / "backfill.log"}')
PYBACKFILL
"$leani_binary" --config "$config_path" serve > "$verification_dir/serve.log" 2>&1 &
node_pid=$!
cleanup() { kill -TERM "$node_pid" 2>/dev/null || true; wait "$node_pid" 2>/dev/null || true; }
trap cleanup EXIT
for _ in {1..60}; do
  kill -0 "$node_pid" 2>/dev/null || { cat "$verification_dir/serve.log" >&2; exit 1; }
  if curl -fsS "http://127.0.0.1:$LEANI_VERIFY_API_PORT/health/live" > /dev/null 2>&1; then break; fi
  sleep 0.2
done
api_base="http://127.0.0.1:$LEANI_VERIFY_API_PORT/v1/processors/uniswap-v2-sync-30d"
curl -fsS "$api_base/status" > "$verification_dir/coverage.json"
curl -fsS "$api_base/collections/uniswap_v2.sync_hourly/entities?limit=3" > "$verification_dir/entities.json"
snapshot_id="$(python3 - "$verification_dir" "$leani_binary" <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import hashlib, json, sys
root, binary = map(Path, sys.argv[1:])
coverage = json.loads((root / 'coverage.json').read_text())
entities = json.loads((root / 'entities.json').read_text())
assert any(r['fromBlock'] <= 17000000 and r['toBlock'] >= 17000999 and r['finality'] == 'finalized' for r in coverage['available']), coverage
assert entities['data'], 'the example must return decoded Sync events'
for entity in entities['data']:
    values = entity['data']['values']
    assert int(values['reserve0']) > 0 and int(values['reserve1']) > 0, entity
report = dict(status='data_checked', checkedAt=datetime.now(timezone.utc).isoformat(),
              binarySha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
              fromBlock=17000000, toBlock=17000999, sourceTrust='trusted_dataset',
              decodedRowsChecked=len(entities['data']), snapshotRows=entities['rowCount'])
report['backfill'] = json.loads((root / 'backfill-attempt.json').read_text())
(root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
print(entities['snapshotId'])
PY
)"
curl -fsS -X DELETE "$api_base/query-snapshots/$snapshot_id" > /dev/null
cleanup
trap - EXIT
"$leani_binary" --config "$config_path" db verify > "$verification_dir/database-verification.json"
python3 - "$verification_dir" <<'PYREPORT'
from pathlib import Path
import json, sys
root = Path(sys.argv[1])
assert json.loads((root / 'database-verification.json').read_text())['ok'] is True
report = json.loads((root / 'report.json').read_text())
report.update(status='passed', databaseVerified=True)
(root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
PYREPORT
cat "$verification_dir/report.json"
echo "Evidence: $verification_dir"
