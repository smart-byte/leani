# Build a node configuration from requirements

`leani init` builds a complete `leani.toml` from what an application needs:
what to index, how much of it to keep, which blocks to follow, how much
history, and the host's limits. Every setting is written out, and each one the
builder derived carries a comment with the reason. Use it instead of editing a
shipped profile by hand.

## 1. Run it

With a preset and flags:

```bash
leani init uniswap-v3 ETH/USDC --keep window --window-days 30 \
  --memory 1GiB --bandwidth 100 --yes --json
leani init blocks --keep result --follow none --history-from-block 20000000 --json
```

With requirements as JSON, from a file or standard input, for any processor:

```bash
leani init --requirements - --json --yes <<'JSON'
{
  "processor": {
    "preset": "events",
    "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
    "events": ["Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"]
  },
  "keep": "window",
  "windowDays": 30,
  "follow": "finalized",
  "limits": { "memory": "512MiB", "disk": "20GiB", "bandwidthMbit": 100 }
}
JSON
```

A configuration that follows the chain needs a finality checkpoint, which
`init` fetches from the checkpoint providers; without a terminal, pass `--yes`
only after the user has approved them. `"follow": "none"` needs no network.
`init` refuses to overwrite an existing file; choose another path with
`--config`. Without any requirement flag, `--requirements`, or `--json`,
`init` writes the short compact configuration instead.

`--json` prints the report on stdout:

| Field | Meaning |
|---|---|
| `path`, `written` | The file, and whether it was written: it is not when `errors` is non-empty. |
| `toml` | The complete configuration. |
| `decisions` | `{setting, value, because}` for each setting derived from the requirements. |
| `tuning` | The tuning recipes applied. |
| `warnings` | Valid choices that may not do what they suggest, such as `latest` on a block-local processor. |
| `errors` | Validation errors, as `leani doctor` reports them. |
| `next` | Commands to run next, such as `leani doctor` and `leani serve`. |

## 2. Requirements

| Field | Values | Default |
|---|---|---|
| `processor` | see below | required |
| `keep` | `everything`, `window`, `latest`, `result`, `stream`, `live` | `everything` |
| `windowDays` | days `window` keeps | 30 |
| `follow` | `head`, `finalized`, `none` | `head` |
| `history` | `{"fromBlock": N}`, or `{"lastDays": N}` through a running node | none |
| `limits` | `memory` and `disk` (`"1GiB"`), `bandwidthMbit` (number) | none |
| `tuning` | recipes to apply instead of those the limits call for | derived |

`processor` is one of:

- `{"preset": "blocks"}`: every block's summary.
- `{"preset": "uniswap", "markets": ["ETH/USDC"]}`: pool observations.
- `{"preset": "events", "address": "0x…", "events": ["Transfer(address indexed from, address indexed to, uint256 value)"]}`:
  one contract's events, decoded from Solidity signatures, one entity per
  event.
- `{"preset": "custom", "id": "…", "version": "…", "instance": "…", "settings": {…}}`:
  any processor the node runs, such as `wasm` with `module` and `params`, or a
  processor compiled into a custom node. `leani guide processors` lists the
  built-in ones and their settings.

`keep` decides what stays in leani and what goes to the application:

| `keep` | Kept to query | To the application |
|---|---|---|
| `everything` | everything the processor writes; grows with the chain | changes for 24 h or 64 MiB |
| `window` | the last `windowDays` days | changes for 24 h or 64 MiB |
| `latest` | the current value of each key | changes for 24 h or 64 MiB |
| `result` | the running result of a range | one result, held until it confirms |
| `stream` | nothing | every change, held until its database confirms (1 GiB or 24 h, then pause) |
| `live` | nothing | the live head, best effort, for 1 h or 64 MiB |

`latest` stays small only for a processor that writes a fixed set of keys,
such as a running total; an event decoder writes a key per event, so the
report warns. `result` and `"follow": "none"` backfill explicit ranges:
`next` names the `leani backfill` command.

## 3. Limits and tuning

The builder applies only measured recipes from the configuration guide's
sizing table:

- `limitedMemory` (memory under 1 GiB): 4 reads, 2 chunks.
- `fasterErae` (memory from 1 GiB): faster eraE backfills at the same peak memory.
- `lessMemory` (memory from 1 GiB to under 4 GiB): 4 chunks instead of 8;
  Xatu backfills then hold about half as much.
- `transactionsFromXatu` (link under 200 Mbit/s, processor reads no logs):
  Xatu first; a processor that reads logs keeps eraE first, which alone
  proves logs complete.
- `slowDisk`: keeps commit batches large on network-attached volumes.

A disk limit gives three quarters to the store and caps temporary files at a
quarter. Pass `tuning` to replace the derived recipes.
