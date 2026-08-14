# Operating-mode profiles

Each file is a complete version-1 configuration and can be checked without
opening a database or network connection:

```bash
cargo run --locked --release -- \
  --config config/modes/windowed.toml doctor
```

The checked-in profiles deliberately disable live ingestion and external
finality so bounded Xatu backfills can be rehearsed without a trust-anchor
placeholder. Before `serve`, configure the verified finality and P2P live
sections from `config/example.toml`, set the processor start block, and use a
fresh `data_dir`.

| File | Durable shape | Intended command |
|---|---|---|
| `externalized.toml` | application-owned ranges; no local output; required acknowledged stream | create a backfill subscription through the API |
| `aggregate-only.toml` | no-consumer constant-size state plus latest aggregate | `backfill --processor transaction-stats` |
| `terminal.toml` | bounded aggregate with one terminal publication contract | bounded `backfill --processor transaction-stats` |
| `head-only.toml` | no materialized output; bounded best-effort replay | add verified live settings, then `serve` |
| `windowed.toml` | no-consumer finalized 30-day-equivalent SQLite window | `backfill --processor evm-events` |
| `full.toml` | no-consumer complete Uniswap observations in SQLite | `backfill --processor uniswap-observations` |

`terminal.toml` leaves a completed job reclaimable after result
acknowledgement. State or savepoint deletion is always an explicit operator
action.
