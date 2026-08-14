# Direct Arrow for the initial Xatu reader

Status: accepted

Date: 2026-08-01

## Context

The first historical workload joins four fixed Xatu tables into one
source-neutral block stream. Its query shape is compiled from a processor
descriptor, not supplied as arbitrary SQL. The daily beacon transaction file
contains over one million rows but only a few type-3 rows for a one-hour range,
so explicit projection, streaming, and future two-pass row selection matter
more than a general query planner.

## Decision

The initial Xatu reader uses Parquet's asynchronous Arrow stream directly
behind the source adapter. It enforces byte/frame/batch budgets, pushes a root
column projection, discards irrelevant rows immediately, and keeps no
temporary files. DataFusion remains an optional reader implementation behind
the same adapter and may replace this layer if later Uniswap or balance
workloads demonstrate materially better plans.

## Consequences

The adapter owns its four-table join and type conversion code. This is more
code than SQL, but it keeps dependencies, startup cost, memory, and transfer
accounting small. Predicate-aware two-pass row selection can be added without
changing normalized frames or processors.

## Evidence

The mainnet Dencun-hour probe for blocks `19426589..=19426888` produced 300
frames and 19 blob transactions in roughly 7–10 seconds. It projected
89,738,189 compressed column bytes from 215,613,184 object bytes, used no RPC
or temporary file, and kept peak Arrow batch memory below 1.7 MB. The daily
beacon transaction projection accounted for about 81.8 MB, identifying the
specific optimization target.

## Compatibility and rollback

Arrow and Parquet types remain private to `source-xatu`. Replacing the reader
with DataFusion changes neither source contracts, `BlockFrame`, processor
deltas, nor durable output. The catalog probe and parity corpus provide the
rollback comparison.
