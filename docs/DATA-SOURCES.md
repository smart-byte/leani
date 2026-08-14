# Leani data sources and upstream context

Schema rechecked against the upstream Xatu schema on 2026-08-04. Public
availability remains operational evidence and must still be probed before a
large backfill.

## 1. Recommended source portfolio

| Source | Role | Strength | Main limitation |
|---|---|---|---|
| Xatu public Parquet | Default specialized backfill | Free, canonical, columnar, rich tables, filter/project before retaining | Donated hosting; filtered rows are trusted data |
| EraE archives | General raw historical source | Sparse native reader; complete block bodies and receipts with locally checked execution commitments | Publisher catalog remains the canonicality trust root |
| Ethereum P2P/Reth | Recent and live | No vendor key, follows head, bodies/receipts | Old history is thinning; consensus finality remains separate |
| Beacon light client/CL | Canonicality/finality | Anchors post-Merge execution payloads | Adds a consensus component |
| AWS public blockchain | Secondary Parquet source | Free public S3, daily updates | Validate schema completeness for each fork/use case |
| Google Blockchain Analytics | Development/fallback SQL | Broad tables and easy SQL | Hosted dependency; first 1 TB/month free, then paid |
| Portal Network | Future decentralized history/state | Designed for low-resource history access | Completeness/maturity must be tested |
| JSON-RPC | Development/rare fallback | Broad compatibility | Reintroduces cost/trust; never default core |

## 2. Xatu

[Xatu Data](https://github.com/ethpandaops/xatu-data) is currently the strongest first backend.

Properties:

- public Parquet is available without access to EthPandaOps' private ClickHouse;
- canonical execution tables are partitioned in 1,000-block chunks;
- canonical beacon tables are generally partitioned daily;
- the repository is CC BY 4.0;
- files can be queried directly or read through Arrow/ClickHouse/DataFusion.

Relevant canonical execution tables include:

- blocks and transactions;
- receipts-derived logs;
- traces;
- contracts and address appearances;
- balance diffs and reads;
- ERC-20 and ERC-721 transfers;
- native transfers;
- nonce and storage diffs/reads.

Relevant canonical beacon tables include:

- execution block hash/number/base fee;
- blob gas used and excess blob gas;
- execution transactions with type, sender, blob gas fee cap, and blob hashes;
- blob sidecars;
- withdrawals.

The implemented `XatuHistorySource` plans four fail-closed physical
projections:

1. headers from minimal execution/beacon block columns;
2. transactions with sender, recipient, exact value, calldata, nonce, gas
   limit, and type;
3. logs with address and all four topic positions; and
4. the existing blob/type-3 projection.

Every immutable chunk encodes its projection kind and exact filters. `open`
therefore cannot execute a different physical projection from the one that
was planned. The source plan reports logical reader, table, selected columns,
predicates, trust, derived status, and completeness. Unsupported combinations
fail before object metadata or Parquet data is downloaded.

Transaction aggregates select `canonical_execution_transaction` without
receipts. Declarative events select `canonical_execution_logs` without beacon
transactions, withdrawals, or blob fields. Both read the minimal execution
and beacon block columns needed for timestamps, canonical hash, and parent
identity.

Source-native transfer/trace/state-diff readers remain optional breadth work;
their stronger derived-table semantics must be explicit before they can
satisfy a processor capability.

For blobs.money, combine:

```text
canonical_beacon_block
canonical_beacon_block_execution_transaction
canonical_beacon_block_withdrawal
canonical_execution_block
canonical_execution_transaction
```

and use canonical execution logs only if rollup attribution later needs emitted events.

Trust note: Xatu's canonical tables are derived by an execution pipeline. Checksums verify downloaded files, not Ethereum execution. Full receipts can be checked against a receipt root; filtered transfer/log/state-diff rows cannot prove their completeness in isolation.

Sources:

- [Xatu repository and Parquet examples](https://github.com/ethpandaops/xatu-data)
- [Canonical beacon schema](https://github.com/ethpandaops/xatu-data/blob/master/schema/canonical_beacon_.md)
- [Canonical execution schema](https://github.com/ethpandaops/xatu-data/blob/master/schema/canonical_execution_.md)

## 3. EraE

[EthPandaOps history endpoints](https://ethpandaops.io/data/history/) describe EraE as complete execution-layer history from genesis, covering pre- and post-Merge blocks and intended for client bootstrapping/verification.

Implemented usage:

1. fetch and validate the checksum catalog;
2. read the e2store version and dynamic block index with bounded range requests;
3. fetch only the selected compressed headers, bodies, and receipts;
4. decode and verify transaction, receipt, ommer, withdrawal, and logs-bloom commitments;
5. map configured processors;
6. discard archive bytes;
7. checkpoint and continue.

This gives bounded memory/network use, needs no temporary archive file, and
avoids relying on old execution P2P peers. The reader supports HTTP(S) mirrors
and local `file:` catalogs and processes large requests in bounded frame
batches.

The direct mainnet catalog was populated on 2026-08-01 even though the rendered
history webpage's generated counter was stale. A live sparse probe decoded and
verified post-Dencun block `19,426,589` from a 967 MB source object, retaining
111,181 bytes of normalized frame data. Catalog continuity, mirror behavior,
and sustained throughput still require release evidence.

A later conformance run verified all 300 blocks in
`19,426,589..=19,426,888` (123,856,697 normalized in-memory bytes) without
retaining the 967 MB EraE object. Exporting canonical byte arrays as pretty
JSON was intentionally not retained: it expanded past 2 GB. Operational
backfills map and commit bounded frames directly; raw JSON is a diagnostic
format, not a storage format.

The catalog advertises each full object's SHA-256. Sparse mode records that
identity but cannot honestly claim to have recomputed a full-file checksum
without downloading the full object. Its local commitment checks establish
internal Ethereum execution consistency; the execution hash's canonicality
still follows the publisher catalog until it is anchored through independently
verified consensus material.

### EraE versus Xatu

Xatu should usually win for a known aggregation:

- it is columnar and already split into logs, transfers, transactions, traces, and state-diff tables;
- the reader can project and filter before most bytes enter the processor;
- it can provide executed artifacts that do not exist in raw receipts;
- it should make blobs, token-transfer, and Uniswap backfills substantially faster.

EraE should win when reproducibility and raw protocol coverage matter:

- it provides block bodies and receipts rather than a provider-defined analytical projection;
- transaction and receipt roots can be recomputed from complete block inputs;
- a new event processor can be run without waiting for a public dataset to expose a new table or field;
- files can theoretically be mirrored by multiple independent hosts;
- it is a useful audit source for sampled or full Xatu ranges.

EraE still does not contain EVM call traces or state diffs, and it normally requires downloading far more data than a filtered Xatu query. “Independent history path” therefore means an interchangeable raw fallback and verification path, not that EraE must be the default or that its current host is trustless.

## 4. Ethereum P2P, Reth, and SHiNode

[SHiNode](https://github.com/vicnaum/shinode) demonstrates that Reth/devp2p can retrieve headers, transaction bodies, receipts, and logs without EVM state and follow the head.

SHiNode context from the research snapshot:

- Rust, MIT/Apache-2.0;
- current documented release v0.3.0;
- claims over 1,000 blocks/second and roughly six hours for full Mainnet on a modern SSD;
- stores roughly 250 GB in events-only mode or under 800 GB with transactions;
- implements `eth_chainId`, `eth_blockNumber`, limited `eth_getBlockByNumber`, and `eth_getLogs`;
- its own transaction receipt RPC and WebSocket subscriptions were planned,
  not implemented at the snapshot date;
- calldata is not currently retained;
- receipt verification and consensus-layer integration are planned;
- its README explicitly warns against production use;
- it already recognizes that EIP-4444 makes pure P2P history unreliable and lists archive/Portal support as future work.

Leani should assess SHiNode as a source implementation, not inherit its storage policy. Our P2P path should map and discard old raw material rather than sealing permanent event shards.

SHiNode is active enough to be valuable upstream work, but its small maintainer base and production warning mean the fork must be independently tested and audited.

The preferred implementation boundary has changed since SHiNode's original
design. [Reth 2.2](https://github.com/paradigmxyz/reth/releases) exposes a
`ReceiptsClient` trait and P2P receipt downloading. Reth also documents
individually usable crates, including networking APIs, eth-wire messages, and
downloaders in its
[repository layout](https://github.com/paradigmxyz/reth/blob/main/docs/repo/layout.md).

Therefore:

1. build `source-p2p` directly on the smallest upstream Reth crate set;
2. keep Reth types and version churn inside that adapter;
3. use SHiNode as a reference for scheduling, peer behavior, and failure tests;
4. contribute a missing general hook upstream where practical;
5. use a narrow isolated fork only if the boundary spike proves it necessary.

This avoids adopting SHiNode's permanent event-shard store, incomplete RPC
surface, and release cadence while preserving the important stateless-history
idea.

The independent node now implements exact receipt/block RPC reconstruction,
post-commit WebSocket subscriptions, and a bounded recent raw-material window;
the bullets above describe SHiNode, not this repository.

## 5. P2P history expiry

[EIP-4444](https://eips.ethereum.org/EIPS/eip-4444) specifies that clients should not serve block bodies and receipts older than one year over execution P2P and may prune them. The [Ethereum Foundation's partial history expiry announcement](https://blog.ethereum.org/2025/07/08/partial-history-exp) states that execution clients support removing pre-Merge history.

Consequences:

- P2P is the best live/recent input, not the only historical source.
- A full-genesis P2P sync may work opportunistically but is not a dependable product contract.
- Archive files, public datasets, Portal, and independent mirrors must be interchangeable.

Leani therefore places its finalized execution-P2P history adapter after
retained history, Xatu, EraE, and configured archives. By default it may plan
every gap at or after the processor's configured start block. An explicitly
configured `history_fallback_blocks` value limits that eligibility to a suffix
ending at the finalized anchor. It verifies the complete header ancestry to
that anchor, checks body and receipt commitments, streams only
processor-requested normalized fields, and resumes material acquisition from
durable processor coverage.
Live requests share the manager but have dispatch priority over queued history
requests.

This is a resilient fallback, not a cheaper archive transport. A recent
receipt-complete block can carry hundreds of kilobytes before devp2p Snappy,
and public peers may be full, slow, or missing old history. Keep the persistent
node identity and scored peer cache across restarts; use archive sources for
predictable large backfills and P2P when those sources lag or fail.

## 6. Portal Network

The [Portal Network documentation](https://ethereum.org/developers/docs/networking-layer/portal-network/) targets lightweight access to:

- beacon light-client data;
- headers;
- block bodies;
- receipts;
- eventually state and transaction lookup.

It is a natural future backend because its goal aligns with this project. It should be introduced after measuring:

- content availability for random historical blocks;
- retrieval throughput for sequential backfills;
- proof/canonicality semantics;
- operational reliability;
- Rust client integration.

Portal is not the MVP's only source.

## 7. AWS public blockchain data

[AWS Public Blockchain Data](https://registry.opendata.aws/aws-public-blockchain/) provides free, date-partitioned Parquet in public S3 and states that new data is delivered daily.

Use it as:

- an independent comparison source;
- a fallback for conventional block/transaction/log/trace data;
- a potential secondary mirror.

Before using it for a processor, inspect a current partition rather than assuming its schema has every recent fork field. Earlier inspection for this project found blob-specific coverage less complete than Xatu.

## 8. Google Blockchain Analytics and Ethereum ETL

[Google Blockchain Analytics](https://docs.cloud.google.com/blockchain-analytics/docs) exposes Ethereum blocks, transactions, receipts, logs, decoded events, token transfers, traces, and account-state-oriented tables in BigQuery. [Pricing](https://cloud.google.com/blockchain-analytics/pricing) follows BigQuery and includes the first 1 TB of processed query data per month.

It is useful for:

- SQL validation;
- one-off research;
- filling a missing backend capability;
- cross-source correctness tests.

It is not the fully free/self-sovereign production core.

[Ethereum ETL](https://github.com/blockchain-etl/ethereum-etl) is extraction/transformation tooling, not itself a free source of Ethereum history. Its schema and decoders are useful references, but running it traditionally still requires a node or RPC. The project now documents blob header and transaction fields, so its schemas can help normalize alternative datasets.

## 9. Firehose, Substreams, and subgraphs

[Firehose data flow](https://firehose.streamingfast.io/architecture/data-flow) accepts protobuf blocks from a reader process and serves a cursor-based historical/live stream. [Firehose storage](https://firehose.streamingfast.io/firehose/architecture/data-storage) normally retains one-block and merged 100-block files. Substreams adds parallel Wasm transformation and module-output caches.

Relevance:

- useful protobuf/cursor conventions;
- potential ecosystem compatibility;
- a SHiNode/EraE adapter can theoretically emit `FIRE BLOCK`;
- event-only processors need only headers/transactions/receipts/logs.

Conflict:

- normal Firehose durability keeps raw chain data;
- Ethereum Firehose blocks often include call traces and state changes unavailable from receipt-only sources;
- running all Firehose/Substreams services is operationally heavier than the desired default.

The project should implement a compatibility experiment after the core normalized stream works, not start by deploying a full Firehose stack.

Traditional subgraphs are another possible processor/output compatibility layer. Event handlers and topic filters fit the receipt-only core. Subgraphs that use contract calls do not: [The Graph's documentation](https://thegraph.com/docs/en/subgraphs/developing/creating/advanced/) shows that mappings can declare `eth_call`, which requires EVM state at the indexed block. A future adapter could emit entity/database changes or feed an event-only Graph mapping, but “subgraph compatible” must carry the same capability restrictions as Substreams.

## 10. Source-selection policy

For each requested range:

1. filter sources by required capabilities;
2. reject sources whose lag/finality violates the processor policy;
3. prefer projection/predicate pushdown for specialized backfills;
4. prefer root-verifiable full data when trust policy requires it;
5. minimize expected downloaded bytes, then latency;
6. schedule an overlapping source at transitions;
7. record source and verification per committed range;
8. fail over at chunk boundaries when possible.

Live execution data and finality are planned separately. A Reth P2P peer can
supply a valid body and receipts for a branch; it does not by itself prove the
post-Merge canonical/finalized branch. The initial finality adapter consumes
verified beacon light-client updates through an untrusted Beacon API endpoint.
The self-contained profile instead discovers consensus peers directly and
uses the standard light-client bootstrap/update/finality req/resp protocols.
Both transports share the same local proof verifier and explicit
weak-subjectivity checkpoint boundary.

An operator must be able to choose:

- `fast/trusted`: Xatu filtered tables;
- `verify/full`: EraE or full P2P receipts;
- `independent`: multiple sources and sampled/full cross-checking.
