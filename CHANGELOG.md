# Leani changelog

All notable changes are documented here. The format follows Keep a Changelog,
and releases use semantic versioning for native API, SDK, processor, durable
record, and documented RPC contracts.

## [Unreleased]

### Security

- Updated Reth's transitive `imbl` dependency to 7.0.2 and
  `imbl-sized-chunks` to 0.2.0 to address RUSTSEC-2026-0292. This also removes
  the unmaintained `bitmaps` dependency and its policy exceptions.
- The weak-subjectivity checkpoint quorum used by `leani init` and
  `leani subscribe` now needs a strict majority of the providers tried, not
  just `--checkpoint-quorum` of them. It fails closed when a responding
  provider reports another root at the agreed slot, or when two answers both
  reach the threshold. The confirmation prompt lists every provider's answer
  or failure. Providers must use HTTPS, with plain HTTP accepted only on
  loopback. Leani does not follow their redirects, reads at most 1 MiB from
  each, and rejects a provider listed twice with the same scheme, host, port,
  and path.
- Beacon API finality endpoints are no longer followed through redirects, and
  their responses, error bodies included, are capped at 16 MiB while they
  stream in. Errors, logs, `source probe finality` reports, and the checkpoint
  prompt show endpoint and provider URLs as `scheme://host[:port]`, with `/…`
  in place of a path, so API keys in userinfo, query strings, or paths are not
  printed. Request errors add the API path, and endpoints that would show
  alike carry their index, such as `finality.endpoints[1]`.
- A persisted finality anchor replaces the configured `finality.checkpoint`
  only when it was verified from that checkpoint. An anchor from another
  trust root is ignored with a warning, and `leani doctor` reports it, so
  changing the checkpoint re-anchors the node.
- Execution P2P no longer bans honest peers, or stores failures against them,
  for contradicting a head that peers only claimed. The live lane used the
  head most peers declared in their handshake status as the expected tip of
  its next header range, and banned every peer that served another tip: any
  one peer could claim a head, and an honestly orphaned head had the same
  effect. Failed responses are now classified. Material that breaks its
  commitments, structure, or block numbers, or that contradicts a verified
  expectation (the consensus-verified finalized anchor, or the header chain
  proven to it), still bans the peer and stores the failure. A mismatch with
  an unverified expectation, and an empty or short reply, costs at most a
  short local cooldown. The same rule applies to the peer preview of
  `leani subscribe`, to reorg reconstruction, which no longer reports a peer
  to Reth for returning no headers, and to
  `leani source probe p2p --expected-tip`.
- An execution peer that serves the live lane the next block's header and
  withholds its body or receipts no longer stalls the lane. A live body or
  receipt request, including the receipts of filtered-log subscriptions, now
  gives up after eight waves across the peer pool or a minute, even inside a
  wave. The lane then reports itself disconnected, which clears readiness,
  and asks another peer for the header. The peer that served the header
  takes a strike: it is not asked for headers for a cooldown that grows with
  each strike, up to 30 seconds, and ranks after other peers until a block
  whose header it served completes. A header it serves in the meantime lifts
  neither, and nothing is banned or stored. A live header also earns its
  peer's stored service evidence and Reth reputation only once its body and
  receipts arrive. The lane used to retry forever while still reporting itself
  ready, and to reward the header first.
- One execution peer serving a forged segment of the history bridge's header
  proof no longer blocks the bridge. The proof is checked from the finalized
  anchor down, each segment against the parent that the proven segment above
  it names. A segment that does not match is reported to Reth and fetched
  again, and the segments already proven are kept. A bad segment, or a wrong
  last block, used to wedge the proof, and every history stream waiting for
  it, until restart. A stream waiting for the proof now also stops once it is
  cancelled.
- eth/70 receipt requests take at most eight continuation rounds and 64 MiB of
  responses, instead of up to 64 rounds of up to 10 MiB each before any check.
  A block never takes more receipts than its known transactions, and each
  block is checked against its header as it completes. A peer whose receipts
  fail that check is banned, and the failure stored, like any peer serving
  invalid receipts.
- An existing execution P2P identity (`execution-p2p-secret`) that group or
  other users can access is restricted to mode 0600 on load, with a warning.
  Creating the identity removes its temporary file on every failure and syncs
  the directory after installing it.
- An `included` block now has its ancestry verified to an execution header the
  Ethereum sync committee attested. Post-merge headers carry no seal, so one
  execution peer could serve a self-consistent child of the tip, with logs of
  its choosing, and the node applied it as included. Both finality sources now
  verify the latest light-client optimistic update each slot: at least 342 of
  the 512 sync-committee members must sign its attested beacon header, whose
  execution branch proves the execution block number and hash. The live lane
  fetches nothing above the newest attested head. It requests the head's
  header, and the header chain below it, by hash, down from the head's own,
  and catches up with those proven headers, so a peer that serves another
  header for a requested hash is banned. An attested head is not final: a peer
  that lacks the block, such as an honest one on another branch after a reorg,
  is only cooled down, and a catch-up or proof that fails is proven again from
  the newest attested head, so one reorg of an attested head no longer stalls
  the lane. A lane reset, a peer reward, and clearing a withheld-header strike
  now need headers anchored that way, including for filtered-log blocks
  without matching logs and for head discovery.
- Verified finality that contradicts the chain the node followed no longer
  waits forever for a canonical block that never arrives. The finalized block
  must be the canonical block at its height, only retained blocks that link to
  it by parent hash are finalized, and a processor's coverage at that height
  must hold its hash. A contradiction with retained unfinalized blocks, such
  as after the lane stalled across a reorg, restarts the network lanes, and
  every start undoes retained unfinalized blocks that do not link to the newly
  verified finalized anchor as a reorg and never finalizes them. That includes
  a branch the network reorged away while the node was down. A contradiction
  with finalized history, or the same unfinalized one recurring a third time
  before finality moves to another block, halts the network lanes, not ready
  and not retried, with the error in the log and in `network.supervisor` (see
  the runbook's "Finality contradicts the followed chain").
- Web pages can no longer read from or change a node's native API or
  JSON-RPC through the visitor's browser, with the exceptions below. The
  native API answers a `Host` name other than `localhost` and those in the
  new `api.allowed_hosts` with 421, so a page that points its own name at
  127.0.0.1 is refused; IP addresses, which such a page cannot send, pass.
  Both listeners answer a browser `Origin` other than a loopback one or one
  listed in the new `api.allowed_origins` and `rpc.allowed_origins` with 403,
  which guards event streams and WebSocket upgrades too. The native API also
  answers 403 to a GET or HEAD that another site's page sends without an
  `Origin` other than as a navigation, such as an image load, when the
  browser marks it with Fetch Metadata. Mutations need a JSON body or
  `x-leani-request: 1`, and JSON-RPC calls `application/json`, so a
  cross-site form or `text/plain` post gets 415. Creating a query snapshot,
  which counts against a node-wide cap of 32, now takes a POST: any page
  could fill the cap with GET requests. Navigations from another site,
  including into a frame, and subresource loads from browsers without Fetch
  Metadata still reach GET routes, except that the consumer stream routes
  refuse such navigations (see below). The page cannot read the response.
- JSON-RPC bounds its work and output. A batch holds at most 100 calls, and
  a response or batch response at most 16 MiB, with each call past that
  answered by `-32005`. `eth_getLogs` returns at most 10,000 logs, and a log
  filter lists at most 1,000 addresses and 1,000 alternatives per topic. A
  WebSocket connection holds at most 128 subscriptions, and at most 256
  connections are open. A 3 KB cross-origin batch used to produce a 4.2 MB
  response, and one connection could hold 2,048 subscriptions.
- `serve` refuses a native API bind beyond loopback without
  `api.bearer_token_env`, and JSON-RPC binds beyond loopback, which have no
  authentication, unless the operator sets `api.allow_unauthenticated_remote`
  or `rpc.allow_unauthenticated_remote`. The container profile served every
  interface without a token. The bearer token must hold at least 16
  printable ASCII characters without spaces and is compared in constant time,
  the `Bearer` scheme matches in any case, and `--help` no longer prints the
  values of `LEANI_API_TOKEN` or `LEANI_CHECKPOINT_URLS`, whose provider URLs
  can carry API keys.
- A durable consumer created with a credential now needs it on every
  consumer-scoped route, in addition to any bearer token: its lease, change,
  and acknowledgement routes, and its live and backfill streams with their
  leases and acknowledgements. A missing or wrong credential gets 403. The
  header was optional, so any client of the node could renew, read, or
  acknowledge another consumer, and move its cursor past changes it never
  received. Credentials are compared in constant time and stored as BLAKE3
  hashes keyed with a random per-store secret, and a new one must hold 32 to
  512 printable ASCII characters without spaces. A backfill subscription's
  credential also enters the subscription's idempotency identity, an
  unkeyed BLAKE3 hash of the whole request that the store keeps. Consumers
  declared in the node configuration have no credential and still need only
  the bearer token. Credentials isolate applications on these delivery
  routes only: listing, inspecting, and revoking a consumer stay
  administrative operations that only the bearer token protects, whether or
  not the consumer has a credential, so an operator can revoke a consumer
  whose credential is lost.
- Consumer session tokens carry a BLAKE3 MAC keyed with the per-store secret
  instead of an unkeyed checksum, so a client can no longer forge a token for
  another consumer's session from the values the API shows.
- An acknowledgement can no longer pass what the consumer was delivered. A
  consumer page read, and each live batch, backfill batch, or completion a
  stream sends, records the consumer's delivered sequence; an
  acknowledgement beyond it gets 409 `acknowledgement_beyond_delivered`. It
  was checked against the stream head only, so a client could acknowledge,
  and let retention prune, changes it never received.
- The generic consumer lease and acknowledgement routes no longer act on a
  consumer of a split live stream, whose streaming sessions fence both, and
  answer 409 `consumer_session_required`; they let a client without the
  session move the consumer.
- The live and backfill consumer stream routes answer 403
  `cross_site_request` to any request whose `Sec-Fetch-Site` is `cross-site`
  or `same-site` and that carries no allowed `Origin`, navigations included:
  a hidden frame or a redirect on another site's page could open a
  consumer's stream and hold its session lease, locking the consumer out.
  Requests without Fetch Metadata still reach these routes, which the bearer
  token and the consumer's credential guard: browsers send it only to HTTPS,
  loopback, and `localhost` URLs, so a node served over plain HTTP by
  another name or address gets none.

### Changed

- Breaking for HTTP clients of the native API that do not use the SDK:
  - A POST or DELETE needs `content-type: application/json` or the header
    `x-leani-request: 1`; one that sends no JSON body, such as a lease renewal,
    a snapshot release, a job cancellation, or a live-lane reset, now gets
    415 `unsupported_media_type` without the header.
  - `POST /v1/processors/{processor}/collections/{collection}/entities`, with
    the query as a JSON body, creates a query snapshot and returns its first
    page. A GET without a cursor no longer creates one: it returns a page of
    current output (`data`, `nextCursor`, `retainedBounds`, `coverage`), and
    its `nextCursor` continues that read, which sees later commits. A GET
    with a snapshot cursor still reads the snapshot's next page.
  - Requests whose `Host` names neither an IP address, `localhost`, nor a
    listed name get 421 `host_not_allowed`, and requests from a browser
    `Origin` that is not loopback or listed get 403 `origin_not_allowed`. List
    the names and origins clients use in `api.allowed_hosts` and
    `api.allowed_origins`. A GET or HEAD whose Fetch Metadata marks it as a
    cross-site or same-site subresource load gets 403 `cross_site_request`.
- Breaking (configuration): a native API bind beyond loopback needs
  `api.bearer_token_env` or `api.allow_unauthenticated_remote = true`, and a
  JSON-RPC HTTP or WebSocket bind beyond loopback needs
  `rpc.allow_unauthenticated_remote = true`. A configured bearer token shorter
  than 16 characters, or with spaces or non-ASCII characters, fails startup.
  `deploy/container.toml` now reads its bearer from `LEANI_API_TOKEN`, opts in
  to remote JSON-RPC, and lists the compose service name `leani` in
  `api.allowed_hosts`. `compose.yaml` passes the token through and stops
  every command, including the benchmark database's, while it is unset.
- Breaking (JSON-RPC): calls need `content-type: application/json` (415
  otherwise), browser origins other than loopback need
  `rpc.allowed_origins` (403 otherwise), and the limits above apply: a longer
  batch gets one `-32600` error, and an over-limit `eth_getLogs` fails with
  `-32005` "query returned more than 10000 results. Try with this block
  range [...]". The `rpc.max_*` settings change each limit. A single block
  with more matching logs than the limit cannot be read with `eth_getLogs`;
  raise `rpc.max_log_results` or use `eth_getBlockReceipts`.
- Breaking (JSON-RPC): block lookups answer `null` only for a block number
  above the node's head: its newest canonical block, or the block its
  progress processor has reached when that is higher. A block at or below the
  head that neither the recent window nor on-demand history serves now fails
  with `-32004` (`block_not_retained`, or the history source's reason)
  instead of `null`, and so does a block hash that the recent window lacks
  when no history block-hash lookup is configured, and `latest` on a node
  that retains no block. This covers `eth_getBlockByNumber`,
  `eth_getBlockByHash`, `eth_getBlockReceipts`,
  `eth_getBlockTransactionCountBy*`, and `eth_getTransactionByBlock*AndIndex`.
  Ethereum clients read `null` as "no such block", so a reorg checker
  concluded that a block below the retained window had been reorged out.
  Clients that took `null` for "not available here" must handle `-32004`. A
  block number above the head that no history source covers now reads as
  `null` instead of failing with `no_viable_historical_source`.
- Breaking (JSON-RPC): requests and parameters follow the JSON-RPC 2.0 and
  Ethereum specifications more closely:
  - A call with `"id": null` gets a response with a `null` ID; it was taken
    for a notification and got none. An object, array, or boolean `id` gets
    `-32600` instead of being echoed.
  - A QUANTITY must be `0x` and hexadecimal digits without leading zeros:
    `0x+1` got through, and now gets `-32602` like `0x01`.
  - The `finalized` and `safe` block tags name the node's verified finalized
    head, and `earliest` names block 0, wherever a block number is accepted,
    `eth_getLogs` bounds included; they got `-32602`. `safe` is the finalized
    head, which is never newer than the safe block other clients report.
    `pending` gets `-32004` (`pending_block_unavailable`): Leani builds no
    pending block.
  - An empty topic alternatives array, `[]`, matches any topic at its
    position in `eth_getLogs` and `eth_subscribe("logs")` filters, as in geth
    and reth; it matched nothing.
  - A request body that is not valid UTF-8 gets a `-32700` parse error with
    HTTP 200 instead of a plain-text HTTP 400, and a body over 1 MiB gets
    HTTP 413 with a JSON-RPC `-32005` error (`request_size_limit_exceeded`)
    instead of plain text.
  - An `eth_getLogs` call refused for the batch's remaining response budget
    ends the batch like any response past `rpc.max_response_bytes`: every
    later call gets `-32005` without running. Later calls used to run.
  - `eth_config` picks the current fork by the retained head block's
    timestamp only. Without a retained head frame it fails with `-32004`
    (`eth_config_head_timestamp_unavailable`) instead of picking the fork by
    the progress processor's block number, or the newest scheduled fork on an
    empty node, and a head older than the first scheduled fork fails with
    `eth_config_head_predates_schedule`.
- SDK: `processors.queryEntities()` without a cursor creates its snapshot with
  the new POST route, and mutations without a JSON body send
  `x-leani-request: 1`. Update the SDK together with the node.
- Breaking for durable consumers of the native API:
  - A consumer created with a credential needs `x-leani-consumer-credential`
    on each of its lease, change, acknowledgement, and stream requests (403
    otherwise). A new credential needs 32 to 512 printable ASCII characters
    without spaces (400 otherwise), such as `openssl rand -hex 32`. Existing
    credentials keep working, and a shorter one still gets 409
    `consumer_exists` when its consumer is created again, or its
    subscription's status when an unchanged backfill request is submitted
    again. A credential stored before the upgrade that begins or ends with
    whitespace cannot be sent as it is, since HTTP trims header values, and
    one with a control character cannot be sent at all; now that the header
    is mandatory, revoke such a consumer and create it again with a new
    credential.
  - An acknowledgement needs a cursor this consumer was delivered (409
    `acknowledgement_beyond_delivered`, with `deliveredSequence` in the error
    details). On a live stream it must also end a block's commit, as every
    batch's `acknowledgeableCursor` does (409 `acknowledgement_mid_block`).
    Each live batch now holds one whole block: a page read that ended inside
    a block used to send it in two batches.
  - For a processor with a split live stream (`block_versioned_idempotent`
    delivery ordering), `POST .../consumers/{consumer}/lease` and `/ack`
    answer 409 `consumer_session_required`; renew and acknowledge through
    `/streams/live/consumers/{consumer}/lease` and `/ack` with the stream's
    session token.
  - Any session token the node cannot verify gets 409
    `consumer_session_lost`: every token issued before the upgrade, and a
    malformed one, which got 400 `cursor_invalid`. The SDK treats the 409 as
    a lost session and opens a new stream on its own; other clients
    reconnect for a new token.
  - A query snapshot of a `finalized_only` processor returns
    `boundaryCursor` and `recovery.follow` as null: its retained state holds
    blocks whose changes are not published yet, so following from it would
    keep reorged rows, and query-and-follow refuses such processors.
- SDK: `BackfillDeliverySession.deliveryBatches()` yields every
  acknowledgement boundary of a backfill stream with the changes it covers,
  including batches without changes and the completion, in the shape of the
  unified history lane. `events()` is unchanged and carries no boundary; do
  not acknowledge a change's own cursor.
- `THIRD_PARTY_LICENSES.txt` is now committed, verified against `Cargo.lock`
  in CI, and linked from the README and site. It adds the Apache-2.0 NOTICE
  files of the Arrow, Parquet, object_store, and Moka dependencies, which the
  generated listing previously omitted. The Homebrew package now also installs
  Leani's own `LICENSE`, and the container image declares OCI license metadata.
- Node stores move to schema 23. Schema 22 adds coverage lookup indexes.
  Schema 23 adds a random per-store secret, drawn once from the operating
  system, for session tokens and consumer credential hashes; keys the stored
  credential hashes with it, so existing credentials keep working; and starts
  every consumer's delivered sequence at its stream head, so acknowledgements
  of changes delivered before the upgrade still pass. A schema-21 or -22
  store upgrades in place on its next start; earlier binaries then refuse
  it, so keep a backup if you might roll back. A backup holds the secret.
- Breaking for processor authors (`leani-processor-api`):
  `DataRequirement::validate_frame` is stricter for requirements with
  `allow_filtered`. Each filtered component that can supply a required
  capability, directly or by derivation, must claim predicate completeness
  (`Completeness::Partial` is rejected), and its `FilterScope` must cover the
  requirement's `filter` at the frame's block. Material filtered for another
  consumer's predicate no longer passes.
- Breaking (`leani-processor-api`): `ProcessorInstanceId::legacy` returns
  `Result<ProcessorInstanceId, ProcessorInstanceIdError>` instead of panicking
  when the derived key is invalid, which happens for a version with `+` build
  metadata or when the ID and version exceed 192 bytes together. Decoding a
  descriptor without an `instance` reports that as an error.
- Breaking (`leani-primitives`, `leani-processor-api`): decoding validates
  values the way their constructors do. A `BlockRange` whose end precedes its
  start, `CapabilitySet` or `LogFieldSet` bits that name no capability or log
  field, and a `ProcessorId` that `ProcessorId::new` rejects all fail to
  decode. Encodings are unchanged; new golden fixtures pin the `BlockFrame`,
  `ProcessorCursor`, and `EncodedDelta` durable encodings.
- `leani-primitives`: `BlockRange::len` saturates at `u64::MAX` for the full
  `0..=u64::MAX` range instead of overflowing, and the new
  `BlockRange::checked_len` returns `None` for it. The new
  `FilterScope::covers` and `FilterScope::covers_at` check whether material
  filtered by one scope is complete for another filter.
- `erc20-balances` 1.1.0 and `evm-events` 1.1.0 change stored output (see
  Fixed). Existing instances need a rebuild: set `version = "1.1.0"` and
  configure a new `instance`, which indexes from its `start_block`. The node
  refuses the new version under an existing instance ID (`processor instance
  … conflicts with its stored descriptor`), and a 1.1.0 binary cannot run a
  1.0.0 instance. Move consumers to the new instance's stream. The old
  instance's rows stay in the store, because no command deletes a single
  processor instance. To reclaim that space, rebuild in a new `data_dir`; or,
  only when nothing else in the data directory must be kept, run
  `leani reset all`, which deletes the whole directory, including every
  processor's state and delivery history, raw history, and the P2P identity.
  The ERC-20 entity and change schemas are now `erc20.balance.entity.v2` and
  `erc20.balance.change.v2`, and both processors use delta schema 2.
- `blobs-money` 1.5.0 changes stored output from Fusaka on (see Fixed).
  Rebuild existing instances the same way: set `version = "1.5.0"` and
  configure a new `instance`, which indexes from its `start_block`; the node
  refuses 1.5.0 under a 1.4.0 instance ID. Entity, change, and delta schemas
  are unchanged.
- ERC-20 balances gain `incompleteFrom` (`null`, or the block from which the
  token/holder pair is no longer derived) in the typed query, the change JSON,
  the OpenAPI `Erc20Balance` schema, and the SDK `Erc20Balance` type.
- Breaking (`leani-processor-api`): `LifecyclePolicies::validate` rejects
  `StatePolicyMode::Ephemeral` for every publication policy, and
  `StatePolicyMode::Checkpointed` unless the checkpoint policy is
  `automatic`. The store persists processor state in every mode, and only the
  checkpoint policy takes checkpoints, so these modes promised behavior that
  did not exist. `[processors.state] mode = "ephemeral"` now fails validation,
  and the configuration schema no longer offers it.
- `leani-processor-api` documents the change-to-mutation contract on
  `ReducerTransaction::emit`: at most one change per key and block, matching
  the key's net mutation.
- `leani-testkit` has no public signature changes, but tests may observe:
  `MemoryReducer` keeps `state_*` data apart from output entities and rejects
  scan limits outside `1..=10_000`, as the store does. `fixture_frame` gives
  every block number its own hash (numbers below 255 keep `[number; 32]`) and
  saturates its timestamp. `ScriptedLiveSource::subscribe` rejects, as the
  execution P2P live source does, a request for another chain, for
  capabilities its descriptor does not supply completely (even when the
  request accepts filtered material), or for finality it does not reach. The
  generated Uniswap-like corpus emits V3 `Swap` logs with sender and
  recipient topics, which changes its frame digests.
- The built-in `ETH/USDT` and `WBTC/ETH` Uniswap V3 markets start at the V3
  factory deployment block 12,369,621 instead of 12,376,729, the creation
  block of the USDC/WETH 0.05% pool, which both pools may predate. No V3 pool
  predates the factory, so this is a safe lower bound. `ETH/USDC` keeps its
  exact creation block, so an ETH/USDC-only starter is unchanged.

  Affected are compact `[uniswap]` nodes and `leani subscribe uniswap-v3`
  subscriptions whose markets include `ETH/USDT` or `WBTC/ETH`. Their data
  from earlier releases may miss those pools' events between pool creation
  and block 12,376,729, and the new start block changes the processor's
  identity, so the node refuses the processor its store holds:
  `processor instance uniswap-observations conflicts with its stored
  descriptor` (`cli-uniswap-v3-prices` for a subscription). The node prints
  the routes for that refusal before the message. In order of preference:

  1. Start the node with a new `data_dir`. Nothing is deleted, and the old
     directory stays usable with the previous binary as a rollback.
  2. Replace the compact document with an advanced configuration, as expanded
     in `config/defaults/ethereum-mainnet.toml`, whose `uniswap-observations`
     `[[processors]]` entry names a new `instance`. The node keeps its raw
     history, P2P identity, and embedded subscriptions; the old instance's rows
     stay in the store.
  3. For a subscription, `leani reset subscription uniswap-v3 <markets> --yes`,
     with the subscription's `--finality`, `--data-dir`, and `--config`
     options, deletes only that subscription's state. The refusal prints this
     exact command.
  4. Only as a last resort, `leani reset all` deletes the entire data
     directory: the node database with every processor's state, output,
     delivery history, and consumer positions, raw history, processor
     artifacts, the execution peer store and P2P identity
     (`execution-p2p-secret`), `checkpoint.json`, and every embedded
     subscription, and with them the rollback.
- `transaction-stats` documents `count` and `totalValueWei` as submitted
  totals: without receipts, reverted transactions and their value are
  included. Behavior is unchanged.
- Verified finality persists its newest epoch-aligned anchor as
  `finality-anchor.json` in `data_dir`, written off the async runtime.
  `/metrics` exports `leani_finality_anchor_age_seconds`,
  `leani_finality_anchor_expiry_seconds` (the time left before a restart can
  no longer bootstrap from that anchor), and
  `leani_finality_anchor_write_failures_total`. `leani doctor` warns, and
  lists the warning under `warnings` in `--json`, when the anchor a restart
  would use expires within three days or was verified from another
  checkpoint. The alert rules add `LeaniFinalityAnchorExpiring`.
  `leani source probe finality` and the benchmark's history probe start from
  the persisted anchor by the same rules but never write it.
  `leani reset all` removes the file.
- Configuration validation rejects `finality.minimum_peers` above 24, the
  consensus P2P peer set; an all-zero `finality.checkpoint`; and
  `finality.endpoints` that name one transport twice, even with different
  letter case, default port, trailing slash, credentials, or query string.
- `leani init`, and `leani subscribe` without a configuration, use the
  managed Beacon endpoint pool plus every responding checkpoint provider that
  also serves the Beacon light-client API, without duplicates, instead of the
  responding providers alone.
- `leani-source-api`: `NetworkSessionTelemetry::observe_head` takes only
  verified heads; its signature is unchanged. Execution P2P now reports as a
  session's `observedHeadBlock`, and so as `syncTargetBlock` and the base of
  `blocksRemaining`, the newest header it validated, not the highest head a
  peer claimed in its status. One status claiming block 18446744073709551615
  set that value for the session's lifetime. While the node catches up, these
  fields now follow its validated progress instead of the claimed network
  head. The network dashboard calls it the verified head throughout.
- Execution P2P schedules every material request under one process-wide
  limit, the larger of `material_request_concurrency` and
  `history_header_request_concurrency`, instead of comparing each caller's
  own limit with the shared count. Live-lane requests may use every slot.
  History, probes, and background peer qualification leave four slots free,
  or half the limit (rounded down) when it is below eight, and a waiting live
  request gets the next free slot first. Qualification never runs at high
  priority.
- Breaking (`leani-source-api`): `FinalityEvent::Finalized` gains
  `block_number`, the finalized execution block's number, next to its hash;
  construct and match it with the new field. The crate adds `AttestedHead`,
  and `AttestedHeadPublisher` and `AttestedHeadReceiver`, the two ends of a
  latest-value channel: a finality source publishes the newest verified
  attested head, and the execution live lane only receives it. It also adds
  `NetworkTelemetry::supervisor_halted`, and now depends on `tokio` with the
  `sync` feature only.
- Breaking (configuration): live following requires verified finality.
  `sources.live.kind = "p2p"` with `finality.kind = "disabled"` already failed
  validation; its error now says why: without a finality source nothing
  verifies the attested heads that bound what the live lane includes. Every
  shipped profile either enables both or disables both, so none changes.
- Included blocks trail the chain tip by one to two slots, the time an
  optimistic update takes to attest the head. The live lane reports itself
  disconnected, dropping live readiness, when no new attested head arrives for
  four slots. `source probe finality` reports the selected attested head, and
  each Beacon endpoint's `attested_head` or, in `attested_head_error`, why it
  gave none. Attested heads need no agreement between endpoints, since each
  carries the sync committee's signature; two different ones at one slot are
  ignored. Consensus P2P asks the next peer when one serves a head no newer
  than the published one.

### Fixed

- Verified finality no longer stops about 14 days after the configured
  checkpoint's slot. Beacon API finality keeps one bootstrapped light client
  per endpoint and advances it with each finality update, plus the
  sync-committee update when a period ends. It no longer verifies the whole
  chain again from the checkpoint, including its age check, on every
  12-second poll. Every start and network lane restart, with either finality
  source, bootstraps from the persisted anchor when that is newer than the
  configured checkpoint and younger than 14 days, so the configured
  checkpoint's age no longer matters after the first start. A corrupt or
  unreadable anchor file falls back to the configured checkpoint with a
  warning, and so does a persisted anchor whose bootstrap no endpoint or peer
  serves. A Beacon API endpoint that is down at start or bootstraps slowly
  bootstraps later from the newest agreed anchor, so it can join after the
  configured checkpoint ages out; its bootstrap is no longer cancelled by the
  agreement grace period.
- Beacon API agreement waits about two seconds after the first verified
  endpoint for the others, then follows the highest finalized slot that
  `finality.minimum_agreement` endpoints have reached or passed. A lagging
  endpoint no longer holds finality back, readiness no longer drops at epoch
  transitions, and finality never moves backwards.
- A Beacon API endpoint without a trailing slash keeps its last path segment:
  `https://host/beacon` is queried as `https://host/beacon/eth/v1/...`.
- One invalid or stale peer no longer ends consensus P2P finality and restarts
  every lane. A peer whose light-client material fails verification is
  banned, and neither asked nor dialed again across reconnects until the
  lanes restart; the next peer is tried, peers are tried in random order, and
  updates that are stale, or signed ahead of the local clock beyond one slot
  of tolerated skew, are retried. An update whose own slots are inconsistent
  is invalid. Contradicting verified anchors still fail closed.
- The consensus P2P Status message advertises the checkpoint's finalized
  epoch rounded up, so a checkpoint whose epoch-boundary slot was skipped
  advertises its own epoch, with a head slot at or after that epoch's start.
- New node stores enable SQLite incremental auto-vacuum, so pruning shrinks
  the database file and storage admission recovers instead of staying at the
  high-water mark. Existing stores log a warning at startup until one
  `leani db compact` converts them.
- The node database path is used literally: `%` and `?` in `data_dir` no
  longer make SQLite open a different file.
- A new store's schema is created in one transaction, so an interrupted first
  start no longer leaves a store that every later start refuses.
- Retained recent frames report their block's current finality after
  finality promotes it. Recompute backfills over the retained recent window no
  longer fail with "historical microbatch must be contiguous, finalized, and
  single-chain", and live-lane replays apply promoted blocks as finalized.
- `publish = "finalized_only"` processors with canonical delivery ordering
  publish their held blocks before a block applied directly as finalized, such
  as after gap recovery, so the stream stays in block order.
- Undoing a block restores each output entity's previous block, timestamp,
  finality, and write time instead of stamping the reverted block. Undo
  records written by earlier versions keep the old stamping.
- Rows that a window output policy pruned while applying an unfinalized block
  come back when that block is undone.
- A full artifact budget no longer stalls finality for every processor.
  Finality commits, finalized artifact candidates stay pending with a warning,
  and a later finality advance promotes them once the retained budget has
  room.
- The undo journal no longer grows forever. Blocks applied as already
  finalized record no undo, and each finality advance deletes finalized
  records more than `[processors.undo] safety_blocks` below the finalized
  height, or all of them with `mode = "none"`. Unfinalized blocks keep their
  records, so reorgs undo as before. A backlog from earlier versions drains
  by at most 10,000 records per finality advance.
- Automatic recovery checkpoints are taken at most once per 1,000 finalized
  blocks instead of on every finalized block and finality event, and they
  stream processor state instead of loading it whole. `keep` is unchanged. A
  completed historical job also checkpoints its processor at rest, and a
  restore accepts a checkpoint whose block was finalized after it was taken.
  A restore still needs a checkpoint at the current cursor, so between
  checkpoints restore a full-store backup or rebuild the processor; portable
  savepoints are exports and cannot be restored.
- Checkpoints and portable savepoints stay valid after lifecycle edits such
  as raising a limit: their identity is the processor contract without its
  lifecycle policies. Snapshots written by earlier versions stay valid while
  the descriptor is unchanged.
- Each processor instance keeps at most 16 portable savepoints, and a new one
  is admitted against the physical store budget. The API answers a full quota
  with `409 savepoint_limit`.
- Coverage that arrives out of order, such as an older backfill range that
  finishes after a newer one, is now owned and therefore compacted. Adjacent
  owner ranges coalesce.
- Re-applying a block that coverage compaction already folded into a compact
  interval is an idempotent no-op instead of re-running the reducer and
  publishing duplicate changes. Hot/cold handoff verification accepts
  compacted blocks through their segment's end hash. Compaction leaves the
  overlap of a running handoff exact until it is verified, logging that once
  per handoff, and a new handoff fails any unverified one left by an
  interrupted run.
- Reducer prefix scans (`scan_prefix`, `state_scan_prefix`) keep reading
  until `limit` rows survive the reducer's own uncommitted deletes, so pages
  are no longer short and an uncommitted key no longer skips ahead of unread
  committed keys.
- A `required` consumer is rejected on a live stream whose delivery mode is
  not `until_acknowledged`, where it would pin window retention indefinitely.
  Backfill streams still accept it. Consumers registered earlier keep their
  role, and re-creating one still returns `409 consumer_exists`. A store that
  holds such consumers logs a startup warning listing them: revoke each one
  (`DELETE /v1/processors/{processor}/consumers/{consumer}`) and register a
  `best_effort` consumer under a new ID, or switch the processor to
  `until_acknowledged` delivery. The durable-consumer SDK example now
  registers a best-effort consumer, matching the window-retained stream of
  the PostgreSQL guide.
- Deleting terminal backfill work removes its subscription ranges and coverage
  ownership even when the job ID differs from the subscription ID.
- A late microbatch commit or completion no longer overwrites a cancelled or
  failed job, and a late state change no longer revives a cancelled, failed,
  or reclaimable subscription.
- Store stats and `/metrics` report changes held for `finalized_only`
  processors: `deferred_changes`, `deferred_change_bytes`,
  `leani_store_deferred_changes`, and `leani_store_deferred_change_bytes`.
- Refusing an older store schema names the actual reason instead of always
  claiming the store predates the included/finalized vocabulary.
- `/health/live` and `/health/ready` no longer count whole store tables on
  every probe, which could exceed the container health check's 3-second
  timeout on large stores. They answer from in-memory readiness plus a trivial
  SQLite query. `/metrics` and `/v1/network/status` reuse store-wide and
  per-processor row and byte counts for up to 10 seconds, and `/v1/status`
  reads the database size from page counts.
- A processor's finalized height and coverage head are found with index
  searches instead of scanning all of its coverage on each metrics scrape,
  status poll, or undo, and block-hash coverage lookups are indexed.
- Read-only API paths no longer queue for the store writer, where each one
  held back history writers (they yield to every waiting live writer), so
  steady API traffic could starve backfill. Consumer change pages, change
  bounds, and delivery stream listings and statistics read without it and no
  longer register an unknown processor as a side effect, and re-registering
  an unchanged processor writes nothing. Query snapshots check coverage,
  retention, and size caps before taking the writer and stop counting once a
  cap is exceeded, so `query_too_expensive` reports at least that many rows
  and bytes; only the copy itself waits for the writer.
- Historical backfills no longer reuse retained recent frames whose material
  was filtered for other processors, which could silently produce incomplete
  results. Such frames now miss and the job reads from its source.
- A paused processor lane that resumes no longer replays retained frames
  filtered for the processors that stayed live meanwhile. Live-gap replay
  treats such a frame as missing: it recovers a finalized block from the
  configured history source. At an unfinalized block, as for a block that was
  never retained, only that processor's lane pauses
  (`unfinalized_gap_waiting_for_finality`); the first drain after finality
  reaches the block recovers it from history, with no operator reset. A
  mapping failure during block-local gap replay now also fails only that
  processor's lane (`processor_live_mapping_failed`), as ordered replay already
  did, instead of stopping shared live ingestion. The replay-miss log is now
  at debug level, since a waiting lane retries on every drain.
- A source request compiled for several filtering requirements no longer
  narrows a field that one requirement leaves unfiltered. Previously an
  unfiltered field such as `addresses` took another requirement's list, so the
  source could omit material the unfiltered requirement needed.
- `coverage_gaps` in `leani-source-api` no longer reports a range covered
  through block `u64::MAX` as missing.
- A processor whose reducer rejects a block no longer stops live ingestion for
  every processor, and a restart no longer loops on the same block. Reducer
  errors, and the store's per-apply invariant errors, fail only that
  processor's live lane (`processor_live_reduce_failed`) while the others keep
  following. This holds for every processor mode and delivery ordering,
  including block-local processors with the default canonical ordering, which
  used to stop the whole lane. A delta that conflicts with the one already
  applied for its block (`processor_live_delta_conflict`), and a mapping or
  finality-variant failure while replaying (`processor_live_mapping_failed`),
  likewise fail only that lane. Live commits and every live-gap replay path
  apply this one isolation policy, so a replay after a restart or an operator
  reset parks the lane again instead of failing the live lane. A failed lane
  keeps its first failure: it is mapped only to move its first unapplied block
  across a reorg, a later failure does not change its reason, and a later park
  does not move its first unapplied block. Only a finality contradiction
  (`processor_finality_conflict`) replaces the reason. The store applies this
  rule atomically, so a park that races another component failing the lane,
  such as finality, cannot unfail it; the store still records the first
  unapplied block of a lane it fails itself for a delivery limit. The live-lane
  reset route (`POST /admin/v1/processors/{processor}/lanes/live/reset`) now
  accepts processors with canonical delivery ordering, which include every
  ordered processor, instead of answering
  `409 processor_has_no_split_live_stream`.
- A failed automatic cold backfill, or a failed hot/cold overlap verification,
  no longer ends the network lanes for every processor. That processor's
  handoff is recorded as failed and its live lane pauses at the live tip
  (`hot_cold_handoff_failed`); the other handoffs and live lanes continue. A
  block-local lane replays and resumes at once. An ordered lane resumes once a
  later backfill passes its gap: ordered replay now moves past a parked block
  that history has already applied, instead of waiting for it forever.
- A lane whose gap crosses the finalized anchor the node seeds at startup no
  longer fails the whole live lane on every restart. The seeded canonical row
  has no parent hash until the block's frame arrives; retaining that frame now
  fills in its parent and timestamp, and a gap check treats a missing parent as
  unknown instead of as a broken chain. A gap whose next canonical block truly
  does not descend from it fails only that lane
  (`live_gap_canonical_identity_changed`).
- The live source request now covers every configured processor, including
  lanes that are paused when the live lane subscribes. Frames retained while a
  lane is paused carry its material, so it resumes by replaying them. A reorg no
  longer fails a paused lane that cannot map the replacement block: the lane
  stays paused, its gap moves to the replacement, and its own replay maps it.
  A reorg that replaces blocks at or below a parked lane's gap moves the gap
  down to the first replacement block. Before, an ordered lane stalled and a
  block-local lane skipped the replacement blocks below its gap, or, after a
  reorg to a shorter branch, resumed with a hole. A reorg without a
  replacement branch no longer leaves a parked lane's gap on a block the chain
  no longer has: the gap moves onto the new tip, which the lane has applied,
  so a paused lane resumes at once, and a failed lane's gap moves onto the
  tip's successor once that block arrives, where a reset replays it. Before,
  a block-local lane skipped the next block or failed
  (`live_gap_canonical_identity_changed`), and an ordered lane stalled. A gap
  found off the canonical chain, as earlier versions could leave one, moves
  back to the last canonical block the lane applied, instead of being
  completed past blocks the lane never applied or failing the lane.
- The shared live lane checks continuity before any processor maps a block. A
  block must extend the canonical tip or repeat a block already retained, and a
  reorg must first revert the canonical tip. A source that breaks this fails
  the lane with `LiveGap` or `InvalidReorg`, and the supervisor reconnects,
  instead of the block being applied.
- The recent-frame hard limit, `budgets.recent_raw_hard_bytes`, now applies
  when live ingestion retains a frame, not only when finality prunes. At the
  limit live ingestion waits with readiness down (`recent_storage_full` in the
  log) and resumes once finality prunes; no unfinalized frame is dropped. After
  15 minutes at the limit the live lane fails, so the supervisor restarts it
  from a fresh finalized anchor that finality can prune through. Finality no
  longer fails its lane when pruning leaves frames above the hard limit; it
  logs a warning.
- One processor's failed finality update no longer stops finality for the
  others or recent-frame pruning. It is logged, reported under
  `failed_processors` in the shared finality report, and retried at the next
  finalized anchor. A processor whose coverage holds the finalized hash at
  another height, or another block at the finalized height, contradicts the
  canonical chain: its lane fails (`processor_finality_conflict`), and that
  reason replaces any earlier failure reason. The reset route refuses that
  lane with `409 live_lane_requires_rebuild`, since only rebuilding the
  processor as a replacement instance repairs it. Finality skips any failed
  lane until it is reset, so no later anchor finalizes a contradicted lane
  through the contradicted height. A gap replay that finds its lane failed
  meanwhile, for example by finality, leaves the lane failed instead of
  failing the whole live lane.
- Backfills that reuse retained recent frames read them in bounded batches, one
  query each: at most `budgets.history_pipeline.commit.maximum_blocks` blocks
  and `maximum_mapped_bytes` encoded bytes, each holding an active-chunk slot.
  A long gap no longer loads the whole recent window into memory with one
  query per block.
- The live lane's gap drains, live commits, and the hot/cold handoff's
  reconciliation no longer race each other over one lane's gap. Two drains of
  the same gap could fail the live lane with "live gap marker changed".
- A processor's retained raw-history source now uses the filter its own
  backfill requests carry, which covers every requirement, instead of the first
  requirement's filter. With several differently filtered requirements, the
  source's material shape never matched those requests, and its filter could
  omit material another requirement needed.
- Network telemetry no longer panics when it shortens a long error message that
  contains non-ASCII text, such as one from a Beacon API endpoint. The panic
  stopped the network supervisor. Messages are now cut at a character boundary.
- A backfill paused by delivery or artifact backpressure, such as a
  subscription whose consumer stopped acknowledging, no longer holds history
  resources while it waits. It used to keep its node-wide active-chunk slots
  (`budgets.history_pipeline.maximum_active_chunks`), the chunks it had read
  ahead, its place in shared source reads, and the mapped blocks of the commit
  batch it could not deliver, counted against
  `budgets.history_pipeline.maximum_mapped_bytes`. So it could stop every other
  historical job, including the cold backfills a hot/cold handoff waits for,
  and hold jobs that shared a source read with it to its pace. A paused job now
  keeps only the one mapped block whose commit was refused. Once it commits,
  the job resumes from its durable progress and reads and maps the blocks it
  dropped again.
- Chunks that a backfill opens ahead of the one it is reading can use at most
  half of `budgets.history_material.memory_bytes`. The rest stays free for
  chunks being read. Before, read-ahead chunks could fill the whole budget
  while the chunk the job needed next waited for memory, and the job stalled
  for good.
- A historical backfill no longer stalls when shared material memory, or room
  in a shared read's frame buffer, is freed just as its reader finds it full.
  The reader now registers for the wakeup before it checks.
- A history source that panics while streaming fails its read with a
  retryable source error. Its backfills retry, on another source when one is
  configured. Before, every backfill waiting on that read hung.
- Transient history-source errors are retried per gap, not per job, and a gap
  that committed blocks before failing starts a fresh retry budget. A long
  backfill with a few scattered transient errors no longer fails. Each gap is
  read from the highest-priority source, and the job moves to the next source
  only after an error. Before, every gap attempt moved on to the next source,
  even after a success.
- A historical job whose task stops just after the node-wide commit scheduler
  picks it no longer leaves every other historical job waiting for a commit
  turn.
- A crash, abort, or error in the middle of a live commit or reorg no longer
  leaves holes in processors, reverted-branch output that finality could
  later finalize, stalled ordered processors, orphan pending deltas that fail
  the hot/cold handoff, or stopped lanes that skip blocks or cannot be reset.
  Every network-lane start now reconciles each processor with the canonical
  chain before its lanes open, and logs what it repaired; see
  [Crash or interrupted commit](docs/operations/runbook.md#crash-or-interrupted-commit).
  A lane the store pauses or fails at its delivery limit, for example in a
  cold-backfill commit, now records where it stopped, even without a restart.
- An ordered lane's replay onto a finalized anchor that the node seeded without
  a parent hash now checks that the block descends from the lane's cursor. The
  parent comes from the block's retained frame, or else from the frame that
  history recovery returns, and a block that does not descend fails the lane
  (`finalized_gap_recovery_canonical_mismatch`) instead of being applied.
- A reorg without a replacement branch onto a block that a paused block-local
  lane never applied, such as the finalized anchor the node seeds after
  downtime, completes the lane's gap, so the lane resumes at the next block.
  Before, the gap moved onto that block, which has no retained frame, and the
  lane waited for a history source to serve a block it did not need. An
  ordered lane, which needs that block in order, still waits for it there.
- A lane failed with `processor_live_delta_conflict` can now be recovered:
  startup reconciliation deletes a pending delta for a block the lane already
  applied, so restarting the node and then resetting the lane replays it.
  Before, the reset failed the lane again on the same delta.
- Logs from unrelated or hostile contracts no longer fail a block. Any
  contract can emit an event's topic zero, and `erc20-balances` failed the
  block on a `Transfer` log with non-zero address padding, and `evm-events` on
  any log that did not decode as the configured event, such as an ERC-721
  `Transfer` (four topics, no data) against the ERC-20 ABI or a `bool` word
  other than 0 or 1. Such logs are now skipped as not being that event, and
  each block's mapped delta counts them.
- `erc20-balances` no longer fails when a transfer cannot be applied to a
  watched balance. Before, one forged `Transfer(watched, x, 1)` from any token
  without an allowlist, or a rebasing token, underflowed the balance and failed
  the processor, even with `complete_from_start = false`. An outgoing transfer
  above the derived balance, or an incoming one that overflows it, now marks
  that token/holder pair incomplete from its block (`incompleteFrom`,
  `complete = false`): its balance keeps the last derived value and later
  transfers for the pair are ignored, and undoing the block restores the
  pair. Only a token in the `tokens` allowlist with
  `complete_from_start = true` still fails the processor, because the
  configuration declares that ledger complete.
- `evm-events` outputs with `key_fields` no longer fail a block that changes
  an existing key twice (`change … has 0 matching state mutations`): a block
  emits one change per key, for its last event. When the processor stores
  output entities (`[processors.output] mode` other than `none`), a keyed
  entity now holds the newest event by block number, transaction index, and
  log index, even when the hot and cold lanes apply blocks out of order;
  before, the block applied last won. An older event no longer replaces a
  stored newer one and emits no change. With `mode = "none"` no entity is
  stored to compare against, so events are published in the order blocks
  apply.
- `evm-events` accepts the included and finalized deltas of one block as
  equivalent on replay. Its entities carry the block's finality, and the
  processor only accepted the exact checksum, so replaying a block whose
  finality changed could report a conflicting delta once the frame was
  pruned.
- `evm-events` ABI parsing no longer reads an unnamed indexed parameter such as
  `address indexed` as a data field named `indexed`; unnamed parameters are
  rejected, since decoded values are keyed by name. A parameter whose name
  contains `anonymous`, such as `anonymousVoter`, is no longer rejected; the
  `anonymous` event attribute still is. The order of configured events stays
  part of the instance identity.
- `uniswap-latest` never lets an older observation replace a newer one, as
  `uniswap-observations` already did. Ordered lanes apply blocks in order, so
  stored output does not change and the version stays 2.0.0.
- `blobs-money` reports the blob base fee the protocol charges. From Fusaka
  on it used the larger of the EIP-4844 fee and the EIP-7918 reserve price,
  but EIP-7918 only changes how excess blob gas evolves. Wherever the reserve
  binds, `blobBaseFee`, the block's `blobEthBurnedWei`, and each blob
  transaction's `blobBurnedWei` and `totalBurnedWei` were overstated: zero
  excess blob gas at a 0.1 gwei execution base fee reported 6,250,000 wei per
  blob gas instead of 1. `reserveFeeWei` still reports the reserve price
  separately. The fee is no longer capped at 10^30 wei, and fork parameters
  follow the block's timestamp, as forks activate, instead of hard-coded
  block numbers. A fork's `activationBlock` remains the block-number label
  that `atBlock` schedule lookups use; for Dencun, 19,426,589 is the first
  block the processor covers, two slots after the fork activated.

  blobs.money's own fee formula has the same bug, so its compatibility exports
  stop matching from Fusaka on: `leani source probe xatu --expected-export`
  reports the block's `blobBaseFee` and `blobEthBurned` and the transaction's
  `blobEthBurned` and `ethBurned` as mismatches wherever the reserve binds,
  until blobs.money charges the protocol fee too.
- JSON-RPC receipts price blob gas (`blobGasPrice`) with the fork active at the
  block's timestamp, from the same schedule and fee function as `blobs-money`,
  instead of with Cancun's parameters for every fork. Prague and later
  receipts were wrong at non-trivial excess blob gas: 107,610,112 under Prague
  costs 2,150,273,305 wei per blob gas, not 99,710,729,314,173. When a block
  with a blob transaction cannot be priced, every receipt of that block, the
  receipts of its other transactions included, fails with `-32004` instead of
  carrying a Cancun price: `blob_gas_price_schedule_unavailable` when no
  scheduled fork covers the block's timestamp or the schedule is for another
  chain, `header_excess_blob_gas_missing` when the header lacks excess blob
  gas, and `quantity_exceeds_u128` when the price does not fit in 128 bits,
  which only a forged excess blob gas reaches. A node on a chain other than
  Mainnet without a blobs processor prices with the checked Mainnet
  schedule, so there the receipts of every block with a blob transaction
  fail. `leani conformance rpc` prices blob receipts the same way.
- `eth_getLogs` with `blockHash` returns only the logs of the block with that
  hash. It resolved the hash to a block number and then read the block at
  that number, so a reorg in between, or on-demand history holding another
  block at that height, answered with another block's logs. Such a call now
  fails with `-32004` (`block_hash_not_retained`).
- A request slot freed just after a waiting execution P2P request found none
  free no longer leaves that request waiting for the next release. Background
  peer qualification, with its limit of 32, no longer holds every slot while
  live requests, limited to 4, wait behind it.
- Polling execution peers for the next block asks one or two peers per poll
  instead of racing every eligible peer, and each poll asks peers that have
  not been asked for that block yet, until every eligible peer has been asked
  and the rotation starts over. An empty reply for a block that is not
  produced yet no longer counts as a failure with an exponential cooldown.
  Once the lane moves on with a block that a poll served, a peer that answered
  "not yet" for it after that serve, or up to 250 ms before it (at most the
  first retry pause), as a peer asked in the same poll does, gets the normal
  lane cooldown, so peers that withhold new blocks drop out of later polls;
  nothing is stored against them and they are not banned. Peers asked before
  the block existed are not cooled, so the polls of each slot no longer push
  honest peers toward the 30-second cooldown cap. The request for the minimum
  live head, the verified tip or the block after the finalized anchor, races
  three eligible peers at a time instead of up to 32, and an empty reply there
  cools the peer's header lane.
- A head poll whose last peer times out, or cannot take the request, no longer
  reports the live lane disconnected: the next poll asks other peers. Once no
  polled peer has answered for the head grace, 12 seconds by default, the lane
  reports itself disconnected, once, and keeps polling.
- The live execution P2P lane reconnects after the zero-peer watchdog stops
  the network manager (`sources.live.peer_recovery_timeout_seconds`, five
  minutes by default): its reconnection rebuilds the manager. The lane kept
  the stopped manager's session and never followed the head again, and a
  live body request kept waiting for a peer in the emptied pool. A live
  subscription still waiting for its first peer head now connects again too,
  instead of waiting forever.
- A shallow reorg while the live lane catches up is reconstructed from where
  the branch forks, not from the head discovered far ahead. That made the
  reorg look deeper than the retained window, reset every lane, and repeated
  after the restart. Reorg reconstruction retries a peer without the branch,
  a short reply, or a timeout, and reports the lane disconnected once until
  the lane moves again; only a branch proven to fork below the retained window
  resets the lane.
- eth/70 peers are no longer rejected for the receipts of blocks without
  transactions, and those receipts are no longer requested: the block's empty
  receipts root proves them.
- Stopping the node no longer waits five seconds for the execution network
  manager and then aborts it before it stores the final peer state.
- Execution peer qualification no longer marks a peer `lagging` from its
  handshake head, which is fixed when the session opens, but asks the peer.
  Lagging and timed-out probes are no longer stored as service failures and no
  longer clear a session's verified header and body lanes. The qualification
  target is a block number and hash, so advertising the same block again with
  another timestamp or parent no longer resets every peer's qualification.
- Execution peers that disconnect with `useless_peer` or
  `tcp_subsystem_error` no longer get a stored service failure. Remote peers
  send `useless_peer` to this node, which serves no chain data, and TCP
  subsystem errors are often local or transient.
- The direct execution-peer pool is reconciled with the network manager's
  active sessions every 10 seconds. A lost session event no longer leaves a
  closed session's request sender in the pool or hides a connected peer. A
  peer dropped for invalid material is not taken back while its session
  lasts, which Reth keeps open for trusted peers. Only the session that
  served the invalid material is dropped: a session the peer opened since
  stays in the pool.
- `leani source probe p2p` uses a throwaway identity, an in-memory peer store,
  and ephemeral ports, so it can run beside a live node without the
  data-directory lock and never changes the node's peer store or identity. It
  advertises the genesis block instead of the probed range's end with a zero
  hash, which made every peer fail qualification and stored those failures in
  the node's peer store.
- `leani e2e mainnet` takes the data directory's lock, so `--resume` no
  longer opens a store that a running node or another run is using.
- The execution peer store path is used literally: `%` and `?` in `data_dir`
  no longer open another file.
- A node restarted after a reorg of its retained unfinalized blocks no longer
  restarts its network lanes every minute: seeding the finalized anchor
  failed when a retained block of the reorged branch held its height. Such
  blocks are now reverted before the anchor is seeded.
- Typed query endpoints report the finality a block has now. Blobs blocks and
  snapshots, block summaries, ERC-20 balances, and Uniswap pools and
  observations read it from the entity's output metadata, which finalization
  updates, instead of the finality embedded in the entity when its block was
  reduced, which stayed `included`.
- The latest Uniswap observation of a backfilled block no longer answers 404
  "no longer canonical". Its timestamp comes from the entity's output
  metadata instead of a retained canonical block, which only blocks followed
  live leave.
- The SSE change stream resumes after the `Last-Event-ID` an EventSource
  sends when it reconnects, when `after` is absent. It ignored the header and
  restarted from the oldest retained change.
- A portable savepoint beyond the physical store budget, like any request the
  budget refuses, answers 507 `physical_storage_limit` with the limit and
  projected bytes in the error details, instead of 500.

## [0.1.0-rc.1] - 2026-09-20

The first public release is a preview. Review the documented preview
limitations before deploying it.

### Changed

- Prepared the processor-authoring Rust libraries for crates.io with explicit
  package contents, MIT notices, READMEs, and matching prerelease dependency
  versions. Other workspace crates remain private packages.
- Standardized Leani's code, packages, and release metadata on the MIT license.
  Dependencies and third-party data retain their respective licenses.
- Release publication now requires passing CI for the exact release commit.
  Native binary releases reuse an inspected candidate, exercise the offline
  fixture on all four target architectures, and distinguish prereleases from
  stable releases. SDK package checks cover both public entry points.
- Updated `h2` to 0.4.16 and `rustls` to 0.23.45 for security fixes, and
  replaced the yanked `chacha20` 0.10.1 dependency with 0.10.2.
- Chain-confidence vocabulary is now `preview`, `included`, and `finalized` on
  every `finality` field: CLI output, HTTP API, SSE, SDK types, and
  configuration. `optimistic` becomes `included`; `safe` is removed because no
  source produced it. `publish = "optimistic_and_finalized"` is now
  `included_and_finalized`, and `leani subscribe --finality` accepts `included`
  (default) or `finalized`. Rendered `data.finality` always equals the envelope
  `finality`. Node stores (schema 21) and embedded subscription directories
  created before this change are refused and must be deleted.
- Execution peer discovery now has one Discv4 owner: Reth's network manager.
  Leani continues to feed independently decoded EIP-1459 DNS records into the
  same bounded peer manager without running a second Discv4 crawler.
- `LiveSource::subscribe` now receives the compiled `DataRequest`, allowing
  sources to acquire only the material required by the active processor set.
- Uniswap 2.1 public entity/change JSON now uses camelCase fields, `0x` address
  and hash strings, decimal quantities, and signed decimal swap amounts.
- Compact finality endpoint lists replace the managed defaults when supplied,
  and runtime data directories are exclusively locked across processes.
- The CLI now discovers `./leani.toml` by default; `LEANI_CONFIG` and
  `--config` remain explicit overrides.
- Execution peer candidates and verified service evidence now use the bounded
  `execution-network.sqlite` WAL store with background incremental persistence;
  the pre-launch JSON peer-state formats are intentionally not migrated.
- Execution peer qualification is now a background ranking signal rather than
  a startup gate. Requests begin at the configured connected-peer floor and
  continue to commitment-check every response.
- `ProcessorFactory::id` accepts an ordinary borrowed string again, and
  `description` has a backwards-compatible default for downstream factories.
- Public documentation and the Astro landing page now live in this monorepo,
  render from verified examples and tracked Rust-owned contract fixtures, and
  promote production docs only from an exact release commit.
- Execution-P2P history fallback now admits every gap at or after the
  processor's configured start block by default. The optional
  `sources.live.history_fallback_blocks` setting applies a hard finalized
  suffix bound only when operators configure it explicitly.
- Query-extension aliases now live under the collision-free `/v1/q/{alias}`
  namespace. Typed SDK helpers accept canonical capability `basePath`
  overrides for ambiguous multi-instance deployments.
- Query-extension scan cursors use the pre-launch v2 encoding and bind
  pagination to extension-defined request scopes. Cursors emitted by the
  unreleased v1 implementation are intentionally not accepted.
- Change envelopes no longer repeat connection-scoped metadata: the
  `processor` summary is gone and `coverage` remains only on
  `reset_required` as the coverage hint. Streams now open every connection
  with a synthesized `hello` frame (`{apiVersion, chainId, processor,
  coverage}`, no SSE id); the SDK exposes it via the `onHello` subscribe
  callback. Poll pages keep their single top-level `coverage`.
- `evm-events` and `transaction-stats` public change/entity JSON now uses
  the same camelCase, `0x`-hex, decimal-string conventions as the other
  processors instead of raw serde output (snake_case keys, byte-array
  addresses, PascalCase finality). SDK gains `EvmEvent` and
  `TransactionStats` types.
- `emittedAt` is now RFC 3339 UTC with millisecond precision
  (`2026-08-06T14:05:51.194Z`) instead of the nonstandard `unix-ms:` prefix
  encoding.

### Added

- `leani subscribe blocks` and the built-in `block-summary` 1.1.0 processor,
  including latest/by-number native queries and typed SDK helpers.
- `leani subscribe uniswap-v3 MARKET...`, embedded/attached auto-detection,
  exact V3 price and base-token volume output, stable JSON/raw formats, and
  scoped cold-start reset commands.
- Processor-owned native query extensions with canonical instance-scoped
  routes, collision-safe optional aliases, bounded read-only contexts,
  capability discovery, a safe same-origin SDK request primitive, and a
  complete downstream custom-processor example.
- Independent Rust workspace and Bun-compatible TypeScript SDK with strict
  configuration, diagnostics, lifecycle, CI, MIT licensing, release
  artifacts, container/Compose/systemd deployment, and operations contracts.
- Source-neutral chain frames with explicit material completeness,
  capabilities, provenance, trust, verification, finality, checksummed
  durable encodings, and interchangeable source interfaces.
- Direct bounded Xatu Parquet projection, checksummed local normalized
  archives, and sparse EraE history reads with local execution-commitment
  verification and no full archive retention.
- Persistent Reth execution P2P ingestion, verified Beacon API finality, and
  a self-contained consensus-P2P light client with embedded Mainnet bootnodes.
- Bounded restartable cold runtime, capability/trust selection, source
  failover from durable coverage, shared live fanout, canonical reorg
  undo/apply, finality advancement, and deterministic recovery.
- SQLite WAL state, entities/indexes, undo, coverage, change streams, outbox,
  jobs, recent canonical frames, pruning, attribution, integrity checks,
  backup, restore, and compaction.
- blobs.money, ERC-20 watchlist, and Uniswap V2/V3 processors with typed
  coverage-aware API queries and resumable SSE changes.
- Exact recent and bounded on-demand EVM-free Ethereum block, transaction,
  receipt, and log RPC methods, plus post-commit WebSocket `newHeads` and log
  subscriptions with removed-log reorg delivery.
- Final EIP-7910 `eth_config` backed by exact checked Mainnet activation
  timestamps, execution blocks, fork hashes, precompiles, system contracts,
  and blob parameters.
- Cross-source frame/processor conformance and exact RPC differential tools,
  benchmark reports, readiness, Prometheus metrics, runbooks, and threat
  model.
- One process-wide Reth execution network with a stable node identity,
  bounded authoritative peer metadata, Discv5/inbound configuration,
  peer-capacity-aware request gating, adaptive material batches, incremental
  backfill progress, and durable canonical live resume.
- Late interchangeable-archive reconciliation of P2P-derived processor output
  using durable per-block delta checksums, without extending raw-frame
  retention.

### Fixed

- `publish = "finalized_only"` is now enforced. Included-block changes are held
  in the store until verified finality promotes them, their bytes count against
  the delivery budget from admission, undo records are never published, and the
  change stream carries only finalized `apply` records and `system.finality`
  markers. `leani subscribe --finality finalized` now honors that promise in
  both embedded and attached mode; attached subscriptions against an
  `included_and_finalized` node print each block once a finality marker covers
  it. Previously every finalized-only processor published included apply and
  undo records, and the attached CLI never printed anything under
  `--finality finalized`. `query-and-follow` now rejects finalized-only
  processors with `finalized_only_snapshot`, because retained state includes
  blocks whose changes are still unpublished and the stream never emits an
  undo for them.
- Correct processor schema OpenAPI string types, reject unsafe processor
  instance route segments, range-scope blob block cursors, and paginate blob
  transaction lists without silent truncation.
- Derive dashboard processor lag from the observed execution peer head rather
  than an in-flight next-block probe, and expose both values independently.
- Clear live readiness before a material-failure reconnect waits for peers.
- Keep Xatu beacon `block_total_bytes` separate from Ethereum execution-block
  RLP size. Derive the latter exactly from fork-aware header, transaction, and
  withdrawal inputs and fail closed on unknown future formats.
- Replace approximate blobs.money BPO block boundaries with the exact BPO1
  block `23,975,778` and BPO2 block `24,179,383`.
- Omit the obsolete `totalDifficulty` extension from canonical post-Merge RPC
  blocks.
