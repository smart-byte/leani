# Leani changelog

All notable changes are documented here. The format follows Keep a Changelog,
and releases use semantic versioning for native API, SDK, processor, durable
record, and documented RPC contracts.

## [Unreleased]

### Added

- `sources.live.stall_timeout_seconds`, ten minutes by default and six
  minutes to 30 days. When the live lane stays not ready this long while
  verified finality is ready, within one run of the network lanes, the node
  exits with status 1 so that systemd or Docker restarts it: a lane restart
  cannot heal the process-wide execution P2P network the lane stalled in. A
  wait at the recent-frame hard limit does not count, and the log says why at
  error level. The exit waits for the stalled lanes no longer than a shutdown
  does, even when the live lane is stuck on a network build.
- `leani backfill` logs its progress every 30 seconds: committed blocks,
  blocks per minute and, over P2P, connected peers. A run waiting on peers, or
  on a network that cannot start, no longer looks the same as a working one.

### Fixed

- The live lane follows the head again after the zero-peer watchdog rebuilds
  the execution P2P network manager with fixed discovery ports, such as the
  recommended Discv4 30303 and Discv5 30304. The session the lane kept while
  it reconnected held the ended manager's Discv5 on its port, so the rebuild
  failed, and the failed rebuild left a new Discv4 bound to 30303. Every later
  rebuild then failed, without a log line, and the lane stayed not ready until
  the node was restarted by hand. The ended manager now releases its handle
  from every session, a busy Discv5 port fails the attempt before anything is
  bound, and a build runs to completion even when its caller is cancelled.
  Each build and each failed reconnection attempt is logged.
- A restart during an automatic backfill could silently erase part of an
  ordered-state processor's history. A live drain that ran while the backfill
  committed a block applied that block as `included`, because the backfill
  kept its delta pending before the apply. Its unfinalized undo record, below
  the retained canonical blocks, then read as a reverted block at the next
  start, and startup reconciliation undid it under thousands of later blocks:
  their contributions to the keys the block wrote were gone, while coverage
  still reported the range complete. History now applies without keeping a
  pending delta, and reconciliation undoes an ordered lane only from its tip.
  A store that already holds such a block keeps it, with a warning, until
  finality promotes it.
- A history source that cannot plan a job's request no longer fails the job
  and parks the processor's live lane with `hot_cold_handoff_failed`. Xatu,
  for example, advertises receipts but serves them only for blob
  transactions, so a recipient-filtered processor that reads receipts failed
  its automatic backfill on a node with Xatu configured. The job now warns
  once and moves to the next configured source for the rest of its range.
  Archive reconciliation, which revisits the range after the handoff, moves
  past such a source the same way instead of stopping every network lane. The
  node also logs why it leaves a configured history source out of a
  processor's jobs.
- History P2P connection retries log at `warn` with their error instead of
  `debug`, so a backfill whose Reth networking cannot start, such as in a
  sandbox that forbids sockets, no longer waits silently.
- The execution P2P pool warns when it stays below
  `sources.live.body_serving_peer_target` connected peers for
  `peer_recovery_timeout_seconds`, again at that interval while it does.
  Requests spread across connected peers, so such a pool also caps backfill
  throughput.
- Coverage reports `catching_up`, not `live`, while a processor is still
  behind the chain's finalized head, even when its range has no gaps and the
  node is ready.
- Each start cancels the automatic backfill jobs earlier starts left queued or
  running for the same processor instance. They were listed as running
  forever, one more after every restart. The runbook says when a restart
  resumes an ordered lane paused with `hot_cold_handoff_failed`.
- A history job no longer fails with "block … was already applied with a
  different delta" when the live lane applies one of its blocks while it runs.
  A processor that records finality in its output, such as `blobs-money`, maps
  that block as `included` live and as `finalized` from history. Subscription
  and materialization microbatches compared the two checksums exactly and
  failed the job, where single-block commits already accepted a finality
  variant. Microbatches now hand every already-applied block to the
  single-block path, which accepts a finality variant and still rejects other
  content, also for a fill-missing subscription that replays the block.

## [0.1.0-rc.2] - 2026-10-01

### Upgrading from 0.1.0-rc.1

Take these steps in order; each links to the entries below that detail it.

1. Stop the node and every `leani subscribe` run on the data directory, then
   copy `data_dir`. `leani db backup` with the rc.1 binary copies only the
   node store, not the `subscriptions/` stores. Verify a backup with the rc.1
   binary: opening it with the new one upgrades the copy. The first start
   upgrades the store from schema 21 to 23, which cannot be undone
   ([schema](#rc1-schema)).
2. Fix the configuration:
   - a relative `data_dir` or archive `manifest` now resolves beside the
     configuration file ([paths](#rc1-paths));
   - fix or remove the options validation now refuses
     ([ignored options](#rc1-refused-options),
     [state modes](#rc1-state-modes), [finality](#rc1-finality-options),
     [eraE](#rc1-erae-http));
   - set `api.bearer_token_env` for a native API bound beyond loopback
     ([binds](#rc1-binds));
   - list the names and browser origins your clients use in
     `api.allowed_hosts`, `api.allowed_origins`, and `rpc.allowed_origins`
     ([browsers](#rc1-browsers));
   - opt JSON-RPC bound beyond loopback in with
     `rpc.allow_unauthenticated_remote` ([binds](#rc1-binds));
   - no persisted finality anchor exists yet, so the first start needs a
     `finality.checkpoint` at most 14 days old ([anchor](#rc1-finality-anchor)).
3. Rebuild each changed processor under a new `instance`:
   `erc20-balances` and `evm-events` at 1.1.0 ([rebuild](#rc1-rebuild)),
   keyed `evm-events` stored block-local ([keyed](#rc1-keyed-evm-events)),
   `blobs-money` at 1.5.0 ([blobs](#rc1-blobs-money)), `block-summary`
   at 1.2.0 ([blocks](#rc1-block-summary); compact `[blocks]` selects a new
   instance automatically, and `leani subscribe blocks` starts cold in a new
   state directory), and a compact
   `[uniswap]` node or `leani subscribe uniswap-v3` with ETH/USDT or WBTC/ETH
   ([Uniswap](#rc1-uniswap)). The container profile brings its renamed
   instance ([container](#rc1-container)), and the `windowed` profile its new
   output ([windowed](#rc1-windowed)). A refused start leaves the store
   unchanged.
4. Upgrade the SDK with the node, by exact version:
   `bun add @smart-byte/leani-sdk@0.1.0-rc.2` ([SDK](#rc1-sdk)).
5. Update HTTP and JSON-RPC clients that do not use the SDK: a JSON body or
   `x-leani-request: 1` on mutations, POST to create query snapshots
   ([HTTP](#rc1-http-clients)), consumer credentials and acknowledgements
   ([consumers](#rc1-consumers)), hex hashes ([hashes](#rc1-hex-hashes)), and
   the JSON-RPC changes ([JSON-RPC](#rc1-json-rpc)).
6. After the first start, expect consumer sessions to reconnect with new
   tokens ([consumers](#rc1-consumers)), and run `leani db compact` once
   while the node is stopped ([compact](#rc1-compact)).

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
  alike carry their index, such as `finality.endpoints[1]`. The part of an
  error body they keep, which a server may echo the request's path and query
  into, shows each segment of the endpoint's path, its query string and
  values, and its userinfo as `…`.
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
  and asks another peer for the header. Once a wave has found no peer serving
  the material, the peer that served the header takes a strike; a minute that
  runs out during the first wave ends the request as a timeout, without one.
  A struck peer is not asked for headers for a cooldown that grows with each
  strike, up to 30 seconds, and ranks after other peers until a block whose
  header it served completes. A header it serves in the meantime lifts
  neither, and nothing is banned or stored. A live header that a frame
  follows also earns its peer's stored service evidence and Reth reputation
  only once that frame completes, with its body and receipts when the frame
  needs them; a header that no frame follows, such as the minimum live head
  the lane starts from, earns them at once, but only when anchored: its hash
  known from verified finality or an attested head, such as the verified
  tip. The block after the finalized anchor, fetched by number, earns
  nothing. The lane used to retry forever while still reporting itself
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
- <a id="rc1-browsers"></a>Web pages can no longer read from or change a
  node's native API or JSON-RPC through the visitor's browser, with the
  exceptions below. The
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
- JSON-RPC WebSocket subscriptions bound their output too. The notifications
  one block or reorg produces for one connection may take at most 16 MiB
  (`rpc.max_subscription_event_bytes`), and a connection holds at most as
  much unsent; past either, the node closes that connection (see Changed).
  Notifications are queued one at a time, and a log or head matching several
  of a connection's subscriptions is serialized once for all of them. With
  responses capped at 1 KiB and `eth_getLogs` at one log, 128 subscriptions
  used to turn one block with two 1 KiB logs into 256 notifications of
  647,648 bytes, all built before the first was sent, and a connection that
  stopped reading held its slot, and a block's notifications, for good.
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
- EraE history no longer trusts more of its mirror than it must. A plain
  `http` mirror is accepted only on loopback, or with the new
  `sources.history[].allow_insecure_http = true`; redirects are not followed.
  The checksum catalog is read with a 16 MiB cap as it streams in, and a
  range response with a cap at the requested length. Each batch's byte
  ranges are checked against the remaining input budget before they are
  requested, an e2store record may not declare more bytes than its indexed
  extent holds, and a block's compressed header, body, and receipts are
  decompressed through one shared cap, the frame budget, at most 32 MiB. Each
  normalized frame is checked against the frame budget before it is queued,
  as the other sources check theirs; EraE never checked it. Errors, logs, and
  frame provenance show the mirror as `scheme://host[:port]/…/<file>`, so
  credentials in the mirror URL's userinfo, query, or path are not printed.
- Xatu history reads are pinned to the object version seen at HEAD: every
  range request carries `If-Match` with the object's ETag, so an object
  rewritten mid-read fails the chunk, which is retried, instead of decoding
  one version's pages with another's footer. `If-Match` never matches a weak
  ETag, so an object without a strong ETag is refused as a schema drift, on
  which a backfill tries its next configured source, and `leani source probe
  xatu` flags it. A range response of another length is refused too, and a
  Parquet footer whose metadata range exceeds 64 MiB is refused before it is
  requested.
- Every byte a Xatu read requests, its Parquet footers and the gaps merged
  between column chunks included, counts against the source budget's
  `max_input_bytes` for the chunk, and is charged before it is requested;
  ranges are merged across a gap only while the budget covers it. Only the
  selected column chunks counted, after the footer was read, so a 1 KiB
  budget let one merged request download 128 KiB. A chunk's `fetched_bytes`
  reports the requested bytes; it reported the selected ones.
- A local archive refuses an object path that crosses a symbolic link below
  the manifest's directory, and checks an object's size before reading it.
- Breaking (SDK): both SDK clients refuse redirects instead of following them,
  which could send the bearer token, a consumer credential, or a session token
  to another origin. A 3xx answer rejects with a non-retryable `LeaniError`
  with code `redirect_refused`, its status, and the `Location` in its message
  and in `details.location`; so does an answer that a custom `fetch` reached
  by following a redirect. `subscribe()` throws it at once instead of
  retrying. Configure the final URL as `baseUrl`.
- Breaking (SDK): request paths stay under the base URL's path. A path that
  starts with `/`, such as a capabilities `basePath`, is joined under a
  prefixed `baseUrl` instead of addressing the origin's root. A path segment
  that decodes to `.` or `..`, or that holds a backslash, an encoded slash or
  backslash, or an encoded percent sign, is refused with a `TypeError` before
  any request, and so is a control character anywhere in the path, raw or
  percent-encoded, which a proxy may drop after decoding. An ID such
  as `..` or `../../other` resolved to another route, directly or through a
  proxy that decodes `%2F` first. No Leani ID contains these characters.
  `request()` applies the same rule to a custom query extension's path, so a
  path parameter whose value can hold `/` or `%`, sent as `%2F` or `%25`,
  must move to the query string, `request()`'s `params`.
- Publication runs in protected GitHub environments: `release-npm`,
  `release-crates`, `release-ghcr`, `release-github`, and `site-production`.
  Build and test jobs run with a read-only token and no secrets or OIDC
  token, and upload what they tested; the publish job downloads it and only
  publishes. Only publish jobs may request write permissions,
  `id-token: write` included, or read secrets, and no checkout keeps its
  credentials. The npm and crates.io jobs used to test and publish in one job
  holding an OIDC token. The crates.io job packages the tested commit again
  without compiling and publishes with `cargo publish --no-verify` only if it
  produced exactly the tested archives, byte for byte. No other publish job
  checks out the repository, npm publishes the local archive file, and no
  workflow runs on `pull_request_target` or `workflow_run`. Site promotion
  builds with a read-only token and moves `site-production` from a separate
  job. `scripts/check-release-ci.rb --workflows` enforces this layout in CI,
  including for npm run by its path, and accepts as the published archive
  only an explicit path or a plain file name: npm reads `user/repo#ref` as a
  repository and `@scope/name` as a package.
  Maintainers must create the environments, move or delete the repository
  secrets `NPM_TOKEN` and `CARGO_REGISTRY_TOKEN`, and add the rulesets listed
  in the release process before the next publication.
- Release archives, the multiarchitecture container image, and each
  architecture's image carry signed build provenance, and each image's
  CycloneDX SBOM is attested to its digest. A release candidate without the
  node's SBOM, `leani.cdx.json`, is not published. The container publication
  summary lists the image digests. The Dockerfile pins its base images by
  digest, and CI requires its Rust image to match `rust-toolchain.toml`.
  Dependabot proposes weekly updates for GitHub Actions, Cargo, Bun, and
  those images, and a weekly workflow runs the cargo-deny advisory check and
  `bun audit`.

### Added

- An [Agent Skills](https://agentskills.io) skill in `skills/leani`, installable
  in Claude Code with `/plugin marketplace add smart-byte/leani`. It teaches
  coding agents the CLI's trust, finality, and exit-code rules, and how to
  configure built-in processors such as `evm-events`. See
  [Use Leani with AI agents](docs/guides/ai-agents.md).

### Changed

- CLI changes for scripts and coding agents:
  - `leani doctor` exits 3, not 2, for an unreadable or invalid
    configuration; 2 stays clap's usage error. `doctor --json` prints a
    report with `valid: false` for an unreadable file too.
  - `leani subscribe --once` stopped by a signal before a match exits 130,
    not 0. New `--timeout DURATION` (`90s`, `5m`, or bare seconds) bounds a
    subscription; an unsatisfied `--once` then exits 124.
  - `leani subscribe --once` no longer ends on an unverified peer `preview`:
    it hides previews and waits for the first row that meets `--finality`,
    by default `included`. The new `--finality preview` restores the fast
    path; previews still show while following without `--once`. `--json` is shorthand for `--format json`.
  - `leani backfill` takes `--from-block`/`--to-block` (the old `--from`/`--to`
    still work), and `--processor` defaults to the configuration's only
    processor instead of `blobs-money`.
  - `benchmark`, `e2e`, and `conformance` no longer appear in `leani --help`;
    they still run. `init`, `subscribe`, and `backfill` help shows examples.

- <a id="rc1-http-clients"></a>Breaking for HTTP clients of the native API
  that do not use the SDK:
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
- <a id="rc1-binds"></a>Breaking (configuration): a native API bind beyond
  loopback needs `api.bearer_token_env` or
  `api.allow_unauthenticated_remote = true`, and a
  JSON-RPC HTTP or WebSocket bind beyond loopback needs
  `rpc.allow_unauthenticated_remote = true`. A configured bearer token shorter
  than 16 characters, or with spaces or non-ASCII characters, fails startup.
  `deploy/container.toml` now reads its bearer from `LEANI_API_TOKEN`, opts in
  to remote JSON-RPC, and lists the compose service name `leani` in
  `api.allowed_hosts`. `compose.yaml` passes the token through and stops
  every command, including the benchmark database's, while it is unset.
- <a id="rc1-json-rpc"></a>Breaking (JSON-RPC): calls need
  `content-type: application/json` (415 otherwise), browser origins other
  than loopback need `rpc.allowed_origins` (403 otherwise), and the limits
  above apply: a longer batch gets one `-32600` error, and an over-limit
  `eth_getLogs` fails with `-32005` "query returned more than 10000 results.
  Try with this block range [...]". The `rpc.max_*` settings change each
  limit. A single block
  with more matching logs than the limit cannot be read with `eth_getLogs`;
  raise `rpc.max_log_results` or use `eth_getBlockReceipts`.
- Breaking (JSON-RPC): block lookups answer `null` only for a block number
  above the node's head: its newest canonical block, or the block its
  progress processor has reached when that is higher. A block at or below the
  head that neither the recent window nor on-demand history serves now fails
  with `-32004` (`block_not_retained`, or the history source's reason)
  instead of `null`, and so does a block hash that the recent window lacks
  when no history block-hash lookup is configured, and `latest` on a node
  that knows no canonical head. This covers `eth_getBlockByNumber`,
  `eth_getBlockByHash`, `eth_getBlockReceipts`,
  `eth_getBlockTransactionCountBy*`, and `eth_getTransactionByBlock*AndIndex`.
  Ethereum clients read `null` as "no such block", so a reorg checker
  concluded that a block below the retained window had been reorged out.
  Clients that took `null` for "not available here" must handle `-32004`. A
  block number above the head that no history source covers now reads as
  `null` instead of failing with `no_viable_historical_source`.
- Breaking (JSON-RPC): `eth_blockNumber`, `latest` and
  `eth_syncing.currentBlock` report the canonical tip, or the highest block
  the progress processor has finalized when that is higher or no tip is
  known, as on the shipped historical-only profiles. They no longer report
  an unfinalized processor cursor. A node that knows no block reports `0x0`,
  and `eth_syncing` keeps `startingBlock` at or below `currentBlock`.
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
    Until the node has verified finality, `finalized` and `safe` get `-32004`
    (`finalized_block_unavailable`). `pending` gets `-32004`
    (`pending_block_unavailable`): Leani builds no pending block.
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
- Breaking (JSON-RPC): the node closes a WebSocket connection whose
  subscriptions produce more than `rpc.max_subscription_event_bytes` (16 MiB)
  of notifications for one block or reorg, with code 1008 and none of that
  event's notifications, and one that leaves that much unread, with code
  1013. The close reason names the setting, and notifications still unsent
  are dropped. Reconnect and subscribe again; after a 1008, with fewer or
  narrower subscriptions per connection. Other connections are unaffected.
- SDK: `processors.queryEntities()` without a cursor creates its snapshot with
  the new POST route, and mutations without a JSON body send
  `x-leani-request: 1`. Update the SDK together with the node.
- <a id="rc1-consumers"></a>Breaking for durable consumers of the native API:
  - A consumer created with a credential needs `x-leani-consumer-credential`
    on each of its lease, change, acknowledgement, and stream requests (403
    otherwise). A new credential needs 32 to 512 printable ASCII characters
    without spaces (400 otherwise), such as `openssl rand -hex 32`. Existing
    credentials keep working, and a shorter one still gets 409
    `consumer_exists` when its consumer is created again, or its
    subscription's status when an unchanged backfill request is submitted
    again. A credential stored before the upgrade that begins or ends with
    whitespace cannot be sent as it is, since HTTP trims header values, and
    one with a control character other than a tab cannot be sent at all; now
    that the header is mandatory, revoke such a consumer and create it again
    with a new credential.
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
- Breaking (SDK): a request that fails below HTTP rejects with the new
  `TransportError`: a `LeaniError` with `status` 0, `code: "transport"`,
  `retryable: true`, and the runtime's error as `cause`. That covers a refused
  or reset connection, a response body that breaks off, a passed deadline,
  and a silent stream. Both clients rejected with the runtime's own error, a
  `TypeError`, an `Error`, or a `TimeoutError`, and the delivery lanes
  recognized few of them by their message: Bun's refused connections and
  socket resets, and Node's `terminated`, ended a lane instead of
  reconnecting it. The lanes now reconnect after any retryable `LeaniError`.
  An abort of the caller's own signal is passed through unchanged, and never
  retried.
- Breaking (SDK): `subscribe()` reconnects after an `error` event marked
  `retryable`, from the last yielded cursor and with the reconnect backoff;
  it threw every error event. After five retryable error events in a row
  without a new change, it throws the last one. It also reconnects when the
  stream stays silent, the node's 15-second keep-alive comments included, for
  the new `idleTimeoutMs` option, 60 seconds by default.
- Breaking (SDK): a delivery session from `stream()`, `streamLive()`, or
  `subscribe()` fails with a `TransportError` when its stream sends nothing,
  heartbeats included, for three `heartbeatIntervalMs` while the iterator
  waits, or sends no hello within `timeoutMs`. A lease renewal that fails
  with a retryable error is retried with backoff while the lease holds. One
  that fails otherwise, or keeps failing until the lease would lapse, ends the
  session at once, and the iterator throws its error; the first failure used
  to go unnoticed until the next record arrived. Lease renewals,
  acknowledgements, lease releases, and `changes()` observe `timeoutMs` too.
  The lanes of `subscribe()` reconnect after each of these. A stream record
  over 65 MiB fails the session with a non-retryable `invalid_response` error.
  A backfill stream can request smaller batches with
  `batching.maximumEncodedBytes`; `streamLive()` takes no batching option, so
  a live stream needs the node's
  `api.delivery.live_batches.maximum_encoded_bytes` at 64 MiB or below.
- SDK: the package requires Node 20.3 or later, the first with
  `AbortSignal.any`, adds a `default` export condition beside `import`, and
  embeds the TypeScript sources in its source maps.
- <a id="rc1-sdk"></a>Breaking (SDK): the TypeScript SDK is published on npm
  as `@smart-byte/leani-sdk`, with the `@smart-byte/leani-sdk/backfill` entry
  point. The `v0.1.0-rc.1` sources and documentation called it `@leani/sdk`,
  a name that was never published; import the new name. Install the SDK
  version that matches the node release by its exact version, as the README
  and the install guide now do: prereleases are published under npm's `next`
  tag, and `latest` stays on `0.1.0-rc.1`, which does not work with this
  node. CI checks that every documented install pins the SDK package's
  version.
- The PostgreSQL example keys applied changes and its stored cursor by the
  durable consumer's stream too. A node whose store is replaced numbers its
  changes from the start again, which the example took for changes it had
  already applied. Drop the example's two tables before running it again.
- `THIRD_PARTY_LICENSES.txt` is now committed, verified against `Cargo.lock`
  in CI, and linked from the README and site. It adds the Apache-2.0 NOTICE
  files of the Arrow, Parquet, object_store, and Moka dependencies, which the
  generated listing previously omitted. The Homebrew package now also installs
  Leani's own `LICENSE`, and the container image declares OCI license metadata.
- <a id="rc1-schema"></a>Node stores move to schema 23. Schema 22 adds
  coverage lookup indexes. Schema 23 adds a random per-store secret, drawn
  once from the operating system, for session tokens and consumer credential
  hashes; keys the stored credential hashes with it, so existing credentials
  keep working; and starts every consumer's delivered sequence at its stream
  head, so acknowledgements of changes delivered before the upgrade still
  pass. A schema-21 or -22 store upgrades in place on its next start; earlier
  binaries then refuse it, so keep a backup if you might roll back. To roll
  back, move `leani.sqlite` aside with its `-wal` and `-shm` files before
  restoring the backup: SQLite replays a leftover WAL onto the restored
  copy. A backup
  holds the secret. Before it upgrades an older store, a command that
  registers processors, `serve`, `backfill`, the runtime of
  `leani subscribe`, or `leani e2e mainnet --resume`, reads the store
  without writing and refuses a processor the store holds under another
  identity, as registration does (`processor instance … conflicts with its
  stored descriptor`), and a configured consumer whose stored role differs;
  the refused store stays at its schema, so the previous binary still opens
  it. `serve` also checks its bearer token and JSON-RPC history sources
  before it opens the store. `leani db backup` copies the store at its
  schema and never upgrades it, refuses a schema newer than the binary
  supports, and refuses a data directory without a store instead of
  creating one.
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
  metadata or when the ID and version together exceed 126 bytes: the key adds
  `@`, `:`, and the 64-digit configuration hash, and may hold 192 bytes.
  Decoding a descriptor without an `instance` reports that as an error.
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
- <a id="rc1-rebuild"></a>`erc20-balances` 1.1.0 and `evm-events` 1.1.0
  change stored output (see Fixed). Existing instances need a rebuild: set
  `version = "1.1.0"` and
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
- <a id="rc1-keyed-evm-events"></a>Breaking for keyed `evm-events` consumers
  and operators: `evm-events` 1.1.0 with `key_fields` is an ordered processor
  (see Fixed). It declares
  `ordered_state` reduction and `canonical` delivery ordering, like
  `erc20-balances`, and its live lane waits for its cold backfill, holding
  live blocks as pending deltas until history reaches them. Without
  `key_fields` it stays block-local, with `block_versioned_idempotent`
  delivery. Consumers of a keyed instance read its canonical stream through
  the generic consumer routes, not the session-fenced `/streams/live/…`
  routes. Application subscriptions and
  `POST /admin/v1/materialization-jobs` refuse a keyed instance, and its
  on-demand `backfill` ranges apply contiguously from `start_block` upward.
  Finalized-coverage compaction is block-local only, so a keyed instance keeps
  one coverage row per block it applies, without bound. The ordering is part
  of the stored processor descriptor, not of the instance ID or the
  configuration and code hashes, so the node refuses a keyed instance that a
  store registered block-local (`processor instance … conflicts with its
  stored descriptor`); rebuild it under a new `instance` as above.
- Breaking: `leani backfill` of an ordered processor, such as
  `erc20-balances`, `transaction-stats`, `uniswap-latest`, or keyed
  `evm-events`, must start at the block after the last one the processor
  applied, or at its `start_block` when it has applied none, and refuses any
  other start with the block to use. The store moves an ordered processor's
  cursor to every block it applies, so a range with a hole below it, or below
  blocks already applied, reduced its history out of chain order. Rerun an
  interrupted backfill from the block the refusal names.
- <a id="rc1-blobs-money"></a>`blobs-money` 1.5.0 changes stored output from
  Fusaka on (see Fixed). Rebuild existing instances the same way: set
  `version = "1.5.0"` and configure a new `instance`, which indexes from its
  `start_block`; the node refuses 1.5.0 under a 1.4.0 instance ID. Entity,
  change, and delta schemas are unchanged.
- <a id="rc1-container"></a>Breaking (container): `deploy/container.toml`
  runs `blobs-money` 1.5.0 as the new instance `blobs-container-1-5`, so
  clients that name it by instance, in API routes or stream IDs, use the new
  name, and its data is indexed afresh. The store refuses 1.5.0 under rc.1's
  `blobs-container` instance, which would keep the container from starting on
  an rc.1 volume. On an rc.1 volume, the new image starts beside the earlier
  instance, whose rows stay inert and count toward
  `[budgets.store] maximum_physical_bytes`. For a clean slate, run
  `docker compose down -v` before `docker compose up --build`, which deletes
  the volume, or restore a snapshot of it. The profile also caps
  `[budgets.history_pipeline] maximum_active_chunks` at 2: a Xatu backfill
  holds about 450 MiB per active chunk, and four would outgrow its 1 GiB
  memory budget and the compose file's 2 GiB container limit. The container
  profile's processor now uses on-demand history, so the documented bounded
  backfill works: with automatic history and live following disabled,
  nothing indexed it, and `leani backfill` refused it
  (`automatic_job_owns_history`). History mode is not part of the
  processor's identity, so the instance is unchanged. The runbook gives the
  compose commands, which stop the service first because it holds the data
  directory's lock.
- <a id="rc1-windowed"></a>Breaking (profiles): the `windowed` profile's
  `uniswap-v2-sync-30d` keeps every Uniswap V2 `Sync` event, stamped with its
  hour, in `uniswap_v2.sync`, instead of the last one per hour in
  `uniswap_v2.sync_hourly`. Keyed `evm-events` output is ordered since 1.1.0
  ([keyed](#rc1-keyed-evm-events)), which would cost the profile its bounded
  window: materialization jobs, reruns of the quickstart backfill and
  coverage compaction need block-local output, which keyless output keeps. A store that holds the
  profile's `evm-events` 1.0.0 instance from 0.1.0-rc.1 refuses the new one,
  as for any instance above: rebuild it under a new `instance` or in a new
  `data_dir`.
- ERC-20 balances gain `incompleteFrom` (`null`, or the block from which the
  token/holder pair is no longer derived) in the typed query, the change JSON,
  the OpenAPI `Erc20Balance` schema, and the SDK `Erc20Balance` type.
- <a id="rc1-state-modes"></a>Breaking (`leani-processor-api`):
  `LifecyclePolicies::validate` rejects `StatePolicyMode::Ephemeral` for
  every publication policy, and
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
- <a id="rc1-uniswap"></a>The built-in `ETH/USDT` and `WBTC/ETH` Uniswap V3
  markets start at the V3 factory deployment block 12,369,621 instead of
  12,376,729, the creation block of the USDC/WETH 0.05% pool, which both
  pools may predate. No V3 pool predates the factory, so this is a safe lower
  bound. `ETH/USDC` keeps its exact creation block, so an ETH/USDC-only
  starter is unchanged.

  Affected are compact `[uniswap]` nodes and `leani subscribe uniswap-v3`
  subscriptions whose markets include `ETH/USDT` or `WBTC/ETH`. Their data
  from earlier releases may miss those pools' events between pool creation
  and block 12,376,729, and the new start block changes the processor's
  identity, so the node refuses the processor its store holds:
  `processor instance uniswap-observations conflicts with its stored
  descriptor` (`cli-uniswap-v3-prices` for a subscription). The node prints
  the routes for that refusal before the message. In order of preference:

  1. Start the node with a new `data_dir`. Nothing is deleted, and the
     refusal leaves the old store at its schema, so the old directory stays
     usable with the previous binary as a rollback.
  2. Replace the compact document with an advanced configuration whose
     `uniswap-observations` `[[processors]]` entry, like the example in
     `docs/reference/processor-contracts.md`, names a new `instance`, lists
     your markets' pools (the Uniswap guide lists the built-in ones), and
     starts at block 12,369,621, or at 12,376,729 for ETH/USDC alone. The node
     keeps its raw history, P2P identity, and embedded subscriptions; the old
     instance's rows stay in the store.
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
- <a id="rc1-finality-options"></a>Configuration validation rejects
  `finality.minimum_peers` above 24, the consensus P2P peer set; an all-zero
  `finality.checkpoint`; and
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
  than the published one, and ends the refresh once it has asked every
  connected peer, instead of waiting out the 15-second request timeout for
  another to connect: two such waits outlasted the four-slot grace and
  reported the lane disconnected.
- Breaking (CLI): an attached `leani subscribe` prints nothing until each
  stream connection's `hello` shows an Ethereum mainnet node (chain 1)
  serving the feed's built-in processor, `block-summary` or
  `uniswap-observations`, under the requested `--processor`. A node for
  another chain or processor, a stream without a hello, or anything else that
  answers `/health/live` at the configured `api.bind` now ends the command
  with an error. Auto mode used to attach to any such listener and print its
  stream. The startup snapshot also waits for the first hello.
- Breaking (CLI): `leani reset subscription` resets only a directory marked
  as embedded subscription state (`.leani-subscription`). `leani subscribe`
  marks each directory it creates, and marks a subscription's default
  directory from an earlier release when it next runs, before the store
  opens. A node command that locks a marked directory (`serve`, `backfill`,
  `db`, and the e2e runs) removes the marker, since the directory is then
  node state, and the reset checks the marker again once it holds the lock.
  An explicit `--data-dir` from an earlier release that already holds state
  is never marked, because it could be a node's data directory: delete it
  yourself once it holds only subscription state. `leani reset all` now
  takes the lock of every embedded subscription under `subscriptions/` first
  and refuses, deleting nothing, while one runs; a subscription that starts
  during the reset keeps its directory, and a subscription directory it
  resets stays marked.
- Breaking (CLI): `leani subscribe` reports two new row kinds on stdout, so
  scripts should dispatch on each JSON row's `schema`. An undo of a change
  this run never saw, because it was applied before the run started or aged
  out of the last 4,096 changes it printed, prints while fresh as a
  `leani.subscription-undo.v1` row (`--format json`) naming the reverted
  block and change key. When an attached `--finality finalized` subscription
  holds 1,024 unfinalized blocks without a finality marker, it drops the
  oldest; finalized output has a hole there, reported as one
  `leani.subscription-gap.v1` row (JSON and raw) or `gap` line per run of
  dropped blocks, `fromBlock` to `toBlock`, right before the next finalized
  row. A blocks feed reports the same way, with `reason: change_log_pruned`,
  blocks not yet final whose changes the node pruned before the subscription
  connected. `--once` ends only after an update, never after an undo or gap
  row.
- `serve` shuts down within 15 seconds of Ctrl-C or SIGTERM. Open change
  streams (SSE) and consumer delivery streams (NDJSON) end after a whole
  event or record, and JSON-RPC WebSocket connections get a `1001` going-away
  close frame; clients reconnect from their last cursor as after any closed
  connection. Connections and background work still open after 10 seconds,
  and blocking work such as a file write still running 5 seconds later, are
  abandoned, and the node exits 0. A second signal during the first 10
  seconds exits at once with status 130. `compose.yaml` now allows the
  container 30 seconds to stop, as `deploy/leani.service` does, instead of
  Docker's default 10.
- Breaking (operators): a background task of `serve` that stops before the
  shutdown shuts the node down with an error and exit status 1, naming the
  task: the durable backfill and raw-history schedulers, the network lane
  supervisor, artifact compaction, query snapshot cleanup, and each
  processor's delivery pruning and coverage compaction. A panic there used to
  end the task silently while the node kept running and, for the network
  lanes, could keep reporting ready. A process manager restarts the node.
  This includes a persistent execution P2P source that cannot be built, such
  as for an invalid NAT setting, which used to leave the node up but never
  ready. Failed network-lane runs still restart in the node, and halted lanes
  still stay stopped while the node serves.
- After a network-lane run that stayed up for at least a minute, the next
  restart waits one second again; the backoff kept doubling up to a minute
  for the rest of the process. Finality contradictions still halt on their
  third occurrence for one finalized block, however long the runs between
  them.
- Automatic cold backfill jobs are named
  `automatic:{chain}:{instance}:{start}-{finalized block}`, hot/cold handoffs
  `handoff:{chain}:{instance}:{from}-{finalized block}`, and archive
  reconciliations `archive-reconciliation:{instance}:{source}:{from}-{to}`,
  after the processor instance instead of its kind. Records under the
  earlier names stay as the history of earlier starts; nothing reads them
  again, because every start already created new records and resumes from
  processor coverage, and the first handoff of an instance marks its
  unverified earlier one failed as superseded. After the upgrade, an
  automatic backfill job that was interrupted under its earlier name stays
  `running` in the job list, at most one per instance, and an archive
  reconciliation that failed under its earlier name is compared once more
  under its new one.
- The durable backfill scheduler looks at its jobs at startup, when a job is
  created, and when a job's task ends, instead of reloading and decoding
  every job every second. Jobs waiting for storage headroom, and jobs that
  failed to resume for a store error, are looked at again every 5 seconds.
- The OpenAPI document gives `202` as the success status of
  `createBackfillSubscription`, `createMaterializationJob`, and
  `createRawHistoryJob`, as the node answers; it said `201`.
- <a id="rc1-hex-hashes"></a>Breaking (native API): recovery checkpoints and
  portable savepoints (`GET /v1/processors/{processor}/checkpoints`, and
  `GET` and `POST` `/v1/processors/{processor}/savepoints`) send `blockHash`
  and `stateChecksum` as `0x`-prefixed hexadecimal strings, like every other
  hash the API returns, and so does `details.currentFinalizedHead.hash` in the
  `409 history_not_finalized` and `range_after_finalized_head` refusals; each
  was an array of 32 integers. Clients that read the arrays must read hex.
  The OpenAPI `RecoveryCheckpoint` and `PortableSavepoint` schemas now use
  `Hash32`, the `ByteArray32` schema is gone, and the SDK's generated types
  follow.
- The OpenAPI document lists what the node already sent: a durable
  consumer's `streamId` and `leaseGeneration`, and `paused` among the
  coverage `state` values. The SDK's `DurableConsumer` type gains both
  fields, its `ProcessorCoverage` type lists `paused`, and its
  `ProcessorCoverage.requested` may be `null`, as the node sends it for a
  status without a requested range.
- <a id="rc1-erae-http"></a>Breaking (configuration): a plain `http` eraE
  `endpoint` off loopback needs `allow_insecure_http = true` on its history
  source, which is only valid for `era_e` sources.
  `leani source probe erae --endpoint` accepts
  `https`, `file`, and loopback `http` mirrors. A `xatu` history source on a
  chain other than Ethereum mainnet fails validation, and
  `XatuHistoryConfig::public`, `XatuCatalogConfig::public`, and
  `XatuCatalog::new` refuse any network but `mainnet`: Xatu projections stamp
  every frame as chain 1.
- EraE plans that need receipts or logs before Byzantium (block 4,370,000)
  fail with an error that says so. Earlier receipts carry an intermediate
  state root instead of a status, which frames cannot represent, so these
  plans used to succeed and then fail every chunk. Header and body requests
  from genesis still plan.
- EraE frames label their checks honestly: `header_hash` is not checked (the
  hash is computed from the archived header, and nothing anchors it to
  consensus), `withdrawals_root` is unavailable before Shanghai, and the
  object identity no longer records the catalog's SHA-256 as a checksum,
  since sparse reads never recompute it. The catalog digest stays the
  object's version.
- Local archive frames must carry the manifest's chain and finality, and
  report only the checks the archive made: the object's BLAKE3 digest, and
  the parent link to the previous frame of the same object. A frame's own
  verification claims and consensus anchor are dropped, and the trust of its
  earlier provenance is capped at the archive's `trusted_dataset`.
- Xatu blob frames declare their logs a partial projection. Their receipts
  carry no logs, so a log requirement can no longer be met through those
  receipts.
- Xatu decodes each projected column by a declared encoding that fixes the
  Arrow types it may arrive as. Hashes, addresses, and byte strings are
  `0x`-prefixed hexadecimal text, or raw bytes in a fixed-width binary column
  of exactly their size; text is no longer taken for raw bytes because of its
  length or its lack of a `0x` prefix. A column of another type fails the
  object before any row is decoded, as a schema drift; it failed as a
  corrupt frame at the first value decoded. A table missing a projected
  column still fails as a corrupt frame.
- Breaking (`leani-source-api`): `SourceBudget` gains `max_resident_bytes`,
  the bytes of raw input one open may hold in memory at once, beside
  `max_input_bytes`, what it may acquire over its life. `validate` rejects
  zero for it, like the other limits; construct budgets with the new field.
  Every source enforces it and names it `resident_bytes` when exceeded: a
  local archive for each line of its object, EraE for each batch of byte
  ranges before it fetches the batch, fetching fewer blocks at a time down
  to one, Xatu for the selected columns of each Parquet row group before it
  requests them, execution P2P for each window of frames it builds, and
  retained raw history for each stored record. The error's text names where
  to raise it: `budgets.memory_bytes` for a node, and `--max-input-bytes` for
  `leani source probe`.
- Each history source read may hold up to `budgets.memory_bytes` of raw input
  at once: backfill and CLI backfill chunks, archive reconciliation, the live
  lane, and raw-history job reads, and a historical RPC request up to
  `memory_bytes` divided by `budgets.source_concurrency`, as before. What a
  read may acquire over its life is unchanged: `budgets.temporary_disk_bytes`
  for backfill and reconciliation reads, `memory_bytes` for the live lane,
  and `raw_history.maximum_segment_logical_bytes` for raw-history job reads.
  The bound is
  per read: the node runs up to `budgets.history_pipeline.maximum_active_chunks`
  backfill reads, one reconciliation read, the live lane, and one read per
  running raw-history job at once, and decoded data has its own budgets, so
  plan RAM for their sum; the configuration guide has mainnet sizing. A read
  that would hold more fails with a `resident_bytes` budget error, and a
  backfill then tries its next configured source. Local archive reads verify
  the object's length and digest first, then stream its frames a line at a
  time from the same open file, hashing it again as they go; they held the
  whole object's requested frames until its digest verified.
- A local archive object whose frames skip a block of the requested range,
  repeat one, or stop short of its end now fails its read as corrupt
  (`CorruptFrame`), as frames out of order already did; a frame count that
  did not match the range failed as a missing range. A backfill reading such
  an object therefore fails instead of trying its next configured source,
  and archive reconciliation fails, which restarts the network lanes,
  instead of waiting for the archive to fill the range. Blocks that no
  manifest object covers are still a missing range.
  `docs/reference/archive-format.md` lists the checks.
- The retained raw-history store opens whatever state its segments are in. At
  startup it checks only that each catalogued segment file exists with its
  closed length, and it verifies a segment's whole-file checksum when it
  first reads it. A segment that fails either check leaves the catalog, so its
  blocks read as missing; its file moves to `raw-history/quarantine/`, and
  each raw-history job that owned it says so in `last_error`: a completed job
  returns to the queue and acquires the blocks again, and a failed or
  cancelled one keeps the error it ended with. A segment whose file is gone
  at startup leaves the catalog the same way, unless most files are gone, as
  when a volume is not mounted yet or a restore is still running: then every
  row stays, the node logs a warning, and the startup recovery report counts
  them as `unavailable_segments`. A file that disappears while the node runs
  fails its reads, which fall back to other sources, and keeps its row until
  the next start. One corrupt or missing segment refused the whole store, and
  so did a store over its budget, which now opens and admits no new segment
  until it is back under. The quarantine keeps at most 1 GiB, or the largest
  segment `raw_history.maximum_segment_physical_bytes` admits when that is
  larger, deleting the files quarantined longest ago first but never the
  newest; it was never collected. The node logs the recovery report at
  startup.
- Admission to the retained raw-history store reserves the catalog rows a
  segment adds: 64 KiB for its own rows and 512 bytes per locator row, one
  per block with block-hash locators and 256 per block with transaction-hash
  locators. A segment with more transactions still publishes its extra rows,
  so the catalog can exceed `maximum_physical_bytes` by one segment's extra
  rows, which the next admission counts. Only 64 KiB was accounted, and
  locators not at all, so a transaction-indexed store could outgrow its
  budget without limit. A job checks its next segment's admission before it
  opens a source, so a job paused at its storage limit no longer downloads
  that segment again each time the node resumes it, every five seconds.
- <a id="rc1-paths"></a>Breaking (configuration): relative paths in a
  configuration file, its `data_dir` and an archive source's `manifest`, are
  relative to the directory of that file, not to the working directory the
  node starts in. A node started with `--config /etc/leani/node.toml` and
  `data_dir = "./data"` now uses `/etc/leani/data`. State derived from
  `data_dir` moves with it: `leani subscribe` keeps its embedded state under
  `<data_dir>/subscriptions/<hash>`, and `leani reset` resolves the same
  directories, so a `leani --config elsewhere/… subscribe` starts cold after
  the upgrade. Before upgrading, make a relative `data_dir` absolute, or move
  the data directory beside the configuration file. If the former location
  still holds a node store (`leani.sqlite`) or embedded subscriptions and the
  directory beside the configuration holds none, loading the configuration
  refuses rather than start on an empty store. When both hold state, the node
  uses the one beside the configuration and warns once, naming both, and
  `leani doctor` reports it. Set an absolute `data_dir` to make the choice
  explicit. Equivalent paths and symlink aliases are accepted. A
  compact configuration's default `data_dir`, `./data`, is beside the file
  too. `leani init` writes `./data` and prepares that directory beside the
  configuration it creates, wherever `--config` puts it; its `--data-dir`,
  like any path flag, is relative to the working directory, and is written
  absolute when the configuration is elsewhere. Path flags such as
  `--config` and the `--data-dir` of other commands are unchanged.
- <a id="rc1-refused-options"></a>Breaking (configuration): validation
  refuses options the node ignored. `[processors.output] finalized_only` was
  never applied; remove it, and set `publish = "finalized_only"` to publish
  only finalized blocks, as the shipped profiles and examples that set it
  already did. A `sources.history`
  entry of `kind = "parquet"` was skipped, since no source reads it; use
  `xatu` for public Parquet history, `era_e`, or `archive`. The
  configuration schema no longer offers either. The node now always leaves
  the lifecycle's `OutputPolicy::finalized_only`, which nothing reads,
  false. An instance that set it, from a configuration file or through
  `leani subscribe --finality finalized`, keeps its state, and the store
  updates its stored descriptor in place. But what it wrote before the
  upgrade is bound to the old descriptor: its recovery checkpoints can no
  longer be restored, and its portable savepoints, which are export-only, can
  no longer be exported; both answer 409 `savepoint_invalid`. Backfill such
  an instance again, or use the checkpoints and savepoints it takes after
  the upgrade. For Rust embedders
  of the `leani` crate, `OutputPolicyConfig::finalized_only` is an
  `Option<bool>` that must be `None`.
- `rpc.transaction_locator` is optional, deprecated, and ignored: nothing
  ever read it. A configuration that sets it still loads, and the node warns
  once in its log and in `leani doctor`; remove it. The shipped
  configurations and the configuration reference no longer list it.
  `RpcConfig::transaction_locator` is an `Option<bool>`.
- Breaking (`leani-processor-api`): the new `MAXIMUM_CONSUMER_LEASE_TTL`
  (100 years) is the longest lease TTL of a durable consumer, and the store
  uses it. `LifecyclePolicies::validate` refuses a consumer lease TTL over it,
  as well as zero, with "delivery consumer lease TTLs must be non-zero and at
  most 100 years"; an invalid or duplicate consumer ID now gets "delivery
  consumers need unique portable IDs". `leani doctor` refuses such a
  `lease_ttl` with the message `serve` gives.
- Raw-history jobs refuse window retention and log indexes when they are
  created, with a 400 that names the replacement: `"retention": "full"`, and
  `"logs": false` in `indexes`, since logs are read from the retained
  receipts. The node refused both before, window retention without saying
  what to use instead, and log indexes as "not implemented".
- Benchmark reports are v20, and real-source reports v8. A summary of fewer
  than 10 measured runs reports their minimum, median, and maximum, with
  `statistics: "min_median_max"` and null p95 fields; the nearest-rank p95 of
  three runs was the slowest run's time and the fastest run's rate. From 10
  runs, `statistics` is `min_median_max_p95`, and both p95 fields describe
  the slow tail, interpolated between runs: the elapsed time 95% of runs
  stayed within, and the block rate 95% of runs reached. Medians of an even
  number of runs are the mean of the middle two. On Linux, peak RSS is the
  kernel's high-water mark, `VmHWM`, reset as each run's sampling starts;
  elsewhere it is the largest periodic sample, and `peakRssMeasurement`
  (`linux_vm_hwm` or `sampled_rss`) says which. It was the largest `VmRSS`
  sample, which misses spikes between samples.
- `leani benchmark real-source` holds its data directory, as a node does,
  before it opens anything there, the execution P2P peer store and identity
  of a `p2p-only` run included. It refuses a directory that a running node or
  another benchmark holds; it used both files without the lock.
- A production site build fails unless its documentation ref,
  `LEANI_DOCS_REF` or else the commit's exact release tag, is `v` followed by
  the workspace version. A stale Cloudflare `LEANI_DOCS_REF` used to publish
  links into an earlier release. Previews derive the ref from their branch;
  unset the preview `LEANI_DOCS_REF` in Cloudflare.
- `LeaniProcessorStalled` and `LeaniFinalityStalled` fire only on nodes that
  require the live source or finality (`leani_live_required` or
  `leani_finality_required` is 1). A historical-only node stops advancing once
  its backfill completes and no longer alerts. The rules join on the scrape
  target's `job` and `instance` labels.
- Linux release archives must not need a glibc newer than 2.39; their build
  checks the binaries' `GLIBC_` symbol versions. The install guide lists the
  supported distributions.

### Fixed

- `leani subscribe` stops gracefully on SIGTERM, as sent by `timeout` and
  agent harnesses, and persists its verified anchor instead of dying.
  Error chains no longer print a cause twice, as in `…(os error 2): No such
  file or directory (os error 2)`.

- Xatu LOG0 rows also accept the public dataset's empty hexadecimal topic
  sentinel (`0x`), including NUL-padded strings. A real 32-byte zero topic
  remains a topic, and malformed short hexadecimal values still fail.
- CLI backfill and node jobs share source assembly, history pipeline budgets,
  and the verified P2P bridge. Standalone backfill initializes finality and
  the peer pool when configured; `backfill --endpoint URL` submits through
  the authenticated node API and waits for completion. Source selection
  considers the bridge before refusing a processor with no static source.
- `eth_getLogs` accepts predicate-complete trusted Xatu and retained raw-log
  projections, while rejecting partial or narrower coverage. Canonical head
  reporting uses canonical metadata or finalized coverage, never an
  unfinalized processor cursor, and `eth_blockNumber` never fails for want
  of a head.
- WebSocket connections and listeners share a 64 MiB budget for queued
  notifications (`rpc.max_outbound_bytes`). When it runs out, the connection
  holding the most of it is closed with `1013`, not the next to notify;
  responses to calls never use it. A connection keeping more than a quarter
  of its notification room queued for 30 s is closed as a slow reader, and a
  send may take 30 s plus its payload at 64 KiB/s.
- Shipped configurations are validated with a standard JSON Schema validator,
  including types, bounds, patterns and exact `oneOf` semantics. The Serde
  field/enum parity probe remains a separate contract check.
- <a id="rc1-block-summary"></a>`block-summary` 1.2.0 accepts trusted dataset projections with full gas
  fields and a checked transaction count. Xatu keeps this material marked
  dataset-declared; it does not advertise cryptographically complete bodies.
  Advanced configurations must use version 1.2.0 with a new instance to
  rebuild existing summaries. Compact `[blocks]` configurations use the new
  instance `block-summary-1-2`, preserving the old instance's stored rows.
  Dataset rows carry no block size, so `sizeBytes` is `null` for them; the
  live lane's overlap with a Xatu backfill matches those summaries.
  `leani subscribe blocks` keys its state directory by this version, so it
  starts cold; run `leani reset subscription blocks` with the rc.1 binary
  first to remove the old directory. Xatu history now expects its daily
  export cadence (24 hours, from 15 minutes): a missing or incomplete
  partition is retried for up to 96 hours before its job fails, and the
  planner ranks Xatu after sources that expect a shorter lag.

- A network fork this release does not know, such as Glamsterdam's Gloas
  before a release supports it, is reported as one: consensus peers that
  answer with an unknown fork digest are skipped rather than banned, with a
  warning to upgrade, and a Beacon API endpoint serving an unknown fork's
  light-client data fails with an error naming it. Finality stays
  unavailable, fail-closed, until Leani is upgraded.
- <a id="rc1-finality-anchor"></a>Verified finality no longer stops about 14
  days after the configured checkpoint's slot. Beacon API finality keeps one
  bootstrapped light client per endpoint and advances it with each finality
  update, plus the sync-committee update when a period ends. It no longer
  verifies the whole chain again from the checkpoint, including its age
  check, on every 12-second poll. Every start and network lane restart, with
  either finality
  source, bootstraps from the persisted anchor when that is newer than the
  configured checkpoint and younger than 14 days, so the configured
  checkpoint's age no longer matters after the first start. A corrupt or
  unreadable anchor file falls back to the configured checkpoint with a
  warning. So does a persisted anchor from which no Beacon API endpoint
  verifies finality at start, or whose bootstrap no peer serves, but only
  if the configured checkpoint then verifies: an outage at start keeps the
  persisted anchor. A Beacon API endpoint that is down at start or bootstraps slowly
  bootstraps later from the newest agreed anchor, so it can join after the
  configured checkpoint ages out; its bootstrap is no longer cancelled by the
  agreement grace period.
- Beacon API agreement waits about two seconds after the first verified
  endpoint for the others, then follows the highest finalized slot that
  `finality.minimum_agreement` endpoints have reached or passed. A lagging
  endpoint no longer holds finality back, readiness no longer drops at epoch
  transitions, and finality never moves backwards.
- A Beacon API endpoint without a trailing slash keeps its last path segment:
  `https://host/beacon` is queried as `https://host/beacon/eth/v1/...`. An
  endpoint's query string, such as a provider's `?apikey=` key, now reaches
  every request after the request's own query, and so does a checkpoint
  provider's for `leani init` and `leani subscribe`; it was dropped, so such
  providers never authenticated.
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
- <a id="rc1-compact"></a>New node stores enable SQLite incremental
  auto-vacuum, so pruning shrinks the database file and storage admission
  recovers instead of staying at the high-water mark. Existing stores log a
  warning at startup until one `leani db compact`, run while the node is
  stopped, converts them.
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
  blocks instead of on every finalized block and finality event. A
  checkpoint still encodes the processor's whole state in memory before it
  stores it, so its peak memory is about the state's encoded size, which
  SQLite's blob limit of about 1 GB caps. `keep` is unchanged. A
  completed historical job also checkpoints its processor at rest, and a
  restore accepts a checkpoint whose block was finalized after it was taken.
  A restore still needs a checkpoint at the current cursor, so between
  checkpoints restore a full-store backup or rebuild the processor; portable
  savepoints are exports and cannot be restored.
- Checkpoints and portable savepoints stay valid after lifecycle edits such
  as raising a limit: their identity is the processor contract without its
  lifecycle policies. Snapshots written by earlier versions stay valid while
  the descriptor is unchanged.
- Breaking (native API): each processor instance keeps at most 16 portable
  savepoints, and a new one is admitted against the physical store budget.
  The API answers a full quota with `409 savepoint_limit`; delete a savepoint
  before creating another.
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
- Breaking (native API): a `required` consumer is rejected with 400 on a live
  stream whose delivery mode is not `until_acknowledged`, where it would pin
  window retention indefinitely. Backfill streams still accept it. Consumers
  registered earlier keep their role, and re-creating one still returns
  `409 consumer_exists`. A store that
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
  configured history source, and without one only that processor's lane
  pauses (`finalized_gap_waiting_for_history_source`). At an unfinalized
  block, as for a block that was never retained, only that processor's lane
  pauses (`unfinalized_gap_waiting_for_finality`); the first drain after
  finality reaches the block recovers it from history, with no operator
  reset. Ordered replay treats such a frame the same way; it failed the lane
  at once, even at a finalized block that history could serve. A
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
  that history has already applied, instead of waiting for it forever. A lane
  that is already paused, failed, or at a gap, or that has no retained frame
  yet, is not parked; its cold range stays uncovered until the next start's
  backfill, and the log says which happened.
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
  no longer has: the gap moves onto the new tip. When the lane has applied
  that tip, a paused lane resumes at once, and a failed lane's gap moves onto
  the tip's successor once that block arrives, where a reset replays it.
  Before, a block-local lane skipped the next block or failed
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
- Chunks that a backfill opens ahead of the one it is reading reserve memory
  only up to half of `budgets.history_material.memory_bytes`. The rest stays
  free for chunks being read. Before, read-ahead chunks could fill the whole
  budget while the chunk the job needed next waited for memory, and the job
  stalled for good. The half is checked as each frame is reserved, so it is
  not strict: a chunk that another job joins, or that its reading job pauses
  and leaves while others wait for it, becomes read-ahead with the frames it
  already holds.
- A historical backfill no longer stalls when shared material memory, or room
  in a shared read's frame buffer, is freed just as its reader finds it full.
  The reader now registers for the wakeup before it checks.
- A history source that panics while streaming fails its read with a
  retryable source error. Its backfills retry, on another source when one is
  configured. Before, every backfill waiting on that read hung. This holds
  for reads through the shared material coordinator, as with the default
  `budgets.history_material.mode`; with `mode = "disabled"`, and in
  `leani backfill`, backfills read their sources directly, and a panic still
  ends the backfill's task.
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
  cold-backfill commit, now records where it stopped at its next live block,
  without a restart: a paused lane when the store refuses that block too, and
  a failed lane unless it is an ordered lane still behind its history.
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
  emits one change per key, for its last event.
- Keyed `evm-events` output follows the canonical chain. It was reduced
  block-locally, so the hot and cold lanes applied a key's blocks in either
  order: the block applied last won, and undoing a block restored the key as
  it was when that block applied. When history delivered an older block after
  a newer live one, undoing the live block deleted the key, or restored a
  stale event, instead of restoring the older block's event. Keyed output now
  reduces blocks in chain order (see Changed), so a key holds its newest event
  and an undo restores the one before it.
- The hot/cold handoff of an ordered processor whose mapped delta records
  finality, `uniswap-latest` since rc.1 and now keyed `evm-events`, no longer
  fails at the first block its live lane holds as a pending delta. The live
  lane maps that block as included and history as finalized, so storing
  history's delta beside the held one conflicted and failed the cold backfill;
  history now applies its own delta, which replaces the held one. The same
  acceptance fixes a block-local live commit that raced history's
  store-then-apply of the same block, which failed the whole live run. A
  drain or gap replay that finds the other finality applied meanwhile counts
  the block as applied instead of failing the lane with
  `processor_live_delta_conflict`. That holds for processors that override
  `finality_variant_checksums`, as every built-in processor whose delta
  records finality does.
- A pending delta whose content differs from its block's applied delta now
  fails the lane with `processor_live_delta_conflict` even while the block's
  frame is retained. The drain deleted it as a finality variant whenever the
  retained frame mapped to the applied delta, without comparing the pending
  delta itself, so a non-deterministic mapper went unseen. A pending delta
  that is one of the frame's finality variants is still cleared. A restart
  still deletes a pending delta for a block the lane applied without
  comparing it: startup reconciliation counts it in
  `discarded_pending_deltas` and fails no lane.
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
- Breaking (JSON-RPC): receipts price blob gas (`blobGasPrice`) with the fork
  active at the block's timestamp, from the same schedule and fee function as
  `blobs-money`, instead of with Cancun's parameters for every fork. Prague
  and later
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
- JSON-RPC notifications, calls without an `id`, run as JSON-RPC 2.0
  requires. Both dispatchers dropped them before running them, so an
  `eth_unsubscribe` notification left its subscription open and sending.
  They still get no response, and a batch of only notifications still gets
  none (HTTP 204). Notifications in a batch also run after its response is
  full, while later calls with IDs still get `-32005` without running.
- A JSON-RPC call written as an array of its fields, such as
  `["2.0", 1, "eth_chainId"]`, gets `-32600` instead of running.
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
  reports the live lane disconnected: the next poll asks other peers. Once
  every poll since the lane's last progress has ended that way for the head
  grace, 12 seconds by default, the lane reports itself disconnected, once,
  and keeps polling. A poll counts
  by its last peer, so another peer's empty answer in the same poll does not
  end the wait.
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
- A reorg no longer ends or silently thins `leani subscribe` output. The
  store records the undo of an entity a block created as a delete with an
  empty payload, and the undo of an update as the previous value, never the
  reverted one. Embedded mode decoded that payload and exited on every reorg
  of `subscribe blocks` and on every reorg removing a matching swap, and
  attached mode dropped undos without data. Both now print an undo as the row
  it reverts, from the last 4,096 changes the run printed, including startup
  rows, without decoding its payload and however late the undo arrives;
  `--format raw` prints the undo envelope. An undo of a change the run saw
  but filtered out, such as another pool's, stays hidden; one of a change it
  never saw prints the `leani.subscription-undo.v1` row while fresh, which on
  a node with more pools can name another pool's change.
- An attached `leani subscribe --finality finalized` no longer misses blocks
  that were included but not final when it connected. It started at the
  change head, after those blocks' changes, so finality later released
  nothing for them. It now replays the node's retained change log and prints
  every block finalized after it connected; the `hello`'s finalized block
  marks what was already final and is not printed.
- `leani subscribe --finality finalized` prints a finalized block however
  late finality arrives. Finalized rows older than 30 minutes were dropped
  without a trace, as during a finality delay; the age limit now applies
  only to included rows, the startup rows, and peer preview rows. A
  finalized block the node republishes, as a recompute does, prints once
  while its change is among the last 4,096 the run remembers, about 13.7
  hours of a block feed.
- `leani subscribe` exits successfully, instead of panicking, when stdout's
  reader goes away, as with `| head -1`. Embedded mode still persists its
  verified anchor on the way out, and its note about that no longer panics
  when stderr is closed too, as with `2>&1 | head`.
- `leani subscribe --mode client --endpoint URL`, and auto mode with
  `--endpoint`, no longer read `./leani.toml` or `--config`, so a broken or
  unrelated local configuration no longer stops them.
- The CLI's SSE parser follows the event-stream format: a lone CR ends a
  line, the last `event` field names the event, a field value loses only one
  leading space, a field without a colon has an empty value, and a leading
  byte-order mark is skipped. It scans each byte once instead of rescanning
  the buffer for every chunk, which was quadratic in the frame size. An
  `event: error` the node marks `retryable` reconnects from the last cursor
  instead of ending the subscription.
- The `leani subscribe` refusal of a changed Uniswap subscription says its
  state was created with another start block or market set, since a reused
  `--data-dir` for other markets is refused the same way. The reset command
  it prints shell-quotes the `--data-dir` and `--config` paths and writes a
  relative path starting with `-` as `./-…`. For a `--data-dir` without a
  subscription marker it says to delete the directory instead of printing a
  reset the command would refuse, and for a path that is not UTF-8 it names
  the options instead of printing an inexact command.
- Two automatically backfilled processor instances of one kind, such as two
  `evm-events` instances, now both reach live. Their hot/cold handoffs and
  automatic backfill jobs shared IDs derived from the kind, so the second
  instance's handoff was refused as another instance's and the network lanes
  failed at every start; with the same start block, the jobs collided too.
  Their archive reconciliations shared IDs the same way, which failed the
  network lanes once both were live.
- A network-lane run that ends early, by an error or a panic after its cold
  backfills started, cancels those backfills and waits for them before the
  next run starts; while it waits, the log names the backfills still running
  every 10 seconds. They kept running unobserved into the next run's startup
  reconciliation and next backfills.
- `serve` no longer waits forever at shutdown for clients of open SSE or
  NDJSON streams, and a second Ctrl-C is no longer ignored.
- A panic in the network lane supervisor no longer leaves readiness as the
  lanes last set it: readiness drops however the supervisor stops.
- Embedded subscriptions (`leani subscribe` without a node) prune their
  delivery log like the node, and compact the finalized coverage of their
  block-local processor. They never pruned, so once the log reached its
  window, 64 MiB or 24 hours of changes, the processor paused and the
  subscription stopped printing, after about one to two weeks.
- One unreadable or obsolete durable backfill job, such as one for a
  processor no longer configured, no longer stops every other job from
  resuming: the scheduler logs it and continues. Finished, failed, and
  cancelled jobs are skipped before their records are decoded.
- `leani backfill` uses the configured `budgets.history_pipeline` settings,
  and three attempts per history source on a gap, as the node's own
  backfills do; it used the built-in defaults.
- A historical job whose task panics is marked failed, with the panic
  message, and frees its place among the jobs that run at once. It stayed
  `running` and kept its place until the node restarted.
- Deleting a historical job right after cancelling it no longer brings the
  job back: the job's task wrote its cancelled checkpoint and outcome after
  the deletion. The deletion now waits up to 10 seconds for the task to end,
  and answers 503 `backfill_unavailable` if it has not.
- A backfill subscription whose `consumer.id` is not 1-128 portable
  characters, or whose `leaseTtlSeconds` is 0 or more than 3,153,600,000
  (100 years), is refused with 400 before its history stream exists; it
  answered 500 and left the stream behind. Creating a durable consumer
  applies the same TTL bound; a TTL too large for the store answered 500
  there too, and one from 100 years up to about 292 million years, where the
  store's millisecond lease expiry overflows, was accepted and now gets 400.
  Existing consumers keep their TTL.
- A long-running EraE source sees eras its mirror publishes after it
  started: the checksum catalog is fetched again after 10 minutes, and a plan
  that asks past the cached catalog refetches it once the catalog is 30
  seconds old. Each refetch that still lacks the range doubles that interval,
  up to 10 minutes, and a refetch that brings new eras starts it over; a
  mirror that has not published the range is no longer polled every 30
  seconds. Refetches send the catalog's `ETag` and `Last-Modified`, so an
  unchanged catalog costs a 304. A chunk already planned opens from the
  cached catalog whatever its age, so a failed refresh no longer fails it.
- A raw-history job waits for a range that only lagging sources lack, those
  with a non-zero expected lag such as an EraE catalog that has not caught
  up, instead of failing: it stays running, with the reasons in
  `last_error`, which a new run keeps until it commits a segment. It fails
  once it has gone without progress for four times the longest expected lag,
  and at least 24 hours, counted from the newest segment it retained or else
  from its creation. Both are persisted, so node restarts do not start the
  count again, and a new job under the same ID counts from its own creation.
  A transport failure, such as an unreachable second mirror, keeps the job
  waiting for at most 72 hours without progress, or the lag bound when that
  is longer, and then fails it with the reasons in `last_error`; it kept the
  job waiting forever. That count includes time the node was down or the job
  paused at its storage limit, so either bound fails a job only once the
  running node has itself seen the range fail for an hour without committing
  a segment; a restart gives the sources that hour again, and frames a source
  delivers without a commit do not. A source
  whose advertised range does not cover the
  range, such as a local archive of older blocks, is passed over. A range
  missing inside the advertised range of a source without expected lag, such
  as a gap in a local archive, still fails the job at once, and another
  source's lag no longer hides a terminal error.
- A backfill whose source read exceeds a source budget, such as the memory a
  Xatu row group needs, tries its next configured source, and fails only
  when none can serve the range; it failed at once. Archive reconciliation
  logs the error and retries the range later instead of ending the network
  lanes.
- A backfill whose source fails with a schema drift, such as a Xatu column of
  another type or an object without a strong ETag, tries its next configured
  source, and fails only when none can serve the range. The drift failed the
  backfill.
- Xatu history reads keep to the source budget's `max_in_flight_requests`
  (`budgets.source_concurrency` in the node). They used `object_store`'s
  default of up to 10 range requests at once.
- A Xatu transaction projection that no filter narrows must hold every
  transaction the beacon payload counts, with indices `0..count`; a filtered
  one may be shorter, but never longer or outside the payload. A block whose
  rows were only partly exported was accepted with transactions missing.
- The Xatu blobs projection proves each block's withdrawals from the global
  withdrawal index before it derives the block's execution size: between two
  slots with rows the indices must continue exactly, so a slot between them
  without rows had none. The nearest slots with rows before and after the
  chunk prove its edges the same way, and a slot with 16 rows, the most a
  payload holds, lacks none. An edge they do not prove, a jump in the
  indices, such as a partly exported slot, or more than 16 rows in one
  payload leaves the chunk incomplete. A slot without rows was taken as a
  block without withdrawals, and a partly exported slot passed, both
  understating the size.
- Xatu log projections accept logs without topics (`LOG0`), whether the
  export leaves `topic0` null, empty, or NUL-padded; one such log aborted the
  whole chunk. A topic after an absent one is refused as corrupt instead of
  being dropped.
- Lookups in retained raw history, such as RPC block and transaction lookups
  served from retained segments, verify a segment's whole-file checksum once
  per process, when the node writes the segment or first reads it, and then
  read one record, checked against its own checksum. Every lookup hashed the
  whole segment, 512 MiB by default, on an async runtime thread. Segment
  reads, writes, and fsyncs now run on blocking threads.
- A raw-history segment whose publication fails after its file closed no
  longer leaves the file and its reservation behind until a restart. Jobs
  whose ranges overlap share segments: before each acquisition a job adopts
  the compatible segments other jobs retained since its last one, and a job
  that publishes the same material another job has just published adopts
  that segment once it verifies; one that fails verification is quarantined,
  and one whose file is gone dropped, and the new copy published in its
  place. It failed durably on the catalog's unique material identity. A job
  adopting other jobs' segments skips one whose file is gone, and forgets it
  when nothing owns it, so deleting and re-creating the jobs that own lost
  blocks acquires them again. A segment file no catalog row claims, such as
  one a failed quarantine left behind, moves to the quarantine when a job
  writes the same segment again; it failed the job with a path collision.
- The retained raw-history source reports transient store failures, such as
  I/O errors, a busy catalog, a catalog disk that fails or fills up, or an
  exhausted connection pool, as unavailable, so reads retry or fail over;
  every failure but an unknown segment was reported as corrupt material. A
  segment that fails verification during a read reports its blocks missing,
  which another source may serve, even when its file cannot be moved to the
  quarantine, and the node logs a warning. One segment's first verification
  no longer holds up first reads of other segments.
- The raw-history catalog opens its SQLite file by name: a data directory
  path containing `%` or `?` was parsed as a URL and opened another file, or
  none.
- A failed processor-artifact segment write removes its partial files, and
  the tiered artifact sink clears a range's stale partial files before it
  writes the range again, never those of a write still running for a caller
  that stopped waiting. A failed write blocked every retry of its range until
  a restart.
- The configuration schema, `config/schema-v1.json`, describes every
  configuration field again. It lacked `processors[].artifacts` and the Xatu
  `chunk_blocks`, `blobs_chunk_blocks`, and `batch_rows`, while refusing
  unknown properties, so the shipped `config/example.toml` failed it. Tests
  now compare the schema with the configuration types field by field and
  value by value, and check that every shipped configuration loads, uses only
  schema keys, and, apart from the two checkpoint templates, validates. An
  output window allows exactly one limit, as validation already required.
- Delivery streams heartbeat on schedule while other processors publish. An
  idle live or backfill consumer stream restarted its heartbeat interval
  whenever another processor's commit or another consumer's acknowledgement
  woke it, so on a busy node it sent none. A backfill stream also sent none
  while a batch waited for its `maximumDelayMs`. Each now sends one at most
  `heartbeatIntervalMs` after its previous record; a heartbeat leaves a
  waiting batch as it is.
- A stored change the node cannot render, or stored data it cannot decode,
  now fails with `retryable: false`, on the changes page, in the SSE `error`
  event, and on every other route. A retry fails the same way, but these
  errors were marked retryable.
- The SDK's delivery sessions no longer leave an abort listener behind for
  each lease renewal, or on the caller's signal for each session that fails
  to open.
- The SDK's SSE and NDJSON parsers scan each received byte once; they
  rescanned the whole buffered event or record for every chunk. An SSE event
  over 8 MiB fails the subscription, and a delivery record over 65 MiB the
  session, with a non-retryable `invalid_response` error instead of growing
  the buffer without bound.
- SDK `subscribe()` resets its reconnect backoff once a stream opens, so a
  quiet but healthy stream reconnects promptly after earlier failures; error
  events that make no progress still back off. Closing a lane of the backfill
  client's `subscribe()` no longer rethrows the caller's abort reason when it
  is not an `AbortError`.
- The Homebrew formula renderer stops on a missing, duplicated, or malformed
  archive checksum. It rendered an empty `sha256` and exited successfully,
  because the lookup failed inside `sed`'s arguments.
- A site build on a commit that carries only an SDK tag stays a preview. The
  SDK tag used to switch the build into production mode and fail it.
- The docs watch check waits until a sentinel page proves that the dev server
  sees docs changes, then gives each step ten measured round trips (20 to 60
  seconds), and reports the last response, the probe files, and Astro's
  output when a step times out. It starts Astro in the foreground on a free
  port without its lock file: Astro 7 detached the server when it detected an
  AI agent, so the check left it running, and the next run found it and
  failed.

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
