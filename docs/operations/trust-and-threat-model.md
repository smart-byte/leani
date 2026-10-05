---
title: Trust and threat model
description: Identify the assets, trust boundaries, controls, and residual risks of a Leani deployment.
section: operations
order: 30
audience:
  - operator
  - app-developer
status: preview
---

## Protected assets

- canonical processor state and its undo history;
- finality/checkpoint provenance;
- API credentials and source URLs;
- host memory, disk, file descriptors, and outbound bandwidth;
- durable consumer cursors and sink delivery state.

## Trust boundaries

Public datasets, Beacon/HTTP endpoints, and consensus P2P peers are untrusted transports. A source
descriptor declares which completeness claims are dataset-trusted and which
commitments were cryptographically verified. Execution P2P peers are
Byzantine. Processor input requirements reject missing, partial, or failed
material before mapping.

The native API and JSON-RPC endpoints accept hostile input. SQLite and the
configured data directory are trusted local state. No dynamic native plugins
are loaded. Statically linked native processors are fully trusted code: they
run in the node process with its CPU, memory, filesystem, and network
privileges. Their descriptors constrain data selection and retention but are
not a sandbox or a security boundary. Only build processor crates from
reviewed source; deploy untrusted extension logic only after a sandboxed
runtime exists.

## Controls

- strict TOML with unknown-field rejection and localhost defaults;
- explicit source capabilities, trust, range, and byte/concurrency budgets;
- checksummed, path-safe local archive objects;
- block/header/transaction/receipt commitment checks on P2P material;
- local checkpoint, sync-committee, BLS, finality-branch, and execution-payload
  proof verification for both finality transports;
- included blocks anchored to attested heads: each slot, the finality source
  verifies a light-client optimistic update, signed by at least 342 of the 512
  sync-committee members, and its execution branch. Execution P2P fetches
  blocks only up to the newest attested head, and requests the attested header
  and the header chain below it by hash, down from the attested hash; a peer
  that serves another header for a requested hash is banned. Only the finality
  source can publish attested heads; the execution source only reads them.
  Only anchored headers earn peer rewards or clear withheld-header strikes,
  and only a mismatch anchored that way can reset the live lane;
- hash-checked finality: a finalized block must be the retained canonical
  block at its height, only retained blocks that link to it are promoted, and
  a processor's coverage at that height must hold its hash, or another block's
  coverage there fails the processor's lane. Neither chain is finalized on a
  contradiction. One with retained unfinalized blocks restarts the network
  lanes; one with finalized history, or the same one recurring a third time
  before finality moves on, halts them. Every start undoes retained
  unfinalized blocks it cannot prove to be ancestors of the verified finalized
  anchor as a reorg. Where parent links do not reach the anchor, execution
  headers from peers, hash-linked down from it, decide which retained
  unfinalized blocks are its ancestors, and those are kept and finalized.
  Peers can delay that proof, and a start that does not complete it within a
  minute undoes the blocks, but they cannot forge it;
- atomic processor state, undo, coverage, change-log, and outbox commits;
- opaque checksummed cursors bound to store, chain, processor, and version;
- bounded API pages, request bodies, SSE batches, and source queues;
- bounded JSON-RPC work: at most 100 calls per batch, 16 MiB per response,
  10,000 `eth_getLogs` results, 1,000 filter addresses and topic alternatives,
  128 subscriptions per WebSocket connection, and 256 connections, each
  configurable;
- browser isolation for loopback listeners: the native API answers a `Host`
  that is an IP address, `localhost`, or a name in `api.allowed_hosts` (421
  otherwise), so a page that points its own name at 127.0.0.1 is refused;
  both listeners refuse a browser `Origin` that is neither loopback nor
  allowed (403), on streams and WebSocket upgrades too; the native API also
  refuses a GET or HEAD that another site's page sends without an `Origin`
  other than as a navigation, such as an image load, when the browser marks
  it with Fetch Metadata (403); mutations need a JSON body or
  `x-leani-request: 1`, and JSON-RPC calls `application/json` (415
  otherwise), which a cross-site page cannot send without a preflight the
  node never grants; only POST creates query snapshots; and a consumer's
  live or backfill stream route refuses any request that Fetch Metadata
  marks as cross-site or same-site without an allowed `Origin`, navigations
  into frames and tabs included (403);
- a native API bind beyond loopback requires a bearer token, and a JSON-RPC
  bind beyond loopback an explicit opt-in; startup rejects tokens shorter than
  16 characters or other than printable ASCII without spaces, tokens are
  compared in constant time, and CLI help never prints the values of
  `LEANI_API_TOKEN` or `LEANI_CHECKPOINT_URLS`;
- optional bearer authentication with redacted debug output;
- per-consumer credentials: a consumer created with one needs it on every
  consumer-scoped delivery route, streams included, besides any bearer token
  (403 otherwise); it is compared in constant time, stored as a BLAKE3 hash
  keyed with a random per-store secret, scoped to that processor instance,
  stream, and consumer identity, and a new one holds at least 32 printable
  ASCII characters. A backfill subscription's credential also enters the
  subscription's idempotency identity, an unkeyed BLAKE3 hash of the whole
  request. Credentials isolate applications on the delivery routes only:
  listing, inspecting, and revoking a consumer are administrative
  operations that only the bearer token protects;
- consumer session tokens authenticated by a BLAKE3 MAC keyed with the
  per-store secret, which is drawn once from the operating system's random
  source and never leaves the store;
- acknowledgements bounded by what each consumer was delivered, by a page
  read or a stream batch, and on live streams by block boundaries; the
  generic consumer routes refuse consumers of split live streams, whose
  sessions fence renewal and acknowledgement;
- no CORS layer; administrative routes share the native API listener and its
  bearer authentication, so binding it beyond localhost exposes those routes
  as well;
- non-root, read-only container profile with all capabilities dropped;
- fail-closed unsupported RPC responses instead of synthesized partial state.

## Residual risks

- `trusted_dataset` sources can omit predicate-matching rows without a
  cryptographic completeness proof;
- event-derived ERC-20 balances do not model rebases or non-standard hidden
  balance changes;
- the Beacon API finality profile still depends on configured transports even
  though proofs are checked locally;
- `included` rests on the sync committee: at least two thirds of it signing a
  header that is not on the canonical chain would make that header an attested
  head. An attested head is not final, so honest reorgs still remove included
  blocks; a peer that follows another branch lacks the attested block and is
  cooled down, not banned. Attested heads need no agreement between Beacon
  endpoints, since each carries the committee's signature; two different ones
  at one slot are ignored;
- live following adds one to two slots of latency, and stops, reporting not
  ready, while the finality source verifies no attested head;
- the embedded CLI still prints `preview` blocks that one peer supplied before
  the anchored lane is ready; they are never stored or applied, and
  `subscribe --once` returns one only with `--finality preview`;
- native consensus and execution P2P peer availability can prevent timely
  progress, and a stale or incorrectly sourced weak-subjectivity checkpoint
  can select the wrong consensus history;
- bearer tokens do not provide transport encryption;
- two kinds of browser GET still reach the native API without the bearer
  token: navigations from another site's page to routes other than consumer
  streams, including into a frame, and subresource loads that carry no Fetch
  Metadata. Browsers send Fetch Metadata only to potentially trustworthy
  URLs, which are HTTPS, loopback addresses, and `localhost`, so a node
  reached over plain HTTP by another name or address, such as a LAN IP with
  `allow_unauthenticated_remote`, gets none from current browsers either.
  The page cannot read either response, and neither creates a query
  snapshot. Without Fetch Metadata, such a GET, a navigation included, can
  still open the live or backfill stream of a consumer without a credential
  and take its session lease, which locks the consumer out while the
  connection stays open. A bearer token, or the consumer's own credential,
  prevents it, since a browser adds neither on its own. Consumers declared
  in the node configuration have no credential.
  `allow_unauthenticated_remote` and the `allowed_hosts` and
  `allowed_origins` lists widen what reaches a listener; keep them to what
  clients need;
- consumer credentials do not guard the administrative routes: whoever holds
  the bearer token, or any client that reaches the API when none is
  configured, can list, inspect, and revoke every consumer, as it can reset
  a live lane;
- the per-store secret lives in the store: anyone who can read the database
  or one of its backups can test guesses of consumer credentials offline and
  make session tokens, and a backfill subscription's idempotency identity,
  an unkeyed hash of its request, lets them test guesses of that
  subscription's credential without the secret. Credentials created before
  the 32-character minimum keep working until their consumer is re-created;
- a malicious or compromised native processor can exhaust resources, disclose
  node secrets, corrupt process memory through unsafe dependencies, or emit
  plausible but false output;
- resource limits reduce denial-of-service impact but do not replace an edge
  proxy for public internet deployment.

Production exposure requires TLS, independent finality transports, filesystem
quotas, monitoring, backups, and a completed fault-injected soak.
