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
  lanes, and every start undoes retained unfinalized blocks that do not link
  to the verified finalized anchor as a reorg. One with finalized history, or
  the same one recurring a third time before finality moves on, halts the
  network lanes;
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
  node never grants; and only POST creates query snapshots;
- a native API bind beyond loopback requires a bearer token, and a JSON-RPC
  bind beyond loopback an explicit opt-in; startup rejects tokens shorter than
  16 characters or other than printable ASCII without spaces, tokens are
  compared in constant time, and CLI help never prints the values of
  `LEANI_API_TOKEN` or `LEANI_CHECKPOINT_URLS`;
- optional bearer authentication with redacted debug output;
- random per-consumer bearer credentials stored only as one-way hashes and
  scoped to that processor instance and consumer identity;
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
  the anchored lane is ready; they are never stored or applied;
- native consensus and execution P2P peer availability can prevent timely
  progress, and a stale or incorrectly sourced weak-subjectivity checkpoint
  can select the wrong consensus history;
- bearer tokens do not provide transport encryption;
- two kinds of browser GET still reach a loopback API without the bearer
  token: navigations from another site's page, including into a frame, and
  subresource loads from browsers that send no Fetch Metadata. The page
  cannot read either response, and neither creates a query snapshot, but a
  GET that opens a consumer's live or backfill stream takes that consumer's
  session lease, which locks the consumer out while the connection stays
  open. A bearer token protects consumers from such pages, since a browser
  never adds it on its own; a consumer credential does not yet, because its
  header is optional. `allow_unauthenticated_remote` and the
  `allowed_hosts` and `allowed_origins` lists widen what reaches a listener;
  keep them to what clients need;
- a malicious or compromised native processor can exhaust resources, disclose
  node secrets, corrupt process memory through unsafe dependencies, or emit
  plausible but false output;
- resource limits reduce denial-of-service impact but do not replace an edge
  proxy for public internet deployment.

Production exposure requires TLS, independent finality transports, filesystem
quotas, monitoring, backups, and a completed fault-injected soak.
