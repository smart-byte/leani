# Finality, live

The data behind [leani.dev/finality](https://leani.dev/finality): one leani node per
chain, and `hub.ts`, which follows every node's change stream with the SDK
and serves all of them as one server-sent event stream. Rollup nodes run the
`block-summary` processor; the Ethereum node runs `blobs`, whose every blob
transaction names its sender and the address it posts to. The page draws
every chain's blocks by their age and the finality level leani reports for
them, with Ethereum, the settlement layer, along the bottom: a rollup's blocks
go into a blob, into an Ethereum block, and become final with it.

```sh
# One node per chain, each on its own ports (18100 + n for the API).
LEANI=leani ./nodes.sh init ./nodes
LEANI=leani ./nodes.sh start ./nodes

# The hub: op-stack batches decoded from a Beacon API's blobs, Nitro batches
# read from the Ethereum node, fast confirmations from a beacon node, and a
# recording of what it streams.
bun hub.ts $(./nodes.sh hub ./nodes) \
  --beacon https://ethereum-beacon-api.publicnode.com \
  --fcr https://lodestar-mainnet.chainsafe.io \
  --port 8797 --record recording.jsonl

# The page, live from the hub.
open "https://leani.dev/finality?hub=http://127.0.0.1:8797"
```

Without `CHAINS`, `nodes.sh` runs Ethereum, OP Mainnet, Base, Arbitrum One,
and Robinhood Chain; `./nodes.sh stop ./nodes` stops them.

## Batches

`batches.ts` decodes what an op-stack batcher posts: each blob's data
(version 0), the frames in it, their channel once every frame is in, and the
channel's batches: a span batch, whose relative timestamp and block count
name the L2 blocks it carries, or singular batches, one block each, which
Base posts. The hub checks each batch's parent check against the
block before it, where its rollup node has served that block. The blobs come
from the Beacon API named by `--beacon`. An op-stack chain without a node
still gets a row, with the batches Ethereum carries for it.

A Nitro chain's batch post calls its SequencerInbox with the message counts
before and after the batch, and block `genesis + n` is message `n` (22,207,817
on Arbitrum One, 0 on Robinhood Chain). The hub reads each call from the
Ethereum node's JSON-RPC (`--ethereum-rpc`, which `nodes.sh hub` names), so a
batch's blocks are known as soon as the Ethereum block carrying it is.

`--fcr` follows a beacon node's `fast_confirmation` events: the fast
confirmation rule's verdict on each slot, which the page draws as Ethereum's
`confirmed`. The hub reads nothing else outside leani.

## Replay

The page plays a recording when it has no hub. Pack one from a window of the
hub's recording; events before the window's start happen at once, so the
replay opens on the state of a page connecting then:

```sh
bun hub.ts pack recording.jsonl FROM_MS TO_MS | gzip -9 > ../../site/public/finality/replay.json.gz
```

## What a host needs

- Rollup nodes with an L1 anchor finalize a block only once they hold it,
  and the anchor's finalized block is days old: the lane catches up from
  there before it follows the head, and rollup finality reaches the page
  about a week after a fresh start.
- Op-stack gossip peers dial in: each op-stack node needs its gossip port
  (`[finality] listen_port` and `discovery_port`) reachable from the
  internet.
- Behind a home router, keep execution P2P small and start the nodes one
  at a time. A dozen nodes exhaust its connection table. Ethereum, OP
  Mainnet with `[sources.live]` `max_outbound_peers = 6`,
  `max_concurrent_dials = 2`, and, once it had stored its peers,
  `enable_discv5 = false`, and the two Nitro nodes lost no pings once they
  had caught up, but the hub's DNS lookups failed for the first 25 minutes
  while all four caught up at once, and OP Mainnet's six execution peers
  once dropped for 14 minutes. A fresh store without discovery finds no
  execution peers.
