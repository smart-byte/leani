export type Level = 'unattested' | 'attested' | 'confirmed' | 'finalized';

/** How far a level goes: a block at one has passed every lower one. */
export const RANK: Record<Level, number> = { unattested: 0, attested: 1, confirmed: 2, finalized: 3 };
export type Family = 'ethereum' | 'op-stack' | 'arbitrum';

export interface Chain {
  /** The chain entry's name, as `leani init --network` takes it. */
  id: string;
  label: string;
  family: Family;
  chainId: number;
  color: string;
  /** How Ethereum settles the rollup, which leani reports as `finalized`:
   * the claims its L1 anchor reads, and how long after a block that
   * typically takes. `null` for Ethereum, final by its own consensus. */
  settlement: { via: string; typical: string; seconds: number } | null;
}

// Ethereum sits in the middle, the op-stack chains to its left, the Nitro
// chains to its right, so every blob and anchor line runs to one column.
export const CHAINS: Chain[] = [
  { id: 'op-mainnet', label: 'OP Mainnet', family: 'op-stack', chainId: 10, color: '#ff4d5e', settlement: { via: 'super-root games', typical: '~7 d', seconds: 7 * 86400 } },
  { id: 'base-mainnet', label: 'Base', family: 'op-stack', chainId: 8453, color: '#3d7bff', settlement: { via: 'dispute games', typical: '~5 d', seconds: 5 * 86400 } },
  { id: 'ethereum-mainnet', label: 'Ethereum', family: 'ethereum', chainId: 1, color: '#9aa8ff', settlement: null },
  { id: 'arbitrum-one', label: 'Arbitrum One', family: 'arbitrum', chainId: 42161, color: '#2fa8f5', settlement: { via: 'BoLD', typical: '~6.4 d', seconds: 6.4 * 86400 } },
  { id: 'robinhood-mainnet', label: 'Robinhood Chain', family: 'arbitrum', chainId: 4663, color: '#c8f53a', settlement: { via: 'BoLD', typical: '~6.4 d', seconds: 6.4 * 86400 } },
];

export const POSTERS: { chain: string; from: string; to: string }[] = [
  { chain: 'op-mainnet', from: '0x6887246668a3b87f54deb3b94ba47a6f63f32985', to: '0xff00000000000000000000000000000000000010' },
  { chain: 'base-mainnet', from: '0x5050f69a9786f081509234f1a7f4684b5e5b76c9', to: '0xff00000000000000000000000000000000008453' },
  { chain: 'arbitrum-one', from: '0xc1b634853cb333d3ad8663715b08f41a3aec47cc', to: '0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6' },
  { chain: 'robinhood-mainnet', from: '0xdaa526086787d9debe1d7f3ffdb1fe50cf8687f4', to: '0xbd0d173eeb87d57a09521c24388a12789f33ba96' },
];

export const FAMILY_LABELS: Record<Family, string> = {
  ethereum: 'Ethereum',
  'op-stack': 'OP Stack',
  arbitrum: 'Arbitrum Nitro',
};

export interface Stage {
  /** The leani level this stage is, or `null` for a milestone leani does not
   * report as a level. */
  level: Level | null;
  name: string;
  evidence: string;
  undo: string;
  /** How long after a block it typically comes: one value for every chain,
   * or each chain's by its id. */
  typical: string | Record<string, string>;
}

const settles = (family: Family) =>
  Object.fromEntries(CHAINS.filter((chain) => chain.family === family && chain.settlement).map((chain) => [chain.id, chain.settlement!.typical]));

export const STAGES: Record<Family, Stage[]> = {
  ethereum: [
    {
      level: 'unattested',
      name: 'Seen',
      evidence:
        'At least two execution peers served the same header, it passes leani’s header rules (base fee, gas limit, blob gas, slot timing, fork fields), and it extends the delivered chain.',
      undo: 'A reorg, an attested head on another block, or 60 s without an attestation.',
      typical: '~2–5 s',
    },
    {
      level: 'attested',
      name: 'Attested',
      evidence:
        'At least 342 of the 512 sync-committee members signed it or a descendant, verified locally from a light-client update.',
      undo: 'A reorg.',
      typical: '~15 s',
    },
    {
      level: 'confirmed',
      name: 'Confirmed',
      evidence:
        'The fast confirmation rule: enough of the slot’s attestations back the block that no reorg removes it while the network is synchronous. The page reads it from a public beacon node’s fast_confirmation stream, the source leani’s confirmed level will read.',
      undo: 'Only a reorg during a long network partition.',
      typical: '~13 s',
    },
    {
      level: 'finalized',
      name: 'Finalized',
      evidence: 'Casper FFG finality, verified locally from a light-client finality update.',
      undo: 'Never.',
      typical: '~13–19 min',
    },
  ],
  'op-stack': [
    {
      level: 'attested',
      name: 'Sequencer-signed',
      evidence:
        'The chain’s sequencer signed the block in its gossip, checked against the key in leani’s chain entry. Blocks reach attested about as fast as unattested.',
      undo: 'Only if the sequencer reorgs its own unsafe chain.',
      typical: '~1 s',
    },
    {
      level: null,
      name: 'In a blob on Ethereum',
      evidence:
        'The batcher compresses a run of blocks into a channel and posts it as blobs to the chain’s batch inbox. Leani’s blobs processor sees the transaction in an Ethereum block; the page decodes which blocks the blobs carry. From here on, anyone can derive the blocks from Ethereum.',
      undo: 'An Ethereum reorg of the block that carries the blobs; the batcher then posts them again.',
      typical: { 'op-mainnet': 'minutes', 'base-mainnet': '~30 s' },
    },
    {
      level: null,
      name: 'Final on Ethereum',
      evidence:
        'The Ethereum block that carries the blobs is finalized, so the blocks’ data can no longer change. Op-stack nodes call these blocks finalized; leani does not report it as a level.',
      undo: 'Never.',
      typical: '~15–30 min',
    },
    {
      level: 'finalized',
      name: 'Settled on Ethereum',
      evidence:
        'A dispute game claimed the block’s output root, or on OP Mainnet a super root over it, and the anchor state registry moved to it after its challenge window, read from finalized Ethereum blocks and their receipts: withdrawals can complete.',
      undo: 'Never.',
      typical: settles('op-stack'),
    },
  ],
  arbitrum: [
    {
      level: 'attested',
      name: 'Feed-signed',
      evidence:
        'The sequencer feed message carries the block’s hash signed by the batch poster. Endpoints only carry blocks: one with another hash is refused.',
      undo: 'Only if the sequencer reorgs its own chain.',
      typical: '<1 s',
    },
    {
      level: null,
      name: 'Batch on Ethereum',
      evidence:
        'The batch poster posts the sequenced transactions to Ethereum, calling the chain’s SequencerInbox with its message counts before and after the batch: the page reads which blocks the batch carries from that call. Leani does not report this as a level.',
      undo: 'An Ethereum reorg before the batch’s L1 block is final.',
      typical: 'minutes',
    },
    {
      level: null,
      name: 'Assertion',
      evidence: 'A validator posts a BoLD assertion naming the block hash at the end of its range. Anyone can challenge it.',
      undo: 'A successful challenge.',
      typical: '~1 h',
    },
    {
      level: 'finalized',
      name: 'Confirmed on Ethereum',
      evidence:
        'The rollup confirmed the assertion after the challenge period, read from finalized Ethereum blocks and their receipts.',
      undo: 'Never.',
      typical: settles('arbitrum'),
    },
  ],
};

export type HubEvent =
  | {
      type: 'block'; chain: string; number: number; hash: string; time: number; seen: number; level: Level;
      txs: number | null; gas: number | null;
      /** An Ethereum block's blobs by the chain that posted them, `other` for the rest. */
      blobs?: Record<string, number>;
    }
  | { type: 'level'; chain: string; level: Exclude<Level, 'unattested'>; through: number; time: number | null; at: number }
  | { type: 'undo'; chain: string; number: number; hash: string; at: number }
  | { type: 'status'; chain: string; connected: boolean; at: number }
  | {
      type: 'batch'; chain: string; l1: number; l1Hash: string; firstTime: number; lastTime: number;
      /** Seconds: the timestamp of Ethereum block `l1`. */
      l1Time: number;
      at: number; blobs: number;
      first: number; last: number; txs: number;
      /** Compressed and decompressed bytes of the channel, 0 when the batch was not decoded. */
      bytes: number; raw: number;
      /** Whether the batch's parent check matched the block before `first`, `null` if unknown. */
      verified: boolean | null;
      /** How the page knows the batch's blocks: decoded from its blobs, or named by its SequencerInbox call. */
      source: 'blobs' | 'inbox';
    };

export interface Hello {
  chains: string[];
  /** The hub's clock, in milliseconds, when it sent this. */
  now: number;
  startedAt: number;
}
