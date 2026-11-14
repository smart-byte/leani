import { FAMILY_LABELS, type Family } from './live/model';

const entries = Object.values(
  import.meta.glob<string>('../../../config/chains/*.toml', { query: '?raw', import: 'default', eager: true }),
);

// Display names by chain entry name. A new entry fails the build until it has one.
const LABELS: Record<string, string> = {
  'ethereum-mainnet': 'Mainnet',
  'ethereum-sepolia': 'Sepolia',
  'ethereum-hoodi': 'Hoodi',
  'op-mainnet': 'OP Mainnet',
  'base-mainnet': 'Base',
  'base-sepolia': 'Base Sepolia',
  'ink-mainnet': 'Ink',
  'mode-mainnet': 'Mode',
  'soneium-mainnet': 'Soneium',
  'unichain-mainnet': 'Unichain',
  'worldchain-mainnet': 'World Chain',
  'zora-mainnet': 'Zora',
  'arbitrum-one': 'Arbitrum One',
  'arbitrum-nova': 'Arbitrum Nova',
  'robinhood-mainnet': 'Robinhood Chain',
};

const chains = entries.map((entry) => {
  const name = /^name = "([^"]+)"/m.exec(entry)?.[1] ?? '';
  const family = /^family = "([^"]+)"/m.exec(entry)?.[1] as Family;
  const label = LABELS[name];
  if (!label || !(family in FAMILY_LABELS)) {
    throw new Error(`site/src/lib/chains.ts names no display name or family for chain entry "${name}"`);
  }
  return { label, family, testnet: /-(sepolia|hoodi)$/.test(name) };
});
const mainnets = chains.filter((chain) => !chain.testnet);

/** How many mainnet rollups a node can follow by name. */
export const ROLLUP_COUNT = mainnets.filter((chain) => chain.family !== 'ethereum').length;

/** Ethereum and its mainnet rollups. */
export const CHAIN_COUNT = mainnets.length;

/** The built-in chains of each family, by display name. */
export const CHAIN_FAMILIES = (Object.keys(FAMILY_LABELS) as Family[]).map((family) => {
  const named = (testnet: boolean) =>
    chains.filter((chain) => chain.family === family && chain.testnet === testnet).map((chain) => chain.label).sort();
  return { label: FAMILY_LABELS[family], mainnets: named(false), testnets: named(true) };
});
