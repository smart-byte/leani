const entries = Object.values(
  import.meta.glob<string>('../../../config/chains/*.toml', { query: '?raw', import: 'default', eager: true }),
);

/** How many mainnet rollups a node can follow by name. */
export const ROLLUP_COUNT = entries.filter(
  (entry) => !/^family = "ethereum"/m.test(entry) && !/^name = "[^"]*-(sepolia|hoodi)"/m.test(entry),
).length;
