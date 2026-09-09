// Descriptions follow docs/contributing/processor-extension-contract.md and
// docs/reference/processor-contracts.md. These are examples, not live metrics.
export const pipelineStages = [
  { id: 'normalize', label: 'Block envelope', title: 'Two sources. One input contract.', description: 'Capability-aware adapters turn historical and live material into the same block envelope. Each processor declares the material it needs; source trust and provenance stay explicit.' },
  { id: 'validate', label: 'Validation', title: 'Check the block before applying it.', description: 'Validate required material, commitments, and canonical ancestry at the appropriate trust boundary. Finality and provenance follow the data into processing.' },
  { id: 'map', label: 'Parallel map', title: 'Your processors shape each block.', description: 'Each small module is a processor with its own map and reduce. Deterministic map work can run concurrently, turning the requested block material into a compact, versioned delta.' },
  { id: 'reduce', label: 'Ordered reduce', title: 'Independent logic. Ordered state.', description: 'Each processor applies its deltas in canonical order to its own collections. The core commits state, coverage, cursor, undo, and emitted changes atomically.' },
  { id: 'store', label: 'State + delivery', title: 'Keep the products your application needs.', description: 'Query each processor’s retained output or follow its durable changes. State, output, delivery, checkpoints, undo, and raw bytes have separate retention policies.' },
] as const;

export const pipelineProcessors = [
  { id: 'erc20', short: 'ERC-20', label: 'Token balances', color: '#8fe3a7', map: 'Decode watched ERC-20 Transfer logs.', reduce: 'Apply transfers to the watchlist ledger in order.', output: 'Event-derived token balances', href: '/docs/reference/processor-contracts/#erc20-balances-100' },
  { id: 'pools', short: 'Pools', label: 'Pool state', color: '#86d8df', map: 'Decode configured Uniswap V2 / V3 pool events.', reduce: 'Replace each pool’s latest observation in order.', output: 'Exact reserves and sqrtPriceX96', href: '/docs/reference/processor-contracts/#uniswap-observations-210--uniswap-latest-200' },
  { id: 'events', short: 'Events', label: 'Decoded events', color: '#b8b2f2', map: 'Decode logs against your static event ABI.', reduce: 'Write configured entities and emit typed changes.', output: 'Application-shaped event collections', href: '/docs/reference/processor-contracts/#evm-events-100' },
  { id: 'custom', short: 'Yours', label: 'Your processor', color: '#edcb89', map: 'Transform block material with your native Rust logic.', reduce: 'Update your own collections and emit domain changes.', output: 'Your schema, queries, and durable streams', href: '/docs/guides/custom-processor/' },
] as const;
