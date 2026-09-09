export const EXAMPLE_LANGUAGES = ['rust', 'toml', 'typescript', 'shell', 'json'] as const;

export type ExampleLanguage = (typeof EXAMPLE_LANGUAGES)[number];
export type ExampleSurface = 'docs' | 'landing';
export type ExampleVerifier =
  | { kind: 'cargo-test'; package: string }
  | { kind: 'typescript'; project: string }
  | { kind: 'shell-syntax' }
  | { kind: 'json-normalized' };

export interface ExampleDefinition {
  id: string;
  title: string;
  description: string;
  language: ExampleLanguage;
  source: { file: string; region?: string };
  surfaces: ExampleSurface[];
  verifier: ExampleVerifier;
}

export const examples = [
  {
    id: 'source-install',
    title: 'Build the alpha from source',
    description: 'Build the pinned workspace and use its binary in this terminal.',
    language: 'shell',
    source: { file: 'examples/fixture-quickstart/snippets.sh', region: 'source-install' },
    surfaces: ['docs'],
    verifier: { kind: 'shell-syntax' },
  },
  {
    id: 'custom-node-main',
    title: 'Run Leani with your processor registry',
    description: 'The leani Rust library supplies the CLI and node runtime.',
    language: 'rust',
    source: { file: 'examples/custom-node/src/main.rs', region: 'custom-node-main' },
    surfaces: ['docs', 'landing'],
    verifier: { kind: 'cargo-test', package: 'leani-custom-example' },
  },
  {
    id: 'custom-processor-transform',
    title: 'Map a block and commit its summary',
    description: 'Processor methods from the complete custom-node example.',
    language: 'rust',
    source: { file: 'examples/custom-node/src/main.rs', region: 'custom-processor-transform' },
    surfaces: ['docs'],
    verifier: { kind: 'cargo-test', package: 'leani-custom-example' },
  },
  {
    id: 'live-block-first-run',
    title: 'Follow Ethereum blocks',
    description: 'Start the embedded P2P subscription and inspect a labelled first result.',
    language: 'shell',
    source: { file: 'examples/fixture-quickstart/snippets.sh', region: 'live-block-first-run' },
    surfaces: ['docs', 'landing'],
    verifier: { kind: 'shell-syntax' },
  },
  {
    id: 'live-block-node',
    title: 'Prepare a reusable block node',
    description: 'Initialize a compact processor config and serve its API.',
    language: 'shell',
    source: { file: 'examples/fixture-quickstart/snippets.sh', region: 'live-block-node' },
    surfaces: ['docs', 'landing'],
    verifier: { kind: 'shell-syntax' },
  },
  {
    id: 'custom-processor-extension',
    title: 'Custom processor query extension',
    description: 'A native processor factory attaching a typed, processor-owned query route.',
    language: 'rust',
    source: { file: 'examples/custom-node/src/main.rs', region: 'custom-processor-extension' },
    surfaces: ['docs'],
    verifier: { kind: 'cargo-test', package: 'leani-custom-example' },
  },
  {
    id: 'custom-processor-config',
    title: 'Custom processor configuration',
    description: 'The complete lifecycle policy for the block-summary processor instance.',
    language: 'toml',
    source: { file: 'examples/custom-node/node.toml', region: 'custom-processor-config' },
    surfaces: ['docs'],
    verifier: { kind: 'cargo-test', package: 'leani-custom-example' },
  },
  {
    id: 'custom-query-client',
    title: 'Discover and query a custom extension',
    description: 'Capability-driven TypeScript lookup of a processor-owned query route.',
    language: 'typescript',
    source: { file: 'examples/custom-node/client.ts', region: 'custom-query-client' },
    surfaces: ['docs', 'landing'],
    verifier: { kind: 'typescript', project: 'examples/sdk-subscription/tsconfig.json' },
  },
  {
    id: 'durable-consumer',
    title: 'Durable SDK consumer',
    description: 'Commit application state and its Leani cursor atomically before acknowledging.',
    language: 'typescript',
    source: { file: 'examples/sdk-subscription/index.ts', region: 'durable-consumer' },
    surfaces: ['docs'],
    verifier: { kind: 'typescript', project: 'examples/sdk-subscription/tsconfig.json' },
  },
  {
    id: 'query-then-follow',
    title: 'Query once, then follow without a race',
    description: 'Seed local state at an exact boundary, resume SSE there, and handle apply, undo, and finalized events.',
    language: 'typescript',
    source: { file: 'examples/sdk-stream/index.ts', region: 'query-then-follow' },
    surfaces: ['docs'],
    verifier: { kind: 'typescript', project: 'examples/sdk-subscription/tsconfig.json' },
  },
  {
    id: 'postgres-destination',
    title: 'Atomic PostgreSQL destination',
    description: 'Apply an idempotent change and persist its opaque cursor in one PostgreSQL transaction.',
    language: 'typescript',
    source: { file: 'examples/sdk-postgres/index.ts', region: 'postgres-destination' },
    surfaces: ['docs'],
    verifier: { kind: 'typescript', project: 'examples/sdk-subscription/tsconfig.json' },
  },
  {
    id: 'offline-quickstart',
    title: 'Offline fixture proof',
    description: 'Exercise the installed binary without an Ethereum provider.',
    language: 'shell',
    source: { file: 'examples/fixture-quickstart/snippets.sh', region: 'offline-quickstart' },
    surfaces: ['docs'],
    verifier: { kind: 'shell-syntax' },
  },
  {
    id: 'bounded-mainnet-quickstart',
    title: 'Bounded Mainnet run',
    description: 'Use the checked-in configuration to index a finite Mainnet range.',
    language: 'shell',
    source: { file: 'examples/fixture-quickstart/snippets.sh', region: 'bounded-mainnet-quickstart' },
    surfaces: ['docs'],
    verifier: { kind: 'shell-syntax' },
  },
  {
    id: 'bounded-mainnet-query',
    title: 'Query indexed Uniswap events',
    description: 'Read decoded Mainnet events and their exact processor coverage from the local API.',
    language: 'shell',
    source: { file: 'examples/fixture-quickstart/snippets.sh', region: 'bounded-mainnet-query' },
    surfaces: ['docs'],
    verifier: { kind: 'shell-syntax' },
  },
] as const satisfies readonly ExampleDefinition[];
