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
    id: 'custom-processor-extension',
    title: 'Custom processor query extension',
    description: 'A native processor factory attaching a typed, processor-owned query route.',
    language: 'rust',
    source: { file: 'examples/custom-node/src/main.rs', region: 'custom-processor-extension' },
    surfaces: ['docs', 'landing'],
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
    id: 'durable-consumer-output',
    title: 'Durable consumer acknowledgement',
    description: 'Normalized expected state after a destination commit and acknowledgement.',
    language: 'json',
    source: { file: 'examples/sdk-subscription/expected-output.json' },
    surfaces: ['docs'],
    verifier: { kind: 'json-normalized' },
  },
  {
    id: 'offline-quickstart',
    title: 'Offline fixture proof',
    description: 'Install a release and exercise its runtime without an Ethereum provider.',
    language: 'shell',
    source: { file: 'examples/fixture-quickstart/run.sh', region: 'offline-quickstart' },
    surfaces: ['docs', 'landing'],
    verifier: { kind: 'shell-syntax' },
  },
  {
    id: 'bounded-mainnet-quickstart',
    title: 'Bounded Mainnet run',
    description: 'Validate a release-pinned configuration before requesting a finite range.',
    language: 'shell',
    source: { file: 'examples/fixture-quickstart/run.sh', region: 'bounded-mainnet-quickstart' },
    surfaces: ['docs', 'landing'],
    verifier: { kind: 'shell-syntax' },
  },
] as const satisfies readonly ExampleDefinition[];
