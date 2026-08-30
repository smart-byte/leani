import { describe, expect, test } from 'bun:test';
import { readdirSync } from 'node:fs';
import heroExamples from '../src/data/hero-examples.json';
import costs from '../src/data/costs.json';
import processorCatalog from '../../docs/reference/generated/processor-catalog.json';

const demoDir = new URL('../src/data/processors/', import.meta.url);

describe('canned data', () => {
  test('hero examples each carry id, label, title, note, and lines', () => {
    expect(heroExamples.length).toBeGreaterThanOrEqual(3);
    for (const example of heroExamples) {
      for (const key of ['id', 'label', 'title', 'note'] as const) {
        expect(typeof example[key]).toBe('string');
        expect(example[key].length).toBeGreaterThan(0);
      }
      if (example.lines) {
        expect(example.lines.length).toBeGreaterThan(3);
        for (const line of example.lines) expect(typeof line.text).toBe('string');
      } else {
        expect(example.example).toBe('query-then-follow');
      }
    }
  });

  test('every processor demo has required fields', async () => {
    const files = readdirSync(demoDir).filter((f) => f.endsWith('.json'));
    expect(files.length).toBe(6);
    const ids = new Set<string>();
    for (const file of files) {
      const demo = (await import(new URL(file, demoDir).pathname)).default;
      ids.add(demo.id);
      for (const key of ['id', 'label', 'caption', 'docsUrl', 'dataType'] as const) {
        expect(typeof demo[key]).toBe('string');
        expect(demo[key].length).toBeGreaterThan(0);
      }
      expect(['toml', 'rust']).toContain(demo.configLang);
      const hasReplay = Array.isArray(demo.output) && demo.output.length > 0;
      const hasTree = demo.outputJson !== null && typeof demo.outputJson === 'object';
      expect(hasReplay || hasTree).toBe(true);
      expect(
        demo.docsUrl.startsWith('https://github.com/smart-byte/leani/') || demo.docsUrl.startsWith('/docs/'),
      ).toBe(true);
      if (demo.helloJson) {
        expect(demo.helloJson.processor.genericApi).toBe('processor-v1');
        expect(Array.isArray(demo.helloJson.processor.queryExtensions)).toBe(true);
        expect('queryApi' in demo.helloJson.processor).toBe(false);
      }
      if (demo.outputJson) {
        expect(['live', 'live_recovery', 'historical_backfill', 'recompute']).toContain(
          demo.outputJson.originKind,
        );
        expect(typeof demo.outputJson.originId).toBe('string');
        expect(typeof demo.outputJson.publicationRevision).toBe('string');
      }
      if ('config' in demo && demo.configLang === 'toml') {
        expect(demo.config).not.toContain('retention =');
        for (const field of ['instance =', 'version =', '[processors.state]', '[processors.output]', '[processors.delivery]', '[processors.checkpoint]', '[processors.undo]']) {
          expect(demo.config).toContain(field);
        }
      }
      if (demo.id === 'custom') {
        expect('config' in demo).toBe(false);
        expect('queryExample' in demo).toBe(false);
        expect(demo.guideUrl).toBe('/docs/guides/custom-processor/');
      }
    }
    expect(ids).toEqual(new Set([
      'blobs-money',
      'erc20-balances',
      'evm-events',
      'uniswap-latest',
      'transaction-stats',
      'custom',
    ]));
  });

  test('landing custom and quickstart snippets use canonical example IDs', async () => {
    const [showcaseSource, quickstartSource] = await Promise.all([
      Bun.file(new URL('../src/components/Showcase.astro', import.meta.url)).text(),
      Bun.file(new URL('../src/components/Quickstart.astro', import.meta.url)).text(),
    ]);
    expect(showcaseSource).toContain("exampleSource('custom-processor-extension')");
    expect(showcaseSource).toContain("exampleSource('custom-query-client')");
    expect(quickstartSource).toContain("exampleSource('bounded-mainnet-quickstart')");
    expect(quickstartSource).toContain("exampleSource('bounded-mainnet-query')");
  });

  test('storage table has 4 lifecycle profiles and evidence notes', () => {
    expect(costs.rows.length).toBe(4);
    expect(costs.rows[0]!.profile).toBe('externalized');
    expect(costs.rows[0]!.highlight).toBe(true);
    expect(costs.rows[3]!.profile).toBe('full output');
    expect(costs.footnotes.length).toBeGreaterThanOrEqual(3);
  });

  test('tracked standard processor catalog includes every built-in factory', () => {
    expect(processorCatalog.processors.map((processor) => processor.id)).toEqual([
      'blobs-money',
      'erc20-balances',
      'evm-events',
      'transaction-stats',
      'uniswap-latest',
      'uniswap-observations',
    ]);
  });
});
