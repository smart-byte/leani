import { describe, expect, test } from 'bun:test';
import { readdirSync } from 'node:fs';
import heroExamples from '../src/data/hero-examples.json';
import costs from '../src/data/costs.json';

const demoDir = new URL('../src/data/processors/', import.meta.url);

describe('canned data', () => {
  test('hero examples each carry id, label, title, note, and lines', () => {
    expect(heroExamples.length).toBeGreaterThanOrEqual(3);
    for (const example of heroExamples) {
      for (const key of ['id', 'label', 'title', 'note'] as const) {
        expect(typeof example[key]).toBe('string');
        expect(example[key].length).toBeGreaterThan(0);
      }
      expect(example.lines.length).toBeGreaterThan(3);
      for (const line of example.lines) expect(typeof line.text).toBe('string');
    }
  });

  test('every processor demo has required fields', async () => {
    const files = readdirSync(demoDir).filter((f) => f.endsWith('.json'));
    expect(files.length).toBe(6);
    const ids = new Set<string>();
    for (const file of files) {
      const demo = (await import(new URL(file, demoDir).pathname)).default;
      ids.add(demo.id);
      for (const key of ['id', 'label', 'caption', 'docsUrl', 'config', 'dataType'] as const) {
        expect(typeof demo[key]).toBe('string');
        expect(demo[key].length).toBeGreaterThan(0);
      }
      expect(['toml', 'rust']).toContain(demo.configLang);
      const hasReplay = Array.isArray(demo.output) && demo.output.length > 0;
      const hasTree = demo.outputJson !== null && typeof demo.outputJson === 'object';
      expect(hasReplay || hasTree).toBe(true);
      expect(demo.docsUrl.startsWith('https://github.com/smart-byte/leani/')).toBe(true);
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
      if (demo.configLang === 'toml') {
        expect(demo.config).not.toContain('retention =');
        for (const field of ['instance =', 'version =', '[processors.state]', '[processors.output]', '[processors.delivery]', '[processors.checkpoint]', '[processors.undo]']) {
          expect(demo.config).toContain(field);
        }
      }
      if (demo.id === 'custom') {
        expect(demo.config).toContain('ProcessorComponents');
        expect(demo.config).toContain('QueryExtension');
        expect(demo.queryExample).toContain('leani.request<BlockSummary>');
        expect(demo.guideUrl).toBe('/docs#custom-processors');
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

  test('storage table has 4 lifecycle profiles and evidence notes', () => {
    expect(costs.rows.length).toBe(4);
    expect(costs.rows[0]!.profile).toBe('externalized');
    expect(costs.rows[0]!.highlight).toBe(true);
    expect(costs.rows[3]!.profile).toBe('full output');
    expect(costs.footnotes.length).toBeGreaterThanOrEqual(3);
  });
});
