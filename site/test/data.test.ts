import { describe, expect, test } from 'bun:test';
import { readdirSync } from 'node:fs';
import followExamples from '../src/data/follow-examples.json';
import followSession from '../src/data/follow-session.json';
import handoffSession from '../src/data/handoff-session.json';
import costs from '../src/data/costs.json';
import processorCatalog from '../../docs/reference/generated/processor-catalog.json';
import { blockRowHtml } from '../src/lib/block-row';

const demoDir = new URL('../src/data/processors/', import.meta.url);

// One `leani subscribe blocks` pretty row, as printed by the CLI.
const BLOCK_ROW =
  /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ {2}block=(\d+) {2}txs=\d+ {2}gas=[\d.]+[kKMG]? \/ [\d.]+[kKMG]? \([\d.]+%\) {2}base_fee=[\d.]+ gwei {2}blobs=\d+ {2}(?:preview|included|finalized)$/;

describe('canned data', () => {
  test('follow examples each carry id, label, title, note, and lines', () => {
    expect(followExamples.length).toBeGreaterThanOrEqual(2);
    for (const example of followExamples) {
      for (const key of ['id', 'label', 'title', 'note', 'noteHref', 'noteLabel'] as const) {
        expect(typeof example[key]).toBe('string');
        expect(example[key].length).toBeGreaterThan(0);
      }
      expect(['ts', 'shell']).toContain(example.lang);
      expect(example.lines.length).toBeGreaterThan(3);
      for (const line of example.lines) expect(typeof line.text).toBe('string');
    }
  });

  test('the follow replay is a recorded session of real CLI rows', () => {
    expect(followSession.recordedAt).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
    expect(followSession.leaniVersion).toMatch(/^\d+\.\d+\.\d+/);
    expect(followSession.command).toBe('leani subscribe blocks');
    // The page renders six rows up front and replays the rest.
    expect(followSession.rows.length).toBeGreaterThan(6);
    let previous = 0;
    for (const row of followSession.rows) {
      const match = row.match(BLOCK_ROW);
      expect(match).not.toBeNull();
      const block = Number(match![1]);
      expect(block).toBeGreaterThan(previous);
      previous = block;
    }
  });

  test('the handoff session is one real parent-hash chain', () => {
    // The landing seam and the docs sequence draw their hash match from these
    // rows, so each block must name the previous block as its parent.
    expect(handoffSession.recordedAt).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/);
    expect(handoffSession.rows.length).toBeGreaterThanOrEqual(2);
    handoffSession.rows.forEach((row, i) => {
      expect(row.blockHash).toMatch(/^0x[0-9a-f]{64}$/);
      if (i === 0) return;
      const previous = handoffSession.rows[i - 1]!;
      expect(row.blockNumber).toBe(previous.blockNumber + 1);
      expect(row.parentHash).toBe(previous.blockHash);
      expect(Number(row.sequence)).toBe(Number(previous.sequence) + 1);
    });
  });

  test('block rows keep their exact text when highlighted', () => {
    for (const row of followSession.rows) {
      const text = blockRowHtml(row).replace(/<[^>]+>/g, '').replace(/&amp;/g, '&');
      expect(text).toBe(row);
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
    expect(showcaseSource).toContain("exampleSource('custom-node-main')");
    expect(showcaseSource).toContain("exampleSource('custom-query-client')");
    expect(quickstartSource).toContain("exampleSource('live-block-first-run')");
    expect(quickstartSource).toContain("exampleSource('live-block-node')");
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
      'block-summary',
      'erc20-balances',
      'evm-events',
      'transaction-stats',
      'uniswap-latest',
      'uniswap-observations',
    ]);
  });
});
