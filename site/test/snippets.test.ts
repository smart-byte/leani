import { describe, expect, test } from 'bun:test';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import type { ExampleDefinition } from '../../examples/manifest';
import {
  extractRegions,
  loadExamples,
  resolveExampleSource,
  validateExampleDefinitions,
} from '../../scripts/docs/extract-snippets';

function definition(overrides: Partial<ExampleDefinition> = {}): ExampleDefinition {
  return {
    id: 'probe',
    title: 'Probe',
    description: 'Test example',
    language: 'typescript',
    source: { file: 'probe.ts', region: 'probe' },
    surfaces: ['docs'],
    verifier: { kind: 'typescript', project: 'tsconfig.json' },
    ...overrides,
  };
}

describe('canonical snippet extraction', () => {
  test('extracts one named region without marker lines', () => {
    const regions = extractRegions('// docs:start probe\nconst value = 1;\n// docs:end probe\n');
    expect(regions.get('probe')).toBe('const value = 1;');
  });

  test('rejects duplicate IDs', () => {
    expect(() => validateExampleDefinitions([definition(), definition()])).toThrow('duplicate example id');
  });

  test('rejects overlapping regions', () => {
    expect(() => extractRegions('// docs:start one\n// docs:start two\n')).toThrow('overlaps');
  });

  test('rejects duplicate regions', () => {
    expect(() =>
      extractRegions(
        '// docs:start probe\none\n// docs:end probe\n// docs:start probe\ntwo\n// docs:end probe\n',
      ),
    ).toThrow('duplicate region');
  });

  test('rejects empty regions', () => {
    expect(() => extractRegions('// docs:start probe\n\n// docs:end probe\n')).toThrow('is empty');
  });

  test('rejects path traversal', () => {
    expect(() => resolveExampleSource('/workspace/repo', '../secret.ts')).toThrow('escapes repository root');
  });

  test('rejects missing sources and missing regions', async () => {
    const root = await mkdtemp(resolve(tmpdir(), 'leani-snippets-'));
    try {
      expect(loadExamples([definition()], root)).rejects.toThrow('missing source');
      await writeFile(resolve(root, 'probe.ts'), 'const value = 1;\n', 'utf8');
      expect(loadExamples([definition()], root)).rejects.toThrow('missing region');
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
