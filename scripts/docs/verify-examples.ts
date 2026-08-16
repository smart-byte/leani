import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { examples } from '../../examples/manifest';
import { loadExamples, verifyExampleReferences } from './extract-snippets';

const repositoryRoot = resolve(import.meta.dir, '../..');
const extracted = await loadExamples(examples, repositoryRoot);
await verifyExampleReferences(examples, repositoryRoot);

for (const example of extracted) {
  if (example.verifier.kind === 'json-normalized') {
    const parsed = JSON.parse(await readFile(resolve(repositoryRoot, example.source.file), 'utf8'));
    const normalized = `${JSON.stringify(parsed, null, 2)}\n`;
    const source = await readFile(resolve(repositoryRoot, example.source.file), 'utf8');
    if (source !== normalized) throw new Error(`${example.source.file}: JSON fixture is not normalized`);
  }
}

if (process.argv.includes('--shell')) {
  const files = new Set(
    extracted
      .filter((example) => example.verifier.kind === 'shell-syntax')
      .map((example) => example.source.file),
  );
  for (const file of files) {
    const process = Bun.spawn(['bash', '-n', resolve(repositoryRoot, file)], {
      stdout: 'inherit',
      stderr: 'inherit',
    });
    if ((await process.exited) !== 0) throw new Error(`${file}: shell syntax check failed`);
  }
}

console.log(`verified ${extracted.length} canonical examples`);
