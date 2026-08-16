import { readdir, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { resolve } from 'node:path';
import { prepareDocs } from './prepare';
import { validatePublicDocs } from './validate';

const repositoryRoot = resolve(import.meta.dir, '../..');
const generatedDirectory = resolve(repositoryRoot, 'site/src/generated');

async function digestGeneratedDirectory(): Promise<string> {
  const files: string[] = [];
  async function collect(directory: string, prefix = ''): Promise<void> {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) await collect(resolve(directory, entry.name), relative);
      else if (entry.isFile()) files.push(relative);
    }
  }
  await collect(generatedDirectory);
  files.sort();
  const digest = createHash('sha256');
  for (const path of files) {
    digest.update(path);
    digest.update('\0');
    digest.update(await readFile(resolve(generatedDirectory, path)));
    digest.update('\0');
  }
  return digest.digest('hex');
}

await validatePublicDocs();
await prepareDocs();
const first = await digestGeneratedDirectory();
await prepareDocs();
const second = await digestGeneratedDirectory();

if (first !== second) {
  throw new Error(`documentation preparation is not deterministic: ${first} != ${second}`);
}

console.log(`documentation render inputs are valid and deterministic (${second})`);
