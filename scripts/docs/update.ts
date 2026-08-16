import { resolve } from 'node:path';
import { prepareDocs } from './prepare';

const repositoryRoot = resolve(import.meta.dir, '../..');
const process = Bun.spawn([resolve(repositoryRoot, 'scripts/update-rust-doc-fixtures.sh')], {
  cwd: repositoryRoot,
  stdout: 'inherit',
  stderr: 'inherit',
});
if ((await process.exited) !== 0) throw new Error('Rust documentation fixture update failed');
await prepareDocs();
console.log('updated tracked Rust fixtures and ignored documentation render inputs');
