import { mkdir, rm, writeFile } from 'node:fs/promises';
import { resolve, sep } from 'node:path';
import { generateExamples } from './extract-snippets';

export interface DocsBuildMetadata {
  mode: 'preview' | 'production';
  docsRef: string;
  sourceCommit: string;
}

const repositoryRoot = resolve(import.meta.dir, '../..');
const generatedDirectory = resolve(repositoryRoot, 'site/src/generated');

function docsBuildMetadata(): DocsBuildMetadata {
  const mode = process.env.LEANI_SITE_MODE === 'production' ? 'production' : 'preview';
  const docsRef = process.env.LEANI_DOCS_REF?.trim() || 'main';
  const sourceCommit =
    process.env.CF_PAGES_COMMIT_SHA?.trim() ||
    process.env.GITHUB_SHA?.trim() ||
    'local';

  if (!/^(main|v\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)$/.test(docsRef)) {
    throw new Error(`invalid LEANI_DOCS_REF: ${docsRef}`);
  }
  if (mode === 'production' && docsRef === 'main') {
    throw new Error('production documentation must use a release tag, not main');
  }

  return { mode, docsRef, sourceCommit };
}

export async function prepareDocs(): Promise<void> {
  const expectedSuffix = `${sep}site${sep}src${sep}generated`;
  if (!generatedDirectory.endsWith(expectedSuffix)) {
    throw new Error(`refusing to replace unexpected generated path: ${generatedDirectory}`);
  }

  await rm(generatedDirectory, { recursive: true, force: true });
  await mkdir(generatedDirectory, { recursive: true });
  await generateExamples(repositoryRoot);
  await writeFile(
    resolve(generatedDirectory, 'build-metadata.json'),
    `${JSON.stringify(docsBuildMetadata(), null, 2)}\n`,
    'utf8',
  );
}

if (import.meta.main) {
  await prepareDocs();
}
