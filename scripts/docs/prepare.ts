import { mkdir, rm, writeFile } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { resolve, sep } from 'node:path';
import { generateExamples } from './extract-snippets';
import { generateReference } from './generate-reference';

export interface DocsBuildMetadata {
  mode: 'preview' | 'production';
  docsRef: string;
  sourceCommit: string;
}

const repositoryRoot = resolve(import.meta.dir, '../..');
const generatedDirectory = resolve(repositoryRoot, 'site/src/generated');

function gitValue(...arguments_: string[]): string | undefined {
  try {
    return execFileSync('git', ['-C', repositoryRoot, ...arguments_], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim() || undefined;
  } catch {
    return undefined;
  }
}

export function docsBuildMetadata(): DocsBuildMetadata {
  const explicitMode = process.env.LEANI_SITE_MODE?.trim();
  if (explicitMode && explicitMode !== 'preview' && explicitMode !== 'production') {
    throw new Error(`invalid LEANI_SITE_MODE: ${explicitMode}`);
  }
  const exactTag = gitValue('describe', '--tags', '--exact-match', 'HEAD');
  const branch =
    process.env.CF_PAGES_BRANCH?.trim() ||
    process.env.GITHUB_HEAD_REF?.trim() ||
    process.env.GITHUB_REF_NAME?.trim() ||
    gitValue('branch', '--show-current');
  const productionBranch = branch === 'site-production';
  const mode = explicitMode || (exactTag || productionBranch ? 'production' : 'preview');
  let docsRef = process.env.LEANI_DOCS_REF?.trim() || exactTag;
  if (!docsRef && mode === 'production') {
    const manifest = readFileSync(resolve(repositoryRoot, 'Cargo.toml'), 'utf8');
    const version = manifest.match(/^version\s*=\s*"([^"]+)"$/m)?.[1];
    if (!version) throw new Error('could not derive production documentation tag from Cargo.toml');
    docsRef = `v${version}`;
  }
  docsRef ||= branch || 'main';
  const sourceCommit =
    process.env.CF_PAGES_COMMIT_SHA?.trim() ||
    process.env.GITHUB_SHA?.trim() ||
    gitValue('rev-parse', 'HEAD') ||
    'local';

  if (!/^[0-9A-Za-z][0-9A-Za-z._/-]*$/.test(docsRef) || docsRef.includes('..')) {
    throw new Error(`invalid documentation ref: ${docsRef}`);
  }
  if (mode === 'production' && !/^v\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/.test(docsRef)) {
    throw new Error('production documentation must resolve to an exact semantic-version tag');
  }

  return { mode: mode as DocsBuildMetadata['mode'], docsRef, sourceCommit };
}

export async function prepareDocs(): Promise<void> {
  const expectedSuffix = `${sep}site${sep}src${sep}generated`;
  if (!generatedDirectory.endsWith(expectedSuffix)) {
    throw new Error(`refusing to replace unexpected generated path: ${generatedDirectory}`);
  }

  await rm(generatedDirectory, { recursive: true, force: true });
  await mkdir(generatedDirectory, { recursive: true });
  await generateExamples(repositoryRoot);
  await generateReference(repositoryRoot);
  await writeFile(
    resolve(generatedDirectory, 'build-metadata.json'),
    `${JSON.stringify(docsBuildMetadata(), null, 2)}\n`,
    'utf8',
  );
}

if (import.meta.main) {
  await prepareDocs();
}
