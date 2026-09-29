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

function gitValue(root: string, ...arguments_: string[]): string | undefined {
  try {
    return execFileSync('git', ['-C', root, ...arguments_], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim() || undefined;
  } catch {
    return undefined;
  }
}

function workspaceVersion(root: string): string {
  const manifest = readFileSync(resolve(root, 'Cargo.toml'), 'utf8');
  const version = manifest.match(/^version\s*=\s*"([^"]+)"$/m)?.[1];
  if (!version) throw new Error('could not read the workspace version from Cargo.toml');
  return version;
}

export function docsBuildMetadata(
  environment: Record<string, string | undefined> = process.env,
  root = repositoryRoot,
): DocsBuildMetadata {
  const explicitMode = environment.LEANI_SITE_MODE?.trim();
  if (explicitMode && explicitMode !== 'preview' && explicitMode !== 'production') {
    throw new Error(`invalid LEANI_SITE_MODE: ${explicitMode}`);
  }
  // Only a node release tag selects production; an SDK tag on the same
  // commit does not.
  const exactTag = gitValue(root, 'describe', '--tags', '--exact-match', '--match', 'v[0-9]*', 'HEAD');
  const branch =
    environment.CF_PAGES_BRANCH?.trim() ||
    environment.GITHUB_HEAD_REF?.trim() ||
    environment.GITHUB_REF_NAME?.trim() ||
    gitValue(root, 'branch', '--show-current');
  const productionBranch = branch === 'site-production';
  const mode = explicitMode || (exactTag || productionBranch ? 'production' : 'preview');
  let docsRef = environment.LEANI_DOCS_REF?.trim() || exactTag;
  if (mode === 'production') {
    // Production links point into the release being built, so a stale or
    // mistyped LEANI_DOCS_REF, or a mismatched tag, fails the build.
    const release = `v${workspaceVersion(root)}`;
    docsRef ||= release;
    if (docsRef !== release) {
      throw new Error(`production documentation must be pinned to ${release}, the workspace version, not ${docsRef}`);
    }
  }
  docsRef ||= branch || 'main';
  const sourceCommit =
    environment.CF_PAGES_COMMIT_SHA?.trim() ||
    environment.GITHUB_SHA?.trim() ||
    gitValue(root, 'rev-parse', 'HEAD') ||
    'local';

  // Branch names such as dependabot/github_actions/... contain underscores.
  if (!/^[0-9A-Za-z][0-9A-Za-z_./-]*$/.test(docsRef) || docsRef.includes('..')) {
    throw new Error(`invalid documentation ref: ${docsRef}`);
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
