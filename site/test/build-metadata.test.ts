import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { execFileSync } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { docsBuildMetadata } from '../../scripts/docs/prepare';

const version = '1.2.3-rc.4';
let root = '';

function git(...arguments_: string[]): string {
  return execFileSync(
    'git',
    [
      '-C',
      root,
      '-c',
      'user.name=Leani Test',
      '-c',
      'user.email=test@example.invalid',
      '-c',
      'commit.gpgsign=false',
      '-c',
      'tag.gpgsign=false',
      ...arguments_,
    ],
    { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] },
  );
}

function tagHead(...tags: string[]): void {
  const existing = git('tag', '--list').split('\n').filter(Boolean);
  if (existing.length > 0) git('tag', '--delete', ...existing);
  for (const tag of tags) git('tag', '--annotate', '--message', tag, tag);
}

function metadata(environment: Record<string, string> = {}) {
  return docsBuildMetadata(environment, root);
}

beforeAll(async () => {
  root = await mkdtemp(join(tmpdir(), 'leani-docs-metadata-'));
  git('init', '--quiet', '--initial-branch', 'main');
  await writeFile(join(root, 'Cargo.toml'), `[workspace.package]\nversion = "${version}"\n`, 'utf8');
  git('add', 'Cargo.toml');
  git('commit', '--quiet', '--message', 'fixture');
});

afterAll(async () => {
  if (root) await rm(root, { recursive: true, force: true });
});

describe('documentation build metadata', () => {
  test('an SDK tag does not switch a build into production', () => {
    tagHead(`sdk-v${version}`);
    expect(metadata({ GITHUB_REF_NAME: 'main' })).toMatchObject({ mode: 'preview', docsRef: 'main' });
  });

  test('a node tag beside an SDK tag pins production to the node tag', () => {
    tagHead(`sdk-v${version}`, `v${version}`);
    expect(metadata()).toMatchObject({ mode: 'production', docsRef: `v${version}` });
  });

  test('production fails when the node tag disagrees with the workspace version', () => {
    tagHead('v1.2.2');
    expect(() => metadata()).toThrow(`v${version}`);
  });

  test('production fails unless LEANI_DOCS_REF names the workspace version', () => {
    tagHead();
    const production = { LEANI_SITE_MODE: 'production' };
    expect(() => metadata({ ...production, LEANI_DOCS_REF: 'v1.2.2' })).toThrow(`v${version}`);
    expect(() => metadata({ ...production, LEANI_DOCS_REF: 'main' })).toThrow(`v${version}`);
    expect(metadata({ ...production, LEANI_DOCS_REF: `v${version}` })).toMatchObject({
      mode: 'production',
      docsRef: `v${version}`,
    });
  });

  test('production without LEANI_DOCS_REF derives the workspace version', () => {
    tagHead();
    expect(metadata({ LEANI_SITE_MODE: 'production' })).toMatchObject({ docsRef: `v${version}` });
  });

  test('previews derive the ref from their branch', () => {
    tagHead();
    const branch = 'dependabot/github_actions/actions/checkout-5.1.0';
    expect(metadata({ LEANI_SITE_MODE: 'preview', CF_PAGES_BRANCH: branch })).toMatchObject({
      mode: 'preview',
      docsRef: branch,
    });
  });
});
