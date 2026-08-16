import { readdir, readFile, writeFile } from 'node:fs/promises';
import { extname, isAbsolute, relative, resolve, sep } from 'node:path';
import {
  EXAMPLE_LANGUAGES,
  examples as exampleManifest,
  type ExampleDefinition,
  type ExampleLanguage,
  type ExampleSurface,
} from '../../examples/manifest';

const LANGUAGE_EXTENSIONS: Record<ExampleLanguage, readonly string[]> = {
  rust: ['.rs'],
  toml: ['.toml'],
  typescript: ['.ts', '.tsx'],
  shell: ['.sh'],
  json: ['.json'],
};

export interface ExtractedExample {
  id: string;
  title: string;
  description: string;
  language: ExampleLanguage;
  code: string;
  source: { file: string; region?: string };
  surfaces: ExampleSurface[];
  verifier: ExampleDefinition['verifier'];
}

export function validateExampleDefinitions(
  definitions: readonly ExampleDefinition[],
): void {
  const ids = new Set<string>();
  for (const definition of definitions) {
    if (!/^[a-z][a-z0-9-]*$/.test(definition.id)) {
      throw new Error(`invalid example id: ${definition.id}`);
    }
    if (ids.has(definition.id)) throw new Error(`duplicate example id: ${definition.id}`);
    ids.add(definition.id);
    if (!definition.title.trim() || !definition.description.trim()) {
      throw new Error(`${definition.id}: title and description are required`);
    }
    if (!EXAMPLE_LANGUAGES.includes(definition.language)) {
      throw new Error(`${definition.id}: unsupported language ${definition.language}`);
    }
    if (definition.surfaces.length === 0 || new Set(definition.surfaces).size !== definition.surfaces.length) {
      throw new Error(`${definition.id}: surfaces must be non-empty and unique`);
    }
  }
}

export function resolveExampleSource(repositoryRoot: string, file: string): string {
  if (isAbsolute(file)) throw new Error(`example source must be repository-relative: ${file}`);
  const absolutePath = resolve(repositoryRoot, file);
  const relativePath = relative(repositoryRoot, absolutePath);
  if (relativePath === '..' || relativePath.startsWith(`..${sep}`) || isAbsolute(relativePath)) {
    throw new Error(`example source escapes repository root: ${file}`);
  }
  if (relativePath.split(sep).includes('.private')) {
    throw new Error(`example source cannot read private content: ${file}`);
  }
  return absolutePath;
}

export function extractRegions(source: string, file = '<source>'): Map<string, string> {
  const regions = new Map<string, string>();
  const lines = source.replaceAll('\r\n', '\n').split('\n');
  const marker = /^\s*(?:\/\/|#|<!--)\s*docs:(start|end)\s+([a-z][a-z0-9-]*)(?:\s*-->)?\s*$/;
  let active: { name: string; line: number; contentStart: number } | undefined;

  for (const [index, line] of lines.entries()) {
    const match = line.match(marker);
    if (!match) continue;
    const [, kind, name] = match as [string, 'start' | 'end', string];
    if (kind === 'start') {
      if (active) {
        throw new Error(`${file}:${index + 1}: region ${name} overlaps open region ${active.name}`);
      }
      if (regions.has(name)) throw new Error(`${file}:${index + 1}: duplicate region ${name}`);
      active = { name, line: index + 1, contentStart: index + 1 };
      continue;
    }
    if (!active) throw new Error(`${file}:${index + 1}: region ${name} ends without a start marker`);
    if (active.name !== name) {
      throw new Error(`${file}:${index + 1}: region ${name} closes open region ${active.name}`);
    }
    const code = lines.slice(active.contentStart, index).join('\n').trim();
    if (!code) throw new Error(`${file}:${active.line}: region ${name} is empty`);
    regions.set(name, code);
    active = undefined;
  }

  if (active) throw new Error(`${file}:${active.line}: region ${active.name} is not closed`);
  return regions;
}

export async function loadExamples(
  definitions: readonly ExampleDefinition[],
  repositoryRoot: string,
): Promise<ExtractedExample[]> {
  validateExampleDefinitions(definitions);
  const extracted: ExtractedExample[] = [];

  for (const definition of definitions) {
    const absolutePath = resolveExampleSource(repositoryRoot, definition.source.file);
    const source = await readFile(absolutePath, 'utf8').catch((error: NodeJS.ErrnoException) => {
      if (error.code === 'ENOENT') throw new Error(`${definition.id}: missing source ${definition.source.file}`);
      throw error;
    });
    const allowedExtensions = LANGUAGE_EXTENSIONS[definition.language];
    if (!allowedExtensions.includes(extname(absolutePath))) {
      throw new Error(`${definition.id}: ${definition.language} does not match ${definition.source.file}`);
    }

    let code = source.trim();
    if (definition.source.region) {
      const regions = extractRegions(source, definition.source.file);
      code = regions.get(definition.source.region) ?? '';
      if (!code) {
        throw new Error(`${definition.id}: missing region ${definition.source.region} in ${definition.source.file}`);
      }
    }
    if (!code) throw new Error(`${definition.id}: extracted example is empty`);

    extracted.push({
      id: definition.id,
      title: definition.title,
      description: definition.description,
      language: definition.language,
      code,
      source: { ...definition.source },
      surfaces: [...definition.surfaces],
      verifier: definition.verifier,
    });
  }
  return extracted;
}

async function sourceFiles(path: string): Promise<string[]> {
  const files: string[] = [];
  for (const entry of await readdir(path, { withFileTypes: true }).catch(() => [])) {
    if (entry.name === 'generated' || entry.name.startsWith('.')) continue;
    const child = resolve(path, entry.name);
    if (entry.isDirectory()) files.push(...(await sourceFiles(child)));
    else if (entry.isFile() && /\.(?:astro|md|mdx|ts|tsx)$/.test(entry.name)) files.push(child);
  }
  return files;
}

export async function verifyExampleReferences(
  definitions: readonly ExampleDefinition[],
  repositoryRoot: string,
): Promise<void> {
  const references = new Map<string, Set<ExampleSurface>>();
  const roots: Array<[string, ExampleSurface]> = [
    [resolve(repositoryRoot, 'docs'), 'docs'],
    [resolve(repositoryRoot, 'site/src'), 'landing'],
  ];
  for (const [root, surface] of roots) {
    for (const file of await sourceFiles(root)) {
      const contents = await readFile(file, 'utf8');
      const ids = [
        ...Array.from(contents.matchAll(/\bexample=["']([a-z][a-z0-9-]*)["']/g), (match) => match[1]!),
        ...Array.from(contents.matchAll(/\bexampleSource\(\s*["']([a-z][a-z0-9-]*)["']\s*\)/g), (match) => match[1]!),
      ];
      for (const id of ids) {
        const surfaces = references.get(id) ?? new Set<ExampleSurface>();
        surfaces.add(surface);
        references.set(id, surfaces);
      }
    }
  }

  const definitionsById = new Map(definitions.map((definition) => [definition.id, definition]));
  for (const id of references.keys()) {
    if (!definitionsById.has(id)) throw new Error(`unknown referenced example id: ${id}`);
  }
  for (const definition of definitions) {
    const actual = references.get(definition.id) ?? new Set<ExampleSurface>();
    for (const surface of definition.surfaces) {
      if (!actual.has(surface)) throw new Error(`${definition.id}: unused on declared ${surface} surface`);
    }
  }
}

export async function generateExamples(repositoryRoot: string): Promise<void> {
  const extracted = await loadExamples(exampleManifest, repositoryRoot);
  await verifyExampleReferences(exampleManifest, repositoryRoot);
  const payload = {
    version: 1,
    examples: Object.fromEntries(extracted.map((example) => [example.id, example])),
  };
  await writeFile(
    resolve(repositoryRoot, 'site/src/generated/examples.json'),
    `${JSON.stringify(payload, null, 2)}\n`,
    'utf8',
  );
}

if (import.meta.main) {
  const repositoryRoot = resolve(import.meta.dir, '../..');
  await generateExamples(repositoryRoot);
  console.log(`generated ${exampleManifest.length} verified examples`);
}
