import { createRequire } from 'node:module';
import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

type JsonObject = Record<string, any>;

const HTTP_METHODS = ['get', 'post', 'put', 'patch', 'delete'] as const;

function siteRequire(repositoryRoot: string): NodeJS.Require {
  return createRequire(resolve(repositoryRoot, 'site/package.json'));
}

async function readJson(path: string, requireNormalized = false): Promise<JsonObject> {
  const source = await readFile(path, 'utf8');
  const parsed = JSON.parse(source) as JsonObject;
  if (requireNormalized && `${JSON.stringify(parsed, null, 2)}\n` !== source) {
    throw new Error(`${path}: tracked JSON fixture must use normalized two-space formatting`);
  }
  return parsed;
}

function apiGroup(path: string): string {
  if (path.startsWith('/health/') || path === '/metrics') return 'Health and observability';
  if (path.startsWith('/v1/q/')) return 'Typed query extensions';
  if (path.startsWith('/v1/processors/')) return 'Processor data and delivery';
  if (path.startsWith('/v1/')) return 'Node';
  if (path.startsWith('/admin/')) return 'Administration';
  return 'Other';
}

function authentication(security: unknown): string {
  if (!Array.isArray(security) || security.length === 0) return 'public';
  if (security.some((entry) => entry && typeof entry === 'object' && Object.keys(entry).length === 0)) {
    return 'optional bearer';
  }
  return 'bearer';
}

async function httpReference(repositoryRoot: string): Promise<JsonObject> {
  const require = siteRequire(repositoryRoot);
  const { parse } = require('yaml') as { parse: (input: string) => JsonObject };
  const document = parse(await readFile(resolve(repositoryRoot, 'api/openapi.yaml'), 'utf8'));
  const routerSource = await readFile(resolve(repositoryRoot, 'crates/api/src/lib.rs'), 'utf8');
  const routedPaths = new Set(
    Array.from(routerSource.matchAll(/\.route\(\s*"([^"]+)"/g), (match) => match[1]!).filter(
      (path) => /^(?:\/v1\/|\/admin\/|\/health\/|\/metrics$|\/debug\/)/.test(path),
    ),
  );
  const documentedPaths = new Set(Object.keys(document.paths ?? {}));
  const undocumented = [...routedPaths].filter((path) => !documentedPaths.has(path)).sort();
  if (undocumented.length > 0) {
    throw new Error(`native API routes missing from OpenAPI: ${undocumented.join(', ')}`);
  }
  const operations: JsonObject[] = [];
  for (const [path, item] of Object.entries(document.paths ?? {}).sort(([left], [right]) => left.localeCompare(right))) {
    for (const method of HTTP_METHODS) {
      const operation = item?.[method];
      if (!operation) continue;
      operations.push({
        group: apiGroup(path),
        method: method.toUpperCase(),
        path,
        summary: operation.summary ?? '',
        operationId: operation.operationId ?? '',
        authentication: authentication(operation.security ?? document.security),
        responses: Object.keys(operation.responses ?? {}),
      });
    }
  }
  if (operations.length < 30) throw new Error(`OpenAPI extraction found only ${operations.length} operations`);
  return { schemaVersion: 1, source: 'api/openapi.yaml', operations };
}

function resolveSchema(root: JsonObject, schema: JsonObject): JsonObject {
  let current = schema;
  const seen = new Set<string>();
  while (typeof current?.$ref === 'string' && current.$ref.startsWith('#/')) {
    if (seen.has(current.$ref)) throw new Error(`cyclic schema reference: ${current.$ref}`);
    seen.add(current.$ref);
    current = current.$ref
      .slice(2)
      .split('/')
      .reduce((value: any, segment: string) => value?.[segment.replaceAll('~1', '/').replaceAll('~0', '~')], root);
    if (!current) throw new Error(`missing schema reference: ${schema.$ref}`);
  }
  return current;
}

function schemaKind(root: JsonObject, schema: JsonObject): string {
  if (schema.$ref) return schema.$ref.split('/').at(-1) ?? 'reference';
  if (Array.isArray(schema.enum)) return schema.enum.map(String).join(' | ');
  if ('const' in schema) return JSON.stringify(schema.const);
  if (Array.isArray(schema.oneOf)) return schema.oneOf.map((candidate: JsonObject) => schemaKind(root, candidate)).join(' | ');
  if (schema.type === 'array') return `array<${schemaKind(root, schema.items ?? {})}>`;
  return schema.type ?? 'any';
}

function schemaConstraints(schema: JsonObject): JsonObject {
  return Object.fromEntries(
    ['default', 'minimum', 'maximum', 'minLength', 'maxLength', 'minItems', 'maxItems', 'pattern', 'format']
      .filter((key) => key in schema)
      .map((key) => [key, schema[key]]),
  );
}

async function configReference(repositoryRoot: string): Promise<JsonObject> {
  const root = await readJson(resolve(repositoryRoot, 'config/schema-v1.json'));
  const fields: JsonObject[] = [];
  const visit = (schema: JsonObject, path: string, required: boolean): void => {
    const resolved = resolveSchema(root, schema);
    fields.push({
      path,
      required,
      kind: schemaKind(root, schema),
      description: schema.description ?? resolved.description ?? '',
      constraints: { ...schemaConstraints(resolved), ...schemaConstraints(schema) },
    });
    const object = resolved.type === 'object' || resolved.properties ? resolved : undefined;
    if (object) {
      const requiredFields = new Set<string>(object.required ?? []);
      for (const [name, child] of Object.entries(object.properties ?? {})) {
        visit(child as JsonObject, path ? `${path}.${name}` : name, requiredFields.has(name));
      }
    }
    if (resolved.type === 'array' && resolved.items) {
      const item = resolveSchema(root, resolved.items);
      if (item.type === 'object' || item.properties) {
        const requiredFields = new Set<string>(item.required ?? []);
        for (const [name, child] of Object.entries(item.properties ?? {})) {
          visit(child as JsonObject, `${path}[].${name}`, requiredFields.has(name));
        }
      }
    }
  };
  const rootRequired = new Set<string>(root.required ?? []);
  for (const [name, schema] of Object.entries(root.properties ?? {})) {
    visit(schema as JsonObject, name, rootRequired.has(name));
  }
  if (fields.length < 50) throw new Error(`configuration extraction found only ${fields.length} fields`);
  return { schemaVersion: 1, source: 'config/schema-v1.json', fields };
}

async function sdkReference(repositoryRoot: string): Promise<JsonObject> {
  const require = siteRequire(repositoryRoot);
  const ts = require('typescript') as any;
  const path = resolve(repositoryRoot, 'packages/sdk/src/index.ts');
  const source = await readFile(path, 'utf8');
  const sourceFile = ts.createSourceFile(path, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const declaration = sourceFile.statements.find(
    (statement: any) => ts.isInterfaceDeclaration(statement) && statement.name.text === 'LeaniClient',
  );
  if (!declaration) throw new Error('packages/sdk/src/index.ts: missing exported LeaniClient interface');

  const methods: JsonObject[] = [];
  const memberName = (member: any): string => member.name?.getText(sourceFile).replace(/^['"]|['"]$/g, '') ?? '';
  const visit = (members: readonly any[], prefix = ''): void => {
    for (const member of members) {
      const name = memberName(member);
      if (!name) continue;
      const qualified = prefix ? `${prefix}.${name}` : name;
      if (ts.isPropertySignature(member) && member.type && ts.isTypeLiteralNode(member.type)) {
        visit(member.type.members, qualified);
        continue;
      }
      methods.push({
        name: qualified,
        kind: ts.isMethodSignature(member) ? 'method' : 'property',
        signature: member.getText(sourceFile).replace(/\s+/g, ' ').replace(/;$/, ''),
      });
    }
  };
  visit(declaration.members);
  if (methods.length < 25) throw new Error(`SDK extraction found only ${methods.length} members`);
  return { schemaVersion: 1, source: 'packages/sdk/src/index.ts', methods };
}

export async function generateReference(repositoryRoot: string): Promise<void> {
  const [http, config, sdk, cli, processors] = await Promise.all([
    httpReference(repositoryRoot),
    configReference(repositoryRoot),
    sdkReference(repositoryRoot),
    readJson(resolve(repositoryRoot, 'docs/reference/generated/cli.json'), true),
    readJson(resolve(repositoryRoot, 'docs/reference/generated/processor-catalog.json'), true),
  ]);
  for (const [name, value] of Object.entries({ http, config, sdk, cli, processors })) {
    if (value.schemaVersion !== 1) throw new Error(`${name} reference uses unsupported schema version`);
    await writeFile(
      resolve(repositoryRoot, `site/src/generated/${name}-reference.json`),
      `${JSON.stringify(value, null, 2)}\n`,
      'utf8',
    );
  }
}

if (import.meta.main) {
  const repositoryRoot = resolve(import.meta.dir, '../..');
  await generateReference(repositoryRoot);
  console.log('generated HTTP, configuration, SDK, CLI, and processor reference inputs');
}
