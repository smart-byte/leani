// Compile a processor written in TypeScript, with this SDK beside it, into a
// Leani Wasm module:
//
//   node build.mjs <processor.ts> <sdk directory> <out.wasm>
//
// The compiler reads only the processor, as `assembly/index.ts`, and the
// SDK, as `assembly/leani/`: no other file, configuration, or transform, so
// a node can build a source it was sent. Prints one JSON line:
// `{ "built": bool, "diagnostics": [{ level, message, line, column, rendered }] }`,
// where `line` and `column` point into the processor's source, and are null
// for other files. Exits 1 when it did not build.

import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import asc from "assemblyscript/asc";

const [entry, sdk, out] = process.argv.slice(2);
if (!entry || !sdk || !out) {
  console.error("usage: node build.mjs <processor.ts> <sdk directory> <out.wasm>");
  process.exit(2);
}

const ENTRY = "assembly/index.ts";
const SDK = "assembly/leani/";
// The options every Leani module builds with: the stub runtime, as each call
// gets a fresh instance, and errors reported through the node.
const OPTIONS = ["--runtime", "stub", "--use", "abort=assembly/leani/ffi/abort", "--optimizeLevel", "3", "--shrinkLevel", "1", "--noColors"];

const sources = new Map();

async function read(name, baseDir) {
  const file = path.posix.normalize(path.posix.join(baseDir || ".", name));
  let real = null;
  if (file === ENTRY) real = entry;
  else if (file.startsWith(SDK) && file.endsWith(".ts") && !file.slice(SDK.length).includes("/")) {
    real = path.join(sdk, file.slice(SDK.length));
  }
  if (real === null) return null;
  try {
    const text = await readFile(real, "utf8");
    sources.set(file, text);
    return text;
  } catch {
    return null;
  }
}

const diagnostics = [];
let binary = null;
const result = await asc.main([ENTRY, "--outFile", "module.wasm", ...OPTIONS], {
  readFile: read,
  writeFile: (name, contents) => {
    if (name === "module.wasm") binary = contents;
    return true;
  },
  listFiles: () => [],
  reportDiagnostic: (diagnostic) => {
    // AssemblyScript's categories: 0 pedantic, 1 info, 2 warning, 3 error.
    const level = diagnostic.category === 3 ? "error" : diagnostic.category === 2 ? "warning" : null;
    if (!level) return;
    const file = diagnostic.range?.source?.normalizedPath ?? null;
    const own = file === ENTRY;
    // The SDK's own warnings are not the processor's to fix.
    if (level === "warning" && !own) return;
    const where = position(file, diagnostic.range?.start ?? 0);
    diagnostics.push({
      level,
      message: own || !file ? diagnostic.message : `${diagnostic.message} (in ${file})`,
      line: own ? where.line : null,
      column: own ? where.column : null,
      rendered: render(level, diagnostic, file, where),
    });
  },
});

let built = !result.error && binary !== null;
if (built) {
  try {
    await writeFile(out, binary);
  } catch (error) {
    // The code only: the message names a path on the building host.
    const message = `cannot write the module: ${error.code ?? "write failed"}`;
    diagnostics.push({ level: "error", message, line: null, column: null, rendered: message });
    built = false;
  }
}
if (!built && !diagnostics.some((diagnostic) => diagnostic.level === "error")) {
  diagnostics.push({ level: "error", message: String(result.error?.message ?? result.error ?? "the compiler failed"), line: null, column: null, rendered: result.stderr.toString() });
}
console.log(JSON.stringify({ built, diagnostics }));
process.exitCode = built ? 0 : 1;

function position(file, offset) {
  const text = sources.get(file) ?? "";
  const before = text.slice(0, offset);
  const line = before.split("\n").length;
  return { line, column: offset - before.lastIndexOf("\n"), text: text.split("\n")[line - 1] ?? "" };
}

function render(level, diagnostic, file, where) {
  const head = `${level} AS${diagnostic.code}: ${diagnostic.message}`;
  if (!file) return head;
  // The compiler's own library: no source here to point into.
  if (!sources.has(file)) return `${head}\n --> ${file}`;
  const number = String(where.line);
  const pad = " ".repeat(number.length);
  const span = Math.max(1, Math.min((diagnostic.range?.end ?? 0) - (diagnostic.range?.start ?? 0), where.text.length - where.column + 1));
  return `${head}\n${pad}--> ${file}:${where.line}:${where.column}\n${pad} |\n${number} | ${where.text}\n${pad} | ${" ".repeat(where.column - 1)}${"^".repeat(span)}`;
}
