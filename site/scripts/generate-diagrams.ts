// Renders each diagram source into light and dark SVGs. GitHub picks one with
// <picture> and the docs swap them with Starlight's theme, so one source keeps
// the geometry in one place. `--check` fails when a committed render is stale.
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

const siteRoot = resolve(import.meta.dir, '..');
const sourceDir = resolve(siteRoot, 'src/assets/diagrams');
const publicDir = resolve(siteRoot, 'public/diagrams');

// Starlight's docs palette (src/styles/docs.css) plus DeliverySequence's amber.
const palettes = {
  light: {
    ink: '#101713', dim: '#536b5e', faint: '#8ca396', edge: '#cbd8d0', panel: '#f1f6f3',
    accent: '#08763c', accentLow: '#d8f8e5', amber: '#8a5a00', amberEdge: '#dcbd7a', amberLow: '#fcf5e5',
  },
  dark: {
    ink: '#e6f0ea', dim: '#8fa89a', faint: '#536b5e', edge: '#2b3c33', panel: '#0e1411',
    accent: '#3ee07f', accentLow: '#123b26', amber: '#edcb89', amberEdge: '#5c4a2a', amberLow: '#16140e',
  },
};

const style = (p: (typeof palettes)['light']) => `<style>
svg{font-family:'JetBrains Mono',ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px}
text{fill:${p.ink}}.d{fill:${p.dim}}.f{fill:${p.faint}}.a{fill:${p.accent}}.h{fill:${p.amber}}
.k{font-size:10px;letter-spacing:.08em}.s{font-size:11px}.b{font-weight:700}
.box{fill:${p.panel};stroke:${p.edge}}.boxa{fill:${p.accentLow};stroke:${p.accent}}.boxh{fill:${p.amberLow};stroke:${p.amberEdge}}
.ln{fill:none;stroke:${p.faint};stroke-width:1.25}.lna{fill:none;stroke:${p.accent};stroke-width:1.5}.lnh{fill:none;stroke:${p.amber};stroke-width:1.5}
.rule{stroke:${p.edge};stroke-dasharray:2 4}.dash{stroke-dasharray:4 4}
.open{fill:none;stroke:${p.faint};stroke-dasharray:4 3}.opena{fill:none;stroke:${p.accent};stroke-dasharray:4 3}
.mk{fill:${p.faint}}.mka{fill:${p.accent}}.mkh{fill:${p.amber}}.hatch{stroke:${p.amberEdge};stroke-width:3}
</style>`;

const check = process.argv.includes('--check');
const stale: string[] = [];
await mkdir(publicDir, { recursive: true });
for (const file of (await readdir(sourceDir)).filter((name) => name.endsWith('.svg'))) {
  const source = await readFile(resolve(sourceDir, file), 'utf8');
  for (const [theme, palette] of Object.entries(palettes)) {
    const target = resolve(publicDir, file.replace(/\.svg$/, `-${theme}.svg`));
    const rendered = source.replace(/(<svg[^>]*>)/, `$1\n${style(palette)}`);
    if (!check) await writeFile(target, rendered);
    else if ((await readFile(target, 'utf8').catch(() => '')) !== rendered) stale.push(target);
  }
}
if (stale.length) {
  console.error(`stale diagram renders; run bun run diagrams:update\n${stale.join('\n')}`);
  process.exit(1);
}
console.log(check ? 'diagram renders are current' : 'rendered light and dark diagrams');
