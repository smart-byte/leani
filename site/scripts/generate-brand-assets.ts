import { copyFile, mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import sharp from 'sharp';

const siteRoot = resolve(import.meta.dir, '..');
const brandDir = resolve(siteRoot, 'src/assets/brand');
const publicDir = resolve(siteRoot, 'public');
// The tile icon is the raster source, and it keeps the old /leani-mark.svg URL working.
const svg = await readFile(resolve(brandDir, 'leani-icon.svg'));
const published: Array<[source: string, target: string]> = [
  ['leani-icon.svg', 'leani-mark.svg'],
  ['leani-mark-on-dark.svg', 'leani-mark-on-dark.svg'],
  ['leani-mark-on-light.svg', 'leani-mark-on-light.svg'],
  ['leani-wordmark-on-dark.svg', 'leani-wordmark-on-dark.svg'],
  ['leani-wordmark-on-light.svg', 'leani-wordmark-on-light.svg'],
  ['favicon.svg', 'favicon.svg'],
];

await mkdir(publicDir, { recursive: true });
await Promise.all(published.map(([from, to]) => copyFile(resolve(brandDir, from), resolve(publicDir, to))));

async function renderPng(size: number): Promise<Buffer> {
  return sharp(svg, { density: 384 })
    .resize(size, size, { fit: 'fill' })
    .png({ compressionLevel: 9, palette: true })
    .toBuffer();
}

async function writePng(name: string, size: number): Promise<void> {
  const path = resolve(publicDir, name);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, await renderPng(size));
}

function encodeIco(images: Array<{ size: number; png: Buffer }>): Buffer {
  const headerSize = 6;
  const entrySize = 16;
  const tableSize = headerSize + entrySize * images.length;
  const header = Buffer.alloc(tableSize);

  header.writeUInt16LE(0, 0);
  header.writeUInt16LE(1, 2);
  header.writeUInt16LE(images.length, 4);

  let offset = tableSize;
  images.forEach(({ size, png }, index) => {
    const entry = headerSize + entrySize * index;
    header.writeUInt8(size === 256 ? 0 : size, entry);
    header.writeUInt8(size === 256 ? 0 : size, entry + 1);
    header.writeUInt8(0, entry + 2);
    header.writeUInt8(0, entry + 3);
    header.writeUInt16LE(1, entry + 4);
    header.writeUInt16LE(32, entry + 6);
    header.writeUInt32LE(png.length, entry + 8);
    header.writeUInt32LE(offset, entry + 12);
    offset += png.length;
  });

  return Buffer.concat([header, ...images.map(({ png }) => png)]);
}

await Promise.all([
  writePng('apple-touch-icon.png', 180),
  writePng('leani-social-avatar.png', 1024),
]);

const faviconImages = await Promise.all(
  [16, 32, 48].map(async (size) => ({ size, png: await renderPng(size) })),
);
await writeFile(resolve(publicDir, 'favicon.ico'), encodeIco(faviconImages));

console.log('generated Leani favicon, app icon, and social avatar assets');
