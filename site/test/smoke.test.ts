import { expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';

test('interactive widgets use scoped selectors and semantic copy controls', () => {
  const showcase = readFileSync(
    new URL('../src/components/Showcase.astro', import.meta.url),
    'utf8',
  );
  const quickstart = readFileSync(
    new URL('../src/components/Quickstart.astro', import.meta.url),
    'utf8',
  );
  const hero = readFileSync(new URL('../src/components/Hero.astro', import.meta.url), 'utf8');

  expect(showcase).toContain("document.getElementById('processors')");
  expect(showcase).not.toContain("document.querySelector<HTMLElement>('[role=\"tablist\"]')");
  expect(quickstart).toContain('button type="button"');
  expect(quickstart).toContain('aria-live="polite"');
  expect(quickstart).not.toContain("addEventListener('click', () => navigator.clipboard");
  expect(hero).toContain('e.preventDefault()');
  expect(hero).not.toContain('api.github.com');
});

test('the share card image matches its declared size', async () => {
  const { SOCIAL_IMAGE } = await import('../src/lib/social');
  const png = readFileSync(new URL(`../public${SOCIAL_IMAGE.path}`, import.meta.url));
  // A PNG's IHDR chunk stores width and height as big-endian u32 at bytes 16 and 20.
  expect(png.subarray(1, 4).toString()).toBe('PNG');
  expect([png.readUInt32BE(16), png.readUInt32BE(20)]).toEqual([SOCIAL_IMAGE.width, SOCIAL_IMAGE.height]);
});
