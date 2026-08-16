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
