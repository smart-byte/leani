import { describe, expect, test } from 'bun:test';
import { hlShell, hlToml } from '../src/lib/highlight';

describe('landing syntax palette', () => {
  test('shell highlights prompts, flags, strings, and numbers semantically', () => {
    const html = hlShell('$ leani backfill --from 100 --report "run.json"');
    expect(html).toContain('class="hl-prompt"');
    expect(html).toContain('class="hl-flag"');
    expect(html).toContain('class="hl-num"');
    expect(html).toContain('class="hl-str"');
  });

  test('toml highlights sections, keys, values, and comments semantically', () => {
    expect(hlToml('[[processors]]')).toContain('class="hl-kw"');
    expect(hlToml('start_block = 100 # bounded')).toContain('class="hl-key"');
    expect(hlToml('start_block = 100 # bounded')).toContain('class="hl-num"');
    expect(hlToml('start_block = 100 # bounded')).toContain('class="hl-com"');
    expect(hlToml('instance = "events"')).toContain('class="hl-str"');
  });
});
