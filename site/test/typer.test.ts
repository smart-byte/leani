import { describe, expect, test } from 'bun:test';
import { buildTimeline, type TyperLine } from '../src/lib/typer';

describe('buildTimeline', () => {
  test('single line emits one step per character at charMs intervals', () => {
    const lines: TyperLine[] = [{ text: 'ab', charMs: 10 }];
    expect(buildTimeline(lines)).toEqual([
      { atMs: 10, lineIndex: 0, chars: 1 },
      { atMs: 20, lineIndex: 0, chars: 2 },
    ]);
  });

  test('delayMs shifts a line start after the previous line end', () => {
    const lines: TyperLine[] = [
      { text: 'a', charMs: 10 },
      { text: 'b', delayMs: 100, charMs: 10 },
    ];
    expect(buildTimeline(lines)).toEqual([
      { atMs: 10, lineIndex: 0, chars: 1 },
      { atMs: 120, lineIndex: 1, chars: 1 },
    ]);
  });

  test('charMs 0 renders whole line as one instant step', () => {
    const lines: TyperLine[] = [{ text: 'abc', charMs: 0, delayMs: 5 }];
    expect(buildTimeline(lines)).toEqual([{ atMs: 5, lineIndex: 0, chars: 3 }]);
  });

  test('default charMs is 14', () => {
    expect(buildTimeline([{ text: 'x' }])).toEqual([{ atMs: 14, lineIndex: 0, chars: 1 }]);
  });

  test('empty line still occupies time via delayMs and yields no char steps', () => {
    const lines: TyperLine[] = [
      { text: '', delayMs: 50 },
      { text: 'a', charMs: 10 },
    ];
    expect(buildTimeline(lines)).toEqual([{ atMs: 60, lineIndex: 1, chars: 1 }]);
  });
});
