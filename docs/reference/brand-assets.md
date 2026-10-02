---
title: Brand assets
description: Download the Leani mark, wordmark, avatar, and share card, with the rules, colors, and fonts that go with them.
section: reference
order: 90
audience:
  - app-developer
  - operator
  - contributor
---

Use these files when you write about Leani, link to it, or list it in a
directory or app store.

| Asset | Size | Use it for |
|---|---|---|
| [Wordmark on dark](/leani-wordmark-on-dark.svg) | SVG | Headers, slides, and diagrams on dark backgrounds |
| [Wordmark on light](/leani-wordmark-on-light.svg) | SVG | Headers, slides, and diagrams on light backgrounds |
| [Mark on dark](/leani-mark-on-dark.svg) | SVG | Small spaces on dark backgrounds, where the wordmark doesn't fit |
| [Mark on light](/leani-mark-on-light.svg) | SVG | Small spaces on light backgrounds |
| [App icon](/leani-mark.svg) | SVG | The mark on its dark tile, for app icons and tiles |
| [Avatar](/leani-social-avatar.png) | 1024×1024 PNG | Profile pictures, app icons, directory listings |
| [Share card](/og-image.png) | 1200×630 PNG | Link previews; the site's pages already use it |

<img src="/leani-social-avatar.png" width="160" height="160" alt="The Leani avatar: a white lowercase i leaning to the right, with a green square for its dot, on a dark tile" />

![The Leani share card: "Keep only the Ethereum data your app needs." above the acquire, validate, reduce, deliver pipeline, which thins a field of dots down to one green block](/og-image.png)

## The mark

The mark is a lowercase i that leans 12°, one degree for each second of an
Ethereum slot. Its dot is a square block: the one block an application keeps. The
wordmark spells `leani` in a geometric lowercase and ends in the same i.

- Keep the 12° lean, the square dot, and the proportions. Don't redraw the
  wordmark in a font.
- Use the dark-background files on dark surfaces and the light-background files
  on light ones. Only the app icon and avatar carry a tile.
- Leave clear space around the mark at least as wide as its dot.
- Below 24 pixels, use the mark rather than the wordmark. The favicon is a
  heavier cut of the mark for 16 and 32 pixels.
- Write the name as Leani in prose.

## The epoch field

The supporting pattern is one epoch's 32 slots: a 6×6 grid of dots without its
corners. One slot can be kept as a green block. The share card and the site use
it as background texture and to show data being reduced.

- Use the field behind headlines and illustrations, never inside the mark or
  next to it.
- Don't use it below 48 pixels or behind running text.

## Colors and type

| Role | Color |
|---|---|
| Background | `#0b0f0d` |
| Text | `#e6f0ea` |
| Accent green | `#3ee07f` |
| Accent green on light backgrounds | `#12a150` |
| Text on light backgrounds | `#0b0f0d` |

Body text uses Inter; code and commands use JetBrains Mono. The wordmark is
drawn, not typeset.

## Source files

The SVGs live in `site/src/assets/brand`. Run `bun run brand:update` in `site/`
to publish them and to regenerate the favicons and avatar from the app icon.
The share card is rendered from `site/scripts/og-card.html`; its header comment
has the command.
