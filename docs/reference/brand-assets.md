---
title: Brand assets
description: Download the Leani logo, avatar, and share card, with the brand colors and fonts.
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
| [Logo mark](/leani-mark.svg) | SVG | Any size: headers, slides, diagrams |
| [Avatar](/leani-social-avatar.png) | 1024×1024 PNG | Profile pictures, app icons, directory listings |
| [Share card](/og-image.png) | 1200×630 PNG | Link previews; the site's pages already use it |

<img src="/leani-social-avatar.png" width="160" height="160" alt="The Leani avatar: a green L-shaped path from four blocks to one dot on a dark tile" />

![The Leani share card: "Keep only the Ethereum data your app needs." above the acquire, validate, reduce, deliver pipeline](/og-image.png)

## Name, colors, and type

Write the name as Leani in prose. The wordmark is lowercase `leani_` in
JetBrains Mono Bold, with `leani` in green and the underscore in the text color.

| Role | Color |
|---|---|
| Background | `#0b0f0d` |
| Text | `#e6f0ea` |
| Accent green | `#3ee07f` |
| Logo tile | `#242625` |

Body text uses Inter; code, commands, and the wordmark use JetBrains Mono.
Keep the mark's proportions and colors; it carries its own dark tile.

## Source files

The mark, favicons, and avatar are generated from
`site/src/assets/leani-mark.svg` with `bun run brand:update` in `site/`. The
share card is rendered from `site/scripts/og-card.html`; its header comment
has the command.
