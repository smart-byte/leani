---
title: Use Leani with AI agents
description: Install the Leani agent skill, point agents at the llms.txt docs, and script the CLI safely.
section: guides
order: 55
audience:
  - app-developer
  - operator
status: preview
---

Coding agents such as Claude Code and Codex can run Leani for you. Give them
the Leani skill, which teaches the commands, the trust and finality rules, and
how to configure processors, and point them at the machine-readable docs.

## Install the skill

In Claude Code, add this repository as a plugin marketplace and install the
plugin:

```text
/plugin marketplace add smart-byte/leani
/plugin install leani@leani
```

The skill follows the open [Agent Skills](https://agentskills.io) format. For
other agents, copy the `skills/leani` directory of a Leani checkout into the
skills directory your agent reads, such as `.agents/skills/` in your project.
Keep the copy on the same release as your `leani` binary.

## Give agents the docs

The site publishes its documentation as plain text for language models:

- [`/llms.txt`](https://leani.dev/llms.txt): a short briefing and an index of
  the files below.
- One file per section, such as
  [`/_llms-txt/reference.txt`](https://leani.dev/_llms-txt/reference.txt).
- [`/llms-full.txt`](https://leani.dev/llms-full.txt): everything in one file.

## What agents should know about the CLI

The skill covers these rules; they matter for any script too:

- `leani subscribe --json` prints NDJSON; progress and logs go to stderr.
- `subscribe` follows until interrupted. Use `--once --timeout 5m` for one
  result: `--once` returns the first row that meets `--finality` (default
  `included`), and an unverified `preview` only with `--finality preview`.
- Checkpoint trust needs a person's approval. Without a terminal, `init` and
  `subscribe` stop until you rerun them with `--yes`; an agent should show you
  the checkpoint first.
- Exit codes tell outcomes apart without parsing stderr; see the
  [command-line reference](/docs/reference/cli/#exit-codes).
