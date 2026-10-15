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

One command installs it for Codex, Claude Code, Cursor, and other agents that
read [Agent Skills](https://agentskills.io):

```bash
npx skills add smart-byte/leani -g
```

`-g` installs it for your user, so every project sees it; leave it out to
install into the current project only. The command installs the skill from
`main`. To match an older `leani` binary, install from its release tag instead,
such as
`npx skills add https://github.com/smart-byte/leani/tree/v0.1.0-rc.6/skills/leani -g`.

Agents with plugin marketplaces can install it as a plugin instead. In Codex:

```bash
codex plugin marketplace add smart-byte/leani
codex plugin add leani@leani
```

In Claude Code:

```text
/plugin marketplace add smart-byte/leani
/plugin install leani@leani
```

The `leani` binary carries the same guide for its own release: `leani guide`
prints the overview and `leani guide processors` the processor reference. An
agent without the skill can read it there, and one with a newer skill than
the binary should prefer it.

Start a new agent session after installing. Then name Leani in the request,
such as "use leani to count the distinct addresses that deposited into Lido in
the last 7 days". The name alone is ambiguous on the web, so without the skill,
write `leani.dev` to give the agent a site to read.

## Questions become processors

The skill teaches agents to answer an Ethereum data question in this order:

1. Use a built-in processor when one produces the data, such as `evm-events`
   for decoded contract events.
2. Otherwise write a [custom processor](/docs/guides/custom-processor/). This
   is the normal way to extend Leani: it is needed for aggregates such as
   distinct counts or sums, for transaction values rather than logs, and for
   state across contracts.
3. Backfill the range, check coverage, and query the result.

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
  `unattested`; see [finality levels](/docs/concepts/coverage-finality-and-reorgs/#finality-levels-and-publish-modes)).
- Checkpoint trust needs a person's approval. Without a terminal, `init` and
  `subscribe` stop until you rerun them with `--yes`; an agent should show you
  the checkpoint first.
- Exit codes tell outcomes apart without parsing stderr; see the
  [command-line reference](/docs/reference/cli/#exit-codes).
