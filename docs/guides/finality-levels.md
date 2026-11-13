---
title: Choose a finality level
description: Compare attested, confirmed, finalized, and settled on Ethereum and on rollups, and make a processor or subscription wait for the one you need.
section: guides
order: 14
audience:
  - app-developer
  - operator
status: preview
---

Leani labels every block with the level it has reached. Waiting for a higher
level costs time and buys certainty, so pick the lowest one your application
can act on.

| Level | An Ethereum block | A rollup block |
|---|---|---|
| `unattested` | At least two peers served the header and it passes Leani's header rules, ~2–5 s | Delivered, not yet signed |
| `attested` | The sync committee signed it, ~15 s | The chain's sequencer signed it, ~1 s |
| `confirmed` | Ethereum's Fast Confirmation Rule confirmed it, as the consensus nodes you name report, ~15 s | Not available |
| `finalized` | Casper FFG finality, ~13–19 min | Its batch is in a finalized Ethereum block, 15–30 min |
| `settled` | Not applicable | Ethereum accepted a claim covering it, 5–7 days |

[Coverage, finality, and reorgs](/docs/concepts/coverage-finality-and-reorgs/)
has each level's exact rule and what can undo it.

## Ask for a level

A processor publishes from the level its `publish` names. Lower blocks stay in
its state, unpublished, until they rise:

```toml
[[processors]]
# id, instance, version ...
publish = "confirmed"
```

The CLI takes the same names:

```bash
leani subscribe blocks --finality confirmed
```

A consumer that reads from `unattested` gets every rise as its own operation
(`attested`, `confirmed`, `finalized`, `settled`) and an `undo` when a block
leaves the chain, as in [Follow a rollup](/docs/guides/follow-a-rollup/).

## `confirmed`: Ethereum's fast confirmation

`finalized` takes 13–19 minutes. `confirmed` arrives with `attested`, a slot
or two after the block, and holds unless the rule's assumptions fail: more
than a quarter of stake adversarial, or attestations delayed past their slot.
Under worse conditions the rule stalls instead of confirming.

Leani does not compute the rule. It reads `fast_confirmation` events from the
consensus nodes that `[confirmation]` names, and counts an event only for a
block this node verified as attested with at least 384 of 512 sync-committee
signers:

```toml
[confirmation]
endpoints = ["https://lodestar-mainnet.chainsafe.io/"]
minimum_agreement = 1
```

Compact Ethereum Mainnet configurations name that public node. For
production, run your own consensus node with Fast Confirmation enabled and
name it here. An advanced configuration without the section, or any with
`endpoints = []`, has no `confirmed` level and refuses `publish = "confirmed"`.
While the section is configured, JSON-RPC `safe` names the newest confirmed
block.

`confirmed` never prunes, compacts, or ends anything: only `finalized` does.
It exists on Ethereum only. A rollup block goes from `attested` to
`finalized`. Its batch usually reaches a confirmed Ethereum block within
minutes, which [Finality, live](https://leani.dev/finality/) shows as a milestone, but Leani
reports no level for it.
