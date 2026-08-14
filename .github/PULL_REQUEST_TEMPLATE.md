## Outcome

Describe the user-visible or operational result.

## Contract and risk

- [ ] Durable/API/SDK/RPC changes include compatibility or an explicit pre-release break.
- [ ] Trust, finality, ordering, retention, and resource-bound claims remain accurate.
- [ ] No secrets, private endpoints, database exports, or peer identities are included.

## Verification

- [ ] `cargo fmt --all --check`
- [ ] `cargo test --workspace --locked`
- [ ] `cargo clippy --workspace --all-targets --locked -- -D warnings`
- [ ] SDK type, unit, build, and packed-package checks when relevant
- [ ] Documentation links and deployment contracts when relevant

