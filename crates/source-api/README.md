# leani-source-api

Historical, live, and finality source contracts for
[Leani](https://github.com/smart-byte/leani).

This crate defines source descriptors, requests, budgets, streams, planning,
verification policies, telemetry, and frame-conformance helpers. Concrete
network and archive readers live in separate crates.

This is a release-candidate API. Use matching versions of `leani-primitives`
and `leani-testkit` when implementing and testing a source.

See the [Leani documentation](https://leani.dev/docs/) for source contracts
and integration guidance.

## License

[MIT](LICENSE). Dependencies retain their own licenses.
