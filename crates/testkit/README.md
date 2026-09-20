# leani-testkit

Deterministic processor and source fixtures for
[Leani](https://github.com/smart-byte/leani).

The testkit provides generated and scripted history sources, live and
finality events, fixture frames, reference processors, an in-memory reducer,
and frame-conformance helpers. Tests can exercise processor behavior without
running a node or connecting to Ethereum.

Use this crate as a development dependency alongside matching versions of
`leani-processor-api`, `leani-source-api`, and `leani-primitives`. These are
release-candidate APIs.

See [build a custom node](https://leani.dev/docs/guides/custom-processor/)
for an example processor and tests.

## License

[MIT](LICENSE). Dependencies retain their own licenses.
