# leani-processor-api

The processor extension contract for [Leani](https://github.com/smart-byte/leani).
It defines `Processor`, `ProcessorDescriptor`, deterministic deltas,
`ReducerTransaction`, domain changes, and lifecycle policies.

Use this crate with `leani-primitives` to implement a processor library, and
`leani-testkit` to test it with deterministic frames and an in-memory reducer.
The release-candidate libraries use matching, exact dependency versions.

See [build a custom node](https://leani.dev/docs/guides/custom-processor/) for
the implementation contract and a complete integration example. Running a
custom native processor currently requires compiling it into a custom Leani
node; this crate does not provide dynamic loading into the prebuilt binary.

## License

[MIT](LICENSE). Dependencies retain their own licenses.
