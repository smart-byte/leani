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

## Changes and mutations

The store derives each block's undo from the changes a reducer emits, so the
changes must describe each key's net mutation in the block. Emit at most one
change per key: an `Upsert` whose payload is the key's final value in the
block, as written with `put` or `state_put`, or a `Delete` for a key the block
removed. A key the block created may carry a different change payload,
because its undo is a delete. A reducer that writes a key more than once in a
block emits one change, for the last write. The store rejects the apply of an
unfinalized block whose changes do not match its mutations.

## License

[MIT](LICENSE). Dependencies retain their own licenses.
