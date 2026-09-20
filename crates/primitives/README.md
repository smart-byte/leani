# leani-primitives

Shared Ethereum chain and data types for [Leani](https://github.com/smart-byte/leani):
block references, normalized frames, source capabilities, provenance, cursors,
and versioned durable encodings.

This is a release-candidate API. Keep the Leani libraries on matching versions
when developing a processor or source.

```rust
use leani_primitives::{BlockNumber, ChainId};

let chain = ChainId(1);
let block = BlockNumber(20_000_000);
```

See the [Leani documentation](https://leani.dev/docs/) for the node and
processor contracts.

## License

[MIT](LICENSE). Dependencies retain their own licenses.
