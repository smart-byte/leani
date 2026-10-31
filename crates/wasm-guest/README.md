# leani-wasm-guest

Write a [Leani](https://github.com/smart-byte/leani) processor in Rust, compile
it to `wasm32-unknown-unknown`, and run it in a stock Leani node without
rebuilding the node: configure it as a `wasm` processor, or run it ad hoc with
`leani processor run`.

See the [Wasm processors guide](https://leani.dev/docs/guides/wasm-processors/)
for the processor contract, limits, and examples; `src/ffi.rs` and
`src/abi.rs` document the ABI.

## License

[MIT](LICENSE). Dependencies retain their own licenses.
