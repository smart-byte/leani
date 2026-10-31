# leani-wasm-guest for TypeScript

Write a [Leani](https://github.com/smart-byte/leani) processor in TypeScript,
as [AssemblyScript](https://www.assemblyscript.org) compiles it, and run it in
a stock Leani node without rebuilding the node: configure it as a `wasm`
processor, or run it ad hoc with `leani processor run`.

It implements the same ABI as the Rust `leani-wasm-guest` crate:
`assembly/` is the SDK, and `build.mjs` compiles a processor with the options
every module needs, reading nothing but the processor and the SDK:

```bash
npm ci
node build.mjs path/to/index.ts assembly out.wasm
```

`leani processor create --language typescript` writes a processor package with a
copy of this SDK. See the
[Wasm processors guide](https://leani.dev/docs/guides/wasm-processors/#write-it-in-typescript).

## License

[MIT](LICENSE). AssemblyScript, the compiler, is Apache-2.0.
