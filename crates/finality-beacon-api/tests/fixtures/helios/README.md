# Helios mainnet light-client fixtures

Real, signed Ethereum mainnet light-client responses in the standard Beacon API
JSON format. The finality tests of `leani-finality-beacon-api` and
`leani-finality-consensus-p2p` verify them end to end with the pinned Helios
verifier.

- Source: <https://github.com/a16z/helios>
- Revision: `204c998a927348e1c000a664f08d5b37b1b0d924` (the revision pinned for
  `helios-consensus-core` in the workspace `Cargo.toml`)
- Path in the source repository: `ethereum/testdata/`
- Files are copied unmodified:

| File | SHA-256 | Content |
|---|---|---|
| `bootstrap.json` | `6a12276c898200089cf8751a938497fd6eb9a711203a385471994d275459a711` | Bootstrap for block root `0x5afc212a7924789b2bc86acad3ab3a6ffb1f6e97253ea50bee7f4f51422c9275`, slot 7,069,376 (sync-committee period 862) |
| `updates.json` | `c972809bb985889cf51019fe6f6fae764d387e511d1bf47c8e5a6b7b82cc3cdb` | Sync-committee updates for periods 862 to 867 |
| `finality.json` | `6579adb2c85f974b5d6b887d4f7e36613d0d37c5c9b0dfd7b4f9aff1cef49f43` | Finality update signed at slot 7,109,431, finalizing slot 7,109,344 (execution block 17,923,026) |
| `optimistic.json` | `b465fbccb939e7f4fcd1782463526e55b53d59b648283ac43e85c6a92240cdc1` | Optimistic update signed at slot 7,109,432; not used by the current tests |

## License

The files are distributed under the Helios MIT license:

```text
MIT License

Copyright (c) 2022-2026 a16z

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
