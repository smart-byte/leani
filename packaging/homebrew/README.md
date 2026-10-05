# Homebrew release preparation

The `Prepare release` workflow produces `leani.rb` from the exact checksums of
the four release archives. After the binary release is public, Coordinate
release opens a PR that copies it to `Formula/leani.rb` in
`smart-byte/homebrew-tap`; a maintainer merges it once the tap's tests pass.
See [Homebrew](../../docs/maintainers/releasing.md#homebrew) for the policy.

The coordinator writes to the tap with `RELEASE_CONTROL_TOKEN`, which must
include the tap with Contents and Pull requests read/write. Protect the tap's
`main` branch with a ruleset that requires a pull request and its passing
`install` checks, so the token can open PRs but never change the published
formula on its own.

Before the first preview release:

1. create the public `smart-byte/homebrew-tap` repository;
2. dispatch `Prepare release` with `publish=false` from the intended tag and
   inspect the release-candidate artifact;
3. run `brew style`, `brew audit --strict --online`, and `brew install --build-from-source`
   against the generated formula on Intel and Apple-silicon macOS;
4. dispatch again with `publish=true` from the signed tag and set
   `candidate_run_id` to the successful preparation run; this publishes the
   inspected archives without rebuilding them;
5. copy the generated formula to `Formula/leani.rb` in the tap, open a pull
   request, and repeat `brew test leani` against the public release URLs.
