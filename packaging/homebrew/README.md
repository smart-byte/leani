# Homebrew release preparation

The `Prepare release` workflow produces `leani.rb` from the exact checksums of
the four release archives. It does not update a tap automatically.

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

The tap write token is intentionally not stored in this repository. Automating
the final tap pull request is safe only after the tap exists and its branch
protection rules are enabled.
