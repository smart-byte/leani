#!/usr/bin/env python3
"""Inspect the exact Cargo dry-run archives before allowing an upload."""

import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parent.parent
PACKAGES = {"leani-primitives", "leani-processor-api", "leani-source-api", "leani-testkit"}


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: check-crate-packages.py ARCHIVE_DIRECTORY")
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    expected = {f"{name}-{version}.crate" for name in PACKAGES}
    archives = list(Path(sys.argv[1]).glob("*.crate"))
    if {archive.name for archive in archives} != expected:
        sys.exit("expected exactly the four authoring library archives at the workspace version")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    license_text = (ROOT / "LICENSE").read_bytes()
    for archive in sorted(archives):
        package_root = archive.name.removesuffix(".crate")
        with tempfile.TemporaryDirectory(prefix="leani-crate-inspect-") as directory:
            destination = Path(directory)
            with tarfile.open(archive, "r:gz") as tar:
                for member in tar.getmembers():
                    path = PurePosixPath(member.name)
                    if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != package_root:
                        sys.exit("invalid path inside crate archive")
                    if not member.isfile():
                        sys.exit("unexpected non-regular file inside crate archive")
                    output = destination.joinpath(*path.parts)
                    output.parent.mkdir(parents=True, exist_ok=True)
                    output.write_bytes(tar.extractfile(member).read())
            package = destination / package_root
            if (package / "LICENSE").read_bytes() != license_text:
                sys.exit("crate must carry the repository MIT license")
            vcs = json.loads((package / ".cargo_vcs_info.json").read_text())["git"]
            if vcs["sha1"] != commit or vcs.get("dirty", False):
                sys.exit("crate must come from the clean release commit")
            manifest = tomllib.loads((package / "Cargo.toml").read_text())
            if manifest["package"]["license"] != "MIT":
                sys.exit("unexpected crate license metadata")
            subprocess.run(["ruby", str(ROOT / "scripts/check-artifact-privacy.rb"), str(package)], check=True)
        print(f"Verified {archive.name}")


if __name__ == "__main__":
    main()
