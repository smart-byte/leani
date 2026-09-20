#!/usr/bin/env python3
"""Exercise failures that must prevent publishing a Cargo archive."""

import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parent.parent
VERSION = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
COMMIT = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
PACKAGES = ["leani-primitives", "leani-processor-api", "leani-source-api", "leani-testkit"]
CASES = ["valid", "dirty", "wrong-commit", "wrong-license", "private-content", "traversal", "symlink", "missing-crate"]

for scenario in CASES:
    with tempfile.TemporaryDirectory(prefix="leani-crate-test-") as directory:
        for package in PACKAGES:
            if scenario == "missing-crate" and package == PACKAGES[0]:
                continue
            root = f"{package}-{VERSION}"
            files = {
                "LICENSE": (ROOT / "LICENSE").read_bytes(),
                "Cargo.toml": f'[package]\nname = "{package}"\nlicense = "MIT"\n'.encode(),
                ".cargo_vcs_info.json": json.dumps({"git": {"sha1": COMMIT}}).encode(),
                "src/lib.rs": b"pub fn example() {}\n",
            }
            if package == PACKAGES[0]:
                if scenario == "dirty":
                    files[".cargo_vcs_info.json"] = json.dumps({"git": {"sha1": COMMIT, "dirty": True}}).encode()
                elif scenario == "wrong-commit":
                    files[".cargo_vcs_info.json"] = json.dumps({"git": {"sha1": "f" * 40}}).encode()
                elif scenario == "wrong-license":
                    files["LICENSE"] = b"unexpected license"
                elif scenario == "private-content":
                    files["src/lib.rs"] = ("/" + "/".join(["home", "private-user", "checkout", "src"])).encode()
                elif scenario == "traversal":
                    files["../../escape"] = b"must not be extracted"
            with tarfile.open(Path(directory) / f"{root}.crate", "w:gz") as archive:
                for name, contents in files.items():
                    member = tarfile.TarInfo(f"{root}/{name}")
                    member.size = len(contents)
                    if scenario == "symlink" and package == PACKAGES[0] and name == "src/lib.rs":
                        member.type = tarfile.SYMTYPE
                        member.linkname = "../../outside"
                    archive.addfile(member, io.BytesIO(contents))
        result = subprocess.run([sys.executable, str(ROOT / "scripts/check-crate-packages.py"), directory], capture_output=True, text=True)
        assert (result.returncode == 0) == (scenario == "valid"), (scenario, result.stdout, result.stderr)
        print(f"PASS {scenario}")
print(f"{len(CASES)} crate archive scenarios passed")
