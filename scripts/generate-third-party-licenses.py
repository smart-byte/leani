#!/usr/bin/env python3
"""Generate or verify THIRD_PARTY_LICENSES.txt for the leani binary.

cargo-about renders the license text and copyright notices of every crate
compiled into the node. Apache License 2.0 section 4(d) additionally requires
redistributing the NOTICE file of each dependency that ships one, which
cargo-about does not collect, so this script appends those files verbatim.

The committed file is the copy shipped in release archives, the container
image, and the Homebrew package. CI runs `--check` so it cannot drift from
Cargo.lock.
"""

import argparse
import difflib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "crates" / "node" / "Cargo.toml"
OUTPUT = ROOT / "THIRD_PARTY_LICENSES.txt"
RULE = "=" * 78
SUBRULE = "-" * 78


def find_cargo_about(explicit):
    candidate = explicit or os.environ.get("CARGO_ABOUT") or shutil.which("cargo-about")
    if candidate and Path(candidate).is_file():
        return candidate
    sys.exit(
        "cargo-about not found; install it with `sh scripts/install-cargo-about.sh DIRECTORY` "
        "and pass --cargo-about DIRECTORY/cargo-about"
    )


def render_licenses(cargo_about):
    with tempfile.TemporaryDirectory(prefix="leani-licenses-") as directory:
        output = Path(directory) / "licenses.txt"
        subprocess.run(
            [
                cargo_about,
                "generate",
                "--locked",
                "--fail",
                "--manifest-path",
                str(MANIFEST),
                "--config",
                str(ROOT / "about.toml"),
                str(ROOT / "about.hbs"),
                "--output-file",
                str(output),
            ],
            cwd=ROOT,
            check=True,
        )
        return output.read_text(encoding="utf-8")


def load_metadata():
    command = ["cargo", "metadata", "--format-version", "1", "--locked", "--manifest-path", str(MANIFEST)]
    return json.loads(subprocess.check_output(command, cwd=ROOT, text=True))


def shipped_packages(metadata):
    """Registry and Git packages reachable from the root through normal and
    build dependencies on every target. This mirrors about.toml, which ignores
    dev dependencies, and cargo-about's default of evaluating all targets."""
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    seen = set()
    pending = [metadata["resolve"]["root"]]
    while pending:
        identifier = pending.pop()
        if identifier in seen:
            continue
        seen.add(identifier)
        for dependency in nodes[identifier]["deps"]:
            if any(kind["kind"] in (None, "build") for kind in dependency["dep_kinds"]):
                pending.append(dependency["pkg"])
    shipped = [packages[identifier] for identifier in seen if packages[identifier]["source"] is not None]
    return sorted(shipped, key=lambda package: (package["name"], package["version"]))


def collect_notices(packages):
    """Yield (name, version, file name, text) for every NOTICE-style file in a
    shipped package root, in deterministic order."""
    for package in packages:
        package_root = Path(package["manifest_path"]).parent
        for path in sorted(package_root.iterdir(), key=lambda entry: entry.name):
            if path.is_file() and path.name.upper().startswith("NOTICE"):
                text = path.read_text(encoding="utf-8", errors="replace").strip()
                yield package["name"], package["version"], path.name, text


def render_notices(notices):
    lines = [
        RULE,
        "Dependency NOTICE files",
        RULE,
        "Apache License 2.0 section 4(d) requires the following notices to accompany",
        "redistributions of the dependencies that ship them.",
        "",
    ]
    for name, version, file_name, text in notices:
        lines.extend([SUBRULE, f"{name} {version} — {file_name}", SUBRULE, text, ""])
    return "\n".join(lines)


def generate(cargo_about):
    licenses = render_licenses(cargo_about).rstrip("\n")
    notices = list(collect_notices(shipped_packages(load_metadata())))
    if not notices:
        sys.exit("no dependency NOTICE files found; the dependency graph or registry cache is incomplete")
    return f"{licenses}\n\n{render_notices(notices)}"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--cargo-about", help="path to the cargo-about binary (default: $CARGO_ABOUT or PATH)")
    parser.add_argument("--check", action="store_true", help="fail if the committed file is stale instead of writing it")
    arguments = parser.parse_args()
    content = generate(find_cargo_about(arguments.cargo_about))
    if not arguments.check:
        OUTPUT.write_text(content, encoding="utf-8")
        print(f"wrote {OUTPUT.relative_to(ROOT)}")
        return
    current = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
    if current == content:
        print(f"{OUTPUT.relative_to(ROOT)} matches Cargo.lock")
        return
    diff = difflib.unified_diff(
        current.splitlines(), content.splitlines(), "committed", "generated", lineterm="", n=1
    )
    sys.stderr.write("\n".join(list(diff)[:60]) + "\n")
    sys.exit(f"{OUTPUT.relative_to(ROOT)} is stale; run scripts/generate-third-party-licenses.py and commit the result")


if __name__ == "__main__":
    main()
