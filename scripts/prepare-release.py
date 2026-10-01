#!/usr/bin/env python3
"""Prepare a coordinated version update without publishing or creating tags."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import tomllib

from lib.release_common import ReleaseError, git, metadata, version, version_order


def prepare(requested, output):
    requested = version(requested)
    old = metadata()
    if version_order(requested) <= version_order(old):
        raise ReleaseError(f"{requested} must be newer than {old}")
    if git("status", "--porcelain", "--untracked-files=no"):
        raise ReleaseError("release preparation requires a clean tracked checkout")
    base = git("rev-parse", "HEAD")
    files = {}

    def update(path, transform):
        current = files.get(path, Path(path).read_text())
        changed = transform(current)
        if changed != current:
            files[path] = changed

    def replace_once(text, before, after):
        if text.count(before) != 1:
            raise ReleaseError(f"expected one occurrence of {before!r}")
        return text.replace(before, after, 1)

    update("Cargo.toml", lambda text: replace_once(text, f'version = "{old}"', f'version = "{requested}"'))
    manifests = [path for path in git("ls-files", "**/Cargo.toml").splitlines()
                 if path.startswith(("crates/", "examples/"))]
    names = {tomllib.loads(Path(path).read_text())["package"]["name"] for path in manifests}
    def internal_requirements(text):
        for name in names:
            pattern = r'(?m)^(' + re.escape(name) + r'\s*=\s*\{(?=[^}\n]*\bpath\s*=)[^}\n]*\bversion\s*=\s*)"=' + re.escape(old) + r'"'
            text = re.sub(pattern, lambda match: match[1] + f'"={requested}"', text)
        document = tomllib.loads(text)
        sections = [document, *document.get("target", {}).values()]
        for section in sections:
            for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
                for name, dependency in section.get(kind, {}).items():
                    if (isinstance(dependency, dict) and dependency.get("package", name) in names
                            and "path" in dependency and dependency.get("version") == f"={old}"):
                        raise ReleaseError("unsupported internal version declaration; review its manifest before preparing")
        return text
    for path in manifests:
        update(path, internal_requirements)

    def lockfile(text):
        blocks = text.split("[[package]]")
        for index, block in enumerate(blocks[1:], 1):
            package = tomllib.loads("[[package]]" + block)["package"][0]
            if package["name"] in names and "source" not in package:
                if package["version"] != old:
                    raise ReleaseError("workspace lockfile versions are inconsistent")
                blocks[index] = block.replace(f'version = "{old}"', f'version = "{requested}"', 1)
            for name in names:
                blocks[index] = blocks[index].replace(f'"{name} {old}"', f'"{name} {requested}"')
        return "[[package]]".join(blocks)

    update("Cargo.lock", lockfile)
    # The license notices list each workspace crate as "name version — repository",
    # and CI regenerates them from Cargo.lock to check the committed copy.
    workspace_entry = r"(?m)^(" + "|".join(re.escape(name) for name in sorted(names)) + r") " + re.escape(old) + " — "
    update("THIRD_PARTY_LICENSES.txt", lambda text: re.sub(workspace_entry, lambda match: f"{match[1]} {requested} — ", text))
    update("packages/sdk/package.json", lambda text: replace_once(text, f'"version": "{old}"', f'"version": "{requested}"'))
    update("packages/sdk/src/index.ts", lambda text: replace_once(text, f'SDK_VERSION = "{old}"', f'SDK_VERSION = "{requested}"'))
    # Only current install instructions change. Historical migration notes,
    # processor versions, schema numbers, and the API contract remain intact.
    # A whole version token only: 0.1.0 must not rewrite 10.1.0 or 0.1.0-rc.1.
    current_version = r"(?<![\d.])" + re.escape(old) + r"(?!\w|\.\d|-rc\.)"
    for path in ("README.md", "docs/getting-started/install.mdx", "skills/leani/references/processors.md"):
        update(path, lambda text: re.sub(current_version, requested, text))
    documents = ["packages/sdk/README.md", *git("ls-files", "docs/*.md", "docs/*.mdx", "docs/**/*.md", "docs/**/*.mdx").splitlines()]
    install = r"(\b(?:bun\s+add|npm\s+(?:install|i)|pnpm\s+add|yarn\s+add)\b[^\n`]*?@smart-byte/leani-sdk@)" + re.escape(old) + r"(?=[\s`'\"]|$)"
    for path in documents:
        update(path, lambda text: re.sub(install, lambda match: match[1] + requested, text))
    date = datetime.now(timezone.utc).date().isoformat()
    def changelog(text):
        if text.count("## [Unreleased]") != 1:
            raise ReleaseError("expected one occurrence of '## [Unreleased]'")
        before, entries = text.split("## [Unreleased]", 1)
        previous = re.search(r"(?m)^## \[", entries)
        boundary = previous.start() if previous else len(entries)
        current = entries[:boundary].replace("bun add @smart-byte/leani-sdk@<version>", f"bun add @smart-byte/leani-sdk@{requested}")
        return before + f"## [Unreleased]\n\n## [{requested}] - {date}" + current + entries[boundary:]
    update("CHANGELOG.md", changelog)
    if "-rc." not in requested:
        update("README.md", lambda text: text.replace("Status: pre-release preview", "Status: stable release", 1))
    # Compute everything before writing, so a malformed manifest cannot leave
    # a half-prepared checkout.
    for path, contents in files.items():
        Path(path).write_text(contents)
    payload = {"schema": 1, "base_sha": base, "version": requested, "files": files}
    Path(output).write_text(json.dumps(payload, indent=2) + "\n")
    print(f"Prepared {requested}: {len(files)} files; upgrade instructions require review")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        prepare(args.version, args.output)
    except (ReleaseError, KeyError, ValueError, OSError) as error:
        print(f"release preparation: {error}", file=sys.stderr)
        sys.exit(1)
