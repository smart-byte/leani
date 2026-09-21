#!/usr/bin/env python3
"""Exercise the dependency selection and NOTICE collection behind THIRD_PARTY_LICENSES.txt."""

import importlib.util
from pathlib import Path
import sys
import tempfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
specification = importlib.util.spec_from_file_location("generator", ROOT / "scripts" / "generate-third-party-licenses.py")
generator = importlib.util.module_from_spec(specification)
specification.loader.exec_module(generator)

REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


def package(directory, name, version, source=REGISTRY, notices=()):
    root = directory / f"{name}-{version}"
    root.mkdir()
    (root / "Cargo.toml").write_text(f'[package]\nname = "{name}"\nversion = "{version}"\n')
    for file_name, text in notices:
        (root / file_name).write_text(text)
    return {"id": f"{name} {version}", "name": name, "version": version, "source": source, "manifest_path": str(root / "Cargo.toml")}


def dependency(target, *kinds):
    return {"pkg": target, "dep_kinds": [{"kind": kind} for kind in kinds]}


with tempfile.TemporaryDirectory(prefix="leani-notice-test-") as directory:
    directory = Path(directory)
    packages = [
        package(directory, "leani", "0.1.0", source=None, notices=[("NOTICE", "workspace crates are not dependencies")]),
        package(directory, "normal", "1.0.0", notices=[("NOTICE.txt", "normal notice")]),
        package(directory, "build-only", "1.0.0", notices=[("NOTICES.md", "build notice")]),
        package(directory, "transitive", "1.0.0", notices=[("NOTICE", "transitive notice"), ("LICENSE", "not a notice")]),
        package(directory, "dev-only", "1.0.0", notices=[("NOTICE", "dev notice must be excluded")]),
        package(directory, "dev-and-normal", "1.0.0", notices=[("notice.md", "lower-case notice")]),
        package(directory, "unreachable", "1.0.0", notices=[("NOTICE", "unreachable notice")]),
    ]
    metadata = {
        "packages": packages,
        "resolve": {
            "root": "leani 0.1.0",
            "nodes": [
                {"id": "leani 0.1.0", "deps": [
                    dependency("normal 1.0.0", None),
                    dependency("build-only 1.0.0", "build"),
                    dependency("dev-only 1.0.0", "dev"),
                    dependency("dev-and-normal 1.0.0", "dev", None),
                ]},
                {"id": "normal 1.0.0", "deps": [dependency("transitive 1.0.0", None)]},
                {"id": "build-only 1.0.0", "deps": []},
                {"id": "transitive 1.0.0", "deps": [dependency("dev-only 1.0.0", "dev")]},
                {"id": "dev-only 1.0.0", "deps": [dependency("unreachable 1.0.0", None)]},
                {"id": "dev-and-normal 1.0.0", "deps": []},
                {"id": "unreachable 1.0.0", "deps": []},
            ],
        },
    }
    shipped = [entry["name"] for entry in generator.shipped_packages(metadata)]
    assert shipped == ["build-only", "dev-and-normal", "normal", "transitive"], shipped
    notices = list(generator.collect_notices(generator.shipped_packages(metadata)))
    assert [(name, file_name, text) for name, _, file_name, text in notices] == [
        ("build-only", "NOTICES.md", "build notice"),
        ("dev-and-normal", "notice.md", "lower-case notice"),
        ("normal", "NOTICE.txt", "normal notice"),
        ("transitive", "NOTICE", "transitive notice"),
    ], notices
    rendered = generator.render_notices(notices)
    assert "normal 1.0.0 — NOTICE.txt\n" in rendered and "dev notice" not in rendered, rendered
    assert str(directory) not in rendered, "rendered notices must not leak local paths"
print("dependency NOTICE collection scenarios passed")
