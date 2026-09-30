"""Shared release metadata and GitHub API operations (Python standard library)."""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tomllib

CRATES = ("leani-primitives", "leani-processor-api", "leani-source-api", "leani-testkit")
SDK = "@smart-byte/leani-sdk"
VERSION = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-rc\.([1-9]\d*))?\Z")


class ReleaseError(Exception):
    pass


def version(value):
    value = value.removeprefix("v")
    if not VERSION.fullmatch(value):
        raise ReleaseError("version must be SemVer, optionally followed by -rc.N")
    return value


def version_order(value):
    match = VERSION.fullmatch(version(value))
    major, minor, patch, rc = match.groups()
    return (int(major), int(minor), int(patch), rc is None, int(rc or 0))


def command(*args, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, timeout=180, **kwargs)
    if result.returncode:
        raise ReleaseError(f"{args[0]} failed: {result.stderr.strip() or result.stdout.strip()}")
    return result.stdout.strip()


def git(*args):
    return command("git", *args)


def metadata(ref="HEAD"):
    workspace = tomllib.loads(git("show", f"{ref}:Cargo.toml"))
    sdk = json.loads(git("show", f"{ref}:packages/sdk/package.json"))
    release = version(workspace["workspace"]["package"]["version"])
    if sdk["version"] != release or sdk["name"] != SDK:
        raise ReleaseError("the workspace and SDK must have the same release version")
    return release


def sha256(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def file_hashes(directory):
    root = Path(directory)
    files = sorted(root.rglob("*"))
    if any(path.is_symlink() for path in files):
        raise ReleaseError("release artifacts must not contain symlinks")
    return {path.relative_to(root).as_posix(): sha256(path) for path in files if path.is_file()}


def verify_sums(directory):
    root = Path(directory)
    sums = root / "SHA256SUMS"
    if not sums.is_file():
        raise ReleaseError(f"missing SHA256SUMS in {root.name}")
    names = set()
    for line in sums.read_text().splitlines():
        match = re.fullmatch(r"([a-f0-9]{64})  (?:\./)?([A-Za-z0-9_.-]+)", line)
        if not match or match[2] in names:
            raise ReleaseError("invalid or duplicate release checksum entry")
        digest, name = match.groups()
        names.add(name)
        path = root / name
        if not path.is_file() or path.is_symlink() or sha256(path) != digest:
            raise ReleaseError(f"checksum mismatch: {name}")
    if not names:
        raise ReleaseError("empty release checksum list")
    return names


class GitHub:
    def __init__(self, repository=None):
        self.repository = repository or os.environ.get("GITHUB_REPOSITORY", "smart-byte/leani")
        if not re.fullmatch(r"[\w.-]+/[\w.-]+", self.repository):
            raise ReleaseError("invalid GitHub repository")
        self.prefix = f"repos/{self.repository}"

    def api(self, path, data=None, missing=False):
        args = ["gh", "api", f"{self.prefix}/{path}"]
        if data is not None:
            args += ["--method", "POST", "--input", "-"]
        result = subprocess.run(args, input=json.dumps(data) if data is not None else None,
                                text=True, capture_output=True, timeout=180)
        if result.returncode:
            if missing and "HTTP 404" in result.stderr:
                return None
            raise ReleaseError(f"GitHub {path.split('?')[0]}: {result.stderr.strip()}")
        return json.loads(result.stdout) if result.stdout.strip() else None

    def run(self, run_id):
        if not re.fullmatch(r"[1-9]\d*", str(run_id)):
            raise ReleaseError("invalid workflow run ID")
        return self.api(f"actions/runs/{run_id}")

    def successful_run(self, run_id, workflow, commit, events=("workflow_dispatch",)):
        run = self.run(run_id)
        if (run.get("head_sha") != commit or run.get("path") != f".github/workflows/{workflow}"
                or run.get("event") not in events or run.get("status") != "completed"
                or run.get("conclusion") != "success"):
            raise ReleaseError(f"run {run_id} must be a successful {workflow} run of {commit}")
        return run

    def artifacts(self, run_id):
        # Release workflows produce fewer than 100 artifacts. Failing closed is
        # preferable to silently omitting an unexpected extra page.
        result = self.api(f"actions/runs/{run_id}/artifacts?per_page=100")
        if result["total_count"] > 100:
            raise ReleaseError("unexpected number of release artifacts")
        return {item["name"]: item for item in result["artifacts"] if not item["expired"]}
