#!/usr/bin/env python3
"""Exercise version preparation and release recovery without registry writes."""

from contextlib import contextmanager
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from lib.release_common import CRATES, GitHub, ReleaseError, git, metadata

SCRIPTS = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("coordinate_release", SCRIPTS / "coordinate-release.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class Preparation(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="leani-version-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.environment = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
        files = {
            "Cargo.toml": '[workspace]\nmembers = ["crates/node", "crates/primitives", "crates/processor-api", "crates/source-api", "crates/testkit"]\n[workspace.package]\nversion = "0.1.0-rc.1"\n',
            "packages/sdk/package.json": '{"name": "@smart-byte/leani-sdk", "version": "0.1.0-rc.1"}\n',
            "packages/sdk/src/index.ts": 'export const SDK_VERSION = "0.1.0-rc.1" as const;\n',
            "packages/sdk/README.md": 'bun add @smart-byte/leani-sdk@0.1.0-rc.1\n',
            "README.md": 'Status: pre-release preview (`v0.1.0-rc.1`).\nbun add @smart-byte/leani-sdk@0.1.0-rc.1\n',
            "docs/getting-started/install.mdx": 'Expect `leani 0.1.0-rc.1`.\nbun add @smart-byte/leani-sdk@0.1.0-rc.1\n',
            "docs/operations/upgrade.md": 'The rc.1 store has schema 21.\nbun add @smart-byte/leani-sdk@0.1.0-rc.1\n',
            "docs/adr/history.md": 'Version 0.1.0-rc.1 used schema 21; the migration goes to schema 23.\n',
            "CHANGELOG.md": '# Changelog\n\n## [Unreleased]\n\n### Upgrading from 0.1.0-rc.1\n\nBack up schema 21 before upgrading to 23.\nbun add @smart-byte/leani-sdk@<version>\n\n## [0.1.0-rc.1]\n\nOriginal release.\n',
        }
        members = {"node": "leani", "primitives": "leani-primitives", "processor-api": "leani-processor-api", "source-api": "leani-source-api", "testkit": "leani-testkit"}
        for directory, name in members.items():
            files[f"crates/{directory}/Cargo.toml"] = f'[package]\nname = "{name}"\nversion.workspace = true\n[dependencies]\nleani-primitives = {{ version = "=0.1.0-rc.1", path = "../primitives" }}\nexternal = {{ version = "=0.1.0-rc.1" }}\n'
        files["Cargo.lock"] = 'version = 4\n' + ''.join(f'\n[[package]]\nname = "{name}"\nversion = "0.1.0-rc.1"\n' for name in members.values())
        files["Cargo.lock"] += '\n[[package]]\nname = "external"\nversion = "0.1.0-rc.1"\nsource = "registry+https://github.com/rust-lang/crates.io-index"\n'
        for name, contents in files.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents)
        self.run_git("init", "-q", "--initial-branch=main")
        self.run_git("add", ".")
        self.run_git("-c", "user.name=Release fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")

    def run_git(self, *args):
        return subprocess.run(["git", *args], cwd=self.root, env=self.environment, text=True, capture_output=True, check=True).stdout

    def prepare(self, version):
        return subprocess.run([sys.executable, str(SCRIPTS / "prepare-release.py"), version, "--output", str(self.root / "prepared.json")],
                              cwd=self.root, env=self.environment, text=True, capture_output=True)

    def test_prepares_matching_versions_and_keeps_upgrade_history(self):
        result = self.prepare("0.1.0-rc.2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(tomllib.loads((self.root / "Cargo.toml").read_text())["workspace"]["package"]["version"], "0.1.0-rc.2")
        packages = tomllib.loads((self.root / "Cargo.lock").read_text())["package"]
        self.assertEqual({item["version"] for item in packages if "source" not in item}, {"0.1.0-rc.2"})
        self.assertEqual(next(item["version"] for item in packages if item["name"] == "external"), "0.1.0-rc.1")
        for path in (self.root / "crates").glob("*/Cargo.toml"):
            self.assertEqual(tomllib.loads(path.read_text())["dependencies"]["leani-primitives"]["version"], "=0.1.0-rc.2")
            self.assertEqual(tomllib.loads(path.read_text())["dependencies"]["external"]["version"], "=0.1.0-rc.1")
        self.assertEqual(json.loads((self.root / "packages/sdk/package.json").read_text())["version"], "0.1.0-rc.2")
        result = subprocess.run(["ruby", str(SCRIPTS / "check-sdk-pins.rb"), str(self.root / "packages/sdk/package.json"),
                                 str(self.root / "README.md"), str(self.root / "docs/operations/upgrade.md")], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / "docs/adr/history.md").read_text(), 'Version 0.1.0-rc.1 used schema 21; the migration goes to schema 23.\n')
        changelog = (self.root / "CHANGELOG.md").read_text()
        self.assertIn("### Upgrading from 0.1.0-rc.1", changelog)
        self.assertIn("Back up schema 21 before upgrading to 23.", changelog)
        self.assertIn("## [0.1.0-rc.2]", changelog)

    def test_preparation_error_leaves_every_tracked_file_unchanged(self):
        (self.root / "CHANGELOG.md").write_text("Missing unreleased heading\n")
        self.run_git("add", "CHANGELOG.md")
        self.run_git("-c", "user.name=Release fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "malformed changelog")
        result = self.prepare("0.1.0-rc.2")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("expected one occurrence", result.stderr)
        self.assertEqual(self.run_git("diff", "--name-only"), "")

    def test_rejects_same_version_and_downgrade(self):
        for value in ("0.1.0-rc.1", "0.0.9"):
            with self.subTest(value=value):
                result = self.prepare(value)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("must be newer", result.stderr)
                self.assertEqual(self.run_git("diff", "--name-only"), "")


class Publication(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="leani-release-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.sha = git("rev-parse", "HEAD")
        self.version = metadata()
        self.dispatches = []
        self.published = {"binaries", "sdk", "container"}
        self.partial_crates = {"leani-primitives", "leani-source-api"}
        self.failed_workflow = None
        self.last_dispatch = None
        self.moved_tag = False
        self.unreviewed_source = False
        self.pending_crates = False
        self.bad_asset = False
        self.files = {}
        self.run_ids = {stage: index + 10 for index, stage in enumerate(release.WORKFLOWS)}

        def artifact(stage, name, files):
            directory = self.root / stage / name
            directory.mkdir(parents=True)
            for filename, contents in files.items():
                (directory / filename).write_bytes(contents)
            sums = ''.join(f"{hashlib.sha256(contents).hexdigest()}  {filename}\n" for filename, contents in files.items())
            (directory / "SHA256SUMS").write_text(sums)
            self.files[name] = directory
            return directory

        binary_name = f"leani-release-candidate-v{self.version}"
        binary = {f"leani-v{self.version}-{target}.tar.gz": target.encode() for target in release.TARGETS}
        binary.update({"leani.cdx.json": b'{"bomFormat":"CycloneDX"}', "THIRD_PARTY_LICENSES.txt": b"MIT", "leani.rb": b"formula"})
        self.binary = artifact("binaries", binary_name, binary)
        self.crates = artifact("crates", "leani-rust-library-candidate", {f"{name}-{self.version}.crate": name.encode() for name in CRATES})
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            contents = json.dumps({"name": "@smart-byte/leani-sdk", "version": self.version}).encode()
            item = tarfile.TarInfo("package/package.json")
            item.size = len(contents)
            archive.addfile(item, io.BytesIO(contents))
        self.sdk = artifact("sdk", "leani-sdk-candidate", {f"smart-byte-leani-sdk-{self.version}.tgz": buffer.getvalue()})
        self.images = {"amd64": "sha256:" + "1" * 64, "arm64": "sha256:" + "2" * 64}
        for arch, identity in self.images.items():
            artifact("container", f"leani-container-{arch}", {f"leani-container-{arch}.tar.gz": arch.encode(),
                     f"leani-container-{arch}.cdx.json": b"{}", "image.id": (identity + "\n").encode()})
        self.plan = {"schema": 1, "state": "verified", "repository": "example/leani", "version": self.version,
                     "commit": self.sha, "coordinator_commit": self.sha, "runs": self.run_ids, "artifacts": {}}
        for stage in ("binaries", "crates", "sdk", "container"):
            names = release.Coordinator(GitHub("example/leani")).artifact_names(stage, self.version)
            self.plan["artifacts"][stage] = {name: {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                                   for path in self.files[name].iterdir()} for name in names}
        self.gh = GitHub("example/leani")

    def run_record(self, run_id, workflow):
        return {"id": run_id, "head_sha": self.sha, "path": f".github/workflows/{workflow}", "event": "workflow_dispatch",
                "head_branch": "feature" if run_id == 99 and self.unreviewed_source else "main",
                "status": "completed", "conclusion": "success", "name": workflow, "html_url": f"https://github.com/example/leani/actions/runs/{run_id}"}

    def api(self, path, data=None, missing=False):
        if path.startswith("compare/"):
            return {"status": "identical"}
        if path.startswith("git/ref/tags/"):
            return {"object": {"type": "tag", "sha": "b" * 40}}
        if path.startswith("git/tags/"):
            return {"object": {"type": "commit", "sha": "c" * 40 if self.moved_tag else self.sha}}
        if path.startswith("releases/tags/"):
            if "binaries" not in self.published:
                return None
            assets = [{"name": path.name, "digest": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()} for path in self.binary.iterdir()]
            if self.bad_asset:
                assets[0]["digest"] = "sha256:" + "0" * 64
            return {"draft": False, "prerelease": "-rc." in self.version, "assets": assets}
        if path.endswith("/dispatches"):
            self.dispatches.append((path, data))
            self.last_dispatch = path.split('/')[2]
            if self.last_dispatch != self.failed_workflow:
                self.published.add(next(stage for stage, workflow in release.WORKFLOWS.items() if workflow == self.last_dispatch))
            return {"workflow_run_id": 900}
        if path.startswith("actions/workflows/"):
            workflow = path.split('/')[2]
            stage = next(stage for stage, value in release.WORKFLOWS.items() if value == workflow)
            runs = [self.run_record(self.run_ids[stage], workflow)]
            if stage == "crates" and self.pending_crates:
                waiting = self.run_record(900, workflow)
                waiting.update(status="waiting", conclusion=None,
                               display_title="Prepare Rust library release [leani-publish-99-crates-" + "0" * 32 + "]")
                runs.insert(0, waiting)
            return {"workflow_runs": runs}
        if path.endswith("/artifacts?per_page=100"):
            run_id = int(path.split('/')[2])
            stage = next(stage for stage, value in self.run_ids.items() if value == run_id)
            names = release.Coordinator(self.gh).artifact_names(stage, self.version)
            return {"total_count": len(names), "artifacts": [{"name": name, "expired": False} for name in names]}
        if path.startswith("actions/runs/"):
            run_id = int(path.split('/')[2])
            if run_id == 900 and self.pending_crates:
                self.published.add("crates")
                self.last_dispatch = "crates-release.yml"
            workflow = "release-coordinate.yml" if run_id == 99 else self.last_dispatch if run_id == 900 else next(
                release.WORKFLOWS[stage] for stage, value in self.run_ids.items() if value == run_id)
            record = self.run_record(run_id, workflow)
            if run_id == 900 and workflow == self.failed_workflow:
                record["conclusion"] = "failure"
            return record
        raise AssertionError(f"unexpected GitHub API request: {path}")

    def download(self, *args):
        self.assertEqual(args[:3], ("gh", "run", "download"))
        name = args[args.index("--name") + 1]
        destination = args[args.index("--dir") + 1]
        shutil.copytree(self.files[name], destination, dirs_exist_ok=True)

    def registry(self, url):
        if url.startswith("https://crates.io/"):
            name = url.split('/')[-2]
            if "crates" not in self.published and name not in self.partial_crates:
                return None
            return {"version": {"checksum": hashlib.sha256((self.crates / f"{name}-{self.version}.crate").read_bytes()).hexdigest()}}
        if "sdk" not in self.published:
            return None
        return {"version": self.version, "dist": {"integrity": "sha512-" + release.base64.b64encode(
            hashlib.sha512((self.sdk / f"smart-byte-leani-sdk-{self.version}.tgz").read_bytes()).digest()).decode()}}

    def image(self, ref):
        if "container" not in self.published:
            return None
        for arch, digest in self.images.items():
            if ref.endswith(f"-{arch}") or ref.endswith(f"@{digest}"):
                return {"config": {"digest": digest}}
        return {"manifests": [{"platform": {"os": "linux", "architecture": arch}, "digest": digest} for arch, digest in self.images.items()]}

    @contextmanager
    def services(self):
        with patch.object(self.gh, "api", side_effect=self.api), patch.object(release, "command", side_effect=self.download), \
                patch.object(release, "registry_json", side_effect=self.registry), patch.object(release, "image_manifest", side_effect=self.image):
            yield release.Coordinator(self.gh)

    def test_partial_release_only_dispatches_the_unfinished_publisher(self):
        with self.services() as coordinator:
            coordinator.publish(self.plan, 99)
        self.assertEqual(len(self.dispatches), 1)
        path, request = self.dispatches[0]
        self.assertEqual(path, "actions/workflows/crates-release.yml/dispatches")
        self.assertEqual(request["ref"], f"v{self.version}")
        self.assertEqual(request["inputs"]["candidate_run_id"], str(self.run_ids["crates"]))
        self.assertEqual(request["inputs"]["container_candidate_run_id"], str(self.run_ids["container"]))
        self.assertFalse(request["inputs"]["bootstrap"])

    def test_candidate_records_the_verified_runs_and_artifact_hashes(self):
        output = self.root / "candidate-plan.json"
        with self.services() as coordinator:
            coordinator.candidate(self.version, output)
        plan = json.loads(output.read_text())
        self.assertEqual(plan["state"], "verified")
        self.assertEqual(plan["commit"], self.sha)
        self.assertEqual(plan["coordinator_commit"], self.sha)
        self.assertEqual(plan["runs"], self.run_ids)
        self.assertEqual(plan["artifacts"], self.plan["artifacts"])
        self.assertEqual(self.dispatches, [])

    def test_different_published_bytes_stop_before_any_publisher_is_dispatched(self):
        self.bad_asset = True
        with self.services() as coordinator, self.assertRaisesRegex(ReleaseError, "asset differs"):
            coordinator.publish(self.plan, 99)
        self.assertEqual(self.dispatches, [])

    def test_retry_rejoins_a_publisher_already_waiting_for_approval(self):
        self.pending_crates = True
        with self.services() as coordinator:
            coordinator.publish(self.plan, 99)
        self.assertEqual(self.dispatches, [])
        self.assertIn("crates", self.published)

    def test_full_release_publishes_all_components_in_order(self):
        self.published.clear()
        self.partial_crates.clear()
        with self.services() as coordinator:
            coordinator.publish(self.plan, 99)
        self.assertEqual([path.split('/')[2] for path, _ in self.dispatches],
                         ["release.yml", "crates-release.yml", "sdk-release.yml", "container.yml"])
        self.assertEqual(self.published, {"binaries", "crates", "sdk", "container"})

    def test_failed_publisher_stops_and_retry_resumes_after_completed_publications(self):
        self.published.clear()
        self.partial_crates.clear()
        self.failed_workflow = "sdk-release.yml"
        with self.services() as coordinator, self.assertRaisesRegex(ReleaseError, "failure"):
            coordinator.publish(self.plan, 99)
        self.assertEqual(self.published, {"binaries", "crates"})
        self.dispatches.clear()
        self.failed_workflow = None
        with self.services() as coordinator:
            coordinator.publish(self.plan, 99)
        self.assertEqual([path.split('/')[2] for path, _ in self.dispatches], ["sdk-release.yml", "container.yml"])

    def test_moved_release_tag_stops_before_publication(self):
        self.moved_tag = True
        with self.services() as coordinator, self.assertRaisesRegex(ReleaseError, "another commit"):
            coordinator.publish(self.plan, 99)
        self.assertEqual(self.dispatches, [])

    def test_plan_from_an_unreviewed_controller_branch_is_rejected(self):
        self.unreviewed_source = True
        with self.services() as coordinator, self.assertRaisesRegex(ReleaseError, "reviewed release control"):
            coordinator.publish(self.plan, 99)
        self.assertEqual(self.dispatches, [])

    def test_missing_crate_selection_verifies_existing_packages(self):
        with self.services():
            self.assertEqual(release.missing_crates(self.crates, self.version), ["leani-processor-api", "leani-testkit"])
        original = self.registry
        def conflict(url):
            if '/leani-primitives/' in url:
                return {"version": {"checksum": "0" * 64}}
            return original(url)
        with patch.object(release, "registry_json", side_effect=conflict), self.assertRaisesRegex(ReleaseError, "different bytes"):
            release.missing_crates(self.crates, self.version)

    def test_changed_candidate_files_are_rejected(self):
        (self.crates / f"leani-testkit-{self.version}.crate").write_bytes(b"modified after verification")
        with self.services() as coordinator, self.assertRaisesRegex(ReleaseError, "checksum mismatch"):
            coordinator.publish(self.plan, 99)
        self.assertEqual(self.dispatches, [])


if __name__ == "__main__":
    unittest.main()
