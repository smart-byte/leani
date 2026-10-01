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
            "skills/leani/references/processors.md": '(`https://raw.githubusercontent.com/smart-byte/leani/v0.1.0-rc.1/examples/node.toml`\nfor 0.1.0-rc.1).\n',
            "THIRD_PARTY_LICENSES.txt": 'leani 0.1.0-rc.1 — https://github.com/smart-byte/leani\nleani-primitives 0.1.0-rc.1 — https://github.com/smart-byte/leani\n'
                                        'external 0.1.0-rc.1 — https://example.invalid/external\n',
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
        self.assertEqual((self.root / "skills/leani/references/processors.md").read_text(),
                         '(`https://raw.githubusercontent.com/smart-byte/leani/v0.1.0-rc.2/examples/node.toml`\nfor 0.1.0-rc.2).\n')
        # The license notices name every workspace crate with its version; CI regenerates them from Cargo.lock.
        self.assertEqual((self.root / "THIRD_PARTY_LICENSES.txt").read_text(),
                         'leani 0.1.0-rc.2 — https://github.com/smart-byte/leani\nleani-primitives 0.1.0-rc.2 — https://github.com/smart-byte/leani\n'
                         'external 0.1.0-rc.1 — https://example.invalid/external\n')
        changelog = (self.root / "CHANGELOG.md").read_text()
        self.assertIn("### Upgrading from 0.1.0-rc.1", changelog)
        self.assertIn("Back up schema 21 before upgrading to 23.", changelog)
        self.assertIn("## [0.1.0-rc.2]", changelog)

    def test_stable_releases_update_status_and_only_whole_version_tokens(self):
        result = self.prepare("0.1.0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Status: stable release (`v0.1.0`).", (self.root / "README.md").read_text())
        with (self.root / "README.md").open("a") as stream:
            stream.write("Previously previewed as `v0.1.0-rc.1`.\n")
        self.run_git("-c", "user.name=Release fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qam", "stable")
        result = self.prepare("0.1.1")
        self.assertEqual(result.returncode, 0, result.stderr)
        readme = (self.root / "README.md").read_text()
        self.assertIn("Status: stable release (`v0.1.1`).", readme)
        self.assertIn("Previously previewed as `v0.1.0-rc.1`.", readme)

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
        self.draft_assets = None
        self.missing_tags = False
        self.created = []
        self.failed_gate = None
        self.compare_status = "identical"
        self.sdk_lag = 0
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

    def github_release(self):
        assets = [{"name": path.name, "id": index, "digest": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()}
                  for index, path in enumerate(sorted(self.binary.iterdir()))]
        if self.bad_asset:
            assets[0]["digest"] = "sha256:" + "0" * 64
        record = {"tag_name": f"v{self.version}", "prerelease": "-rc." in self.version}
        if self.draft_assets is not None:
            return {**record, "draft": True, "assets": assets[:self.draft_assets]}
        return {**record, "draft": False, "assets": assets} if "binaries" in self.published else None

    def api(self, path, data=None, missing=False):
        if path.startswith("compare/"):
            return {"status": self.compare_status}
        if path.startswith("git/ref/tags/"):
            return None if self.missing_tags else {"object": {"type": "tag", "sha": "b" * 40}}
        if path in ("git/tags", "git/refs"):
            self.created.append(path)
            return {"sha": "d" * 40}
        if path.startswith("git/tags/"):
            return {"object": {"type": "commit", "sha": "c" * 40 if self.moved_tag else self.sha}}
        if path == "releases?per_page=100":
            record = self.github_release()
            return [record] if record else []
        if path.startswith("releases/tags/"):
            # Like GitHub, the tag lookup only returns a published release.
            record = self.github_release()
            return record if record and not record["draft"] else None
        if path.endswith("/dispatches"):
            self.dispatches.append((path, data))
            self.last_dispatch = path.split('/')[2]
            if self.last_dispatch != self.failed_workflow:
                self.published.add(next(stage for stage, workflow in release.WORKFLOWS.items() if workflow == self.last_dispatch))
                if self.last_dispatch == "release.yml":
                    self.draft_assets = None
            return {"workflow_run_id": 900}
        if path.startswith("actions/workflows/"):
            workflow = path.split('/')[2]
            stage = next(stage for stage, value in release.WORKFLOWS.items() if value == workflow)
            runs = [self.run_record(self.run_ids[stage], workflow)]
            if stage == self.failed_gate:
                runs.insert(0, {**self.run_record(500, workflow), "event": "push", "conclusion": "failure"})
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
            return {"total_count": len(names), "artifacts": [{"name": name, "expired": False, "expires_at": f"2026-10-{run_id}T00:00:00Z"}
                                                            for name in names]}
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
        if self.sdk_lag:
            self.sdk_lag -= 1
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
                patch.object(release, "registry_json", side_effect=self.registry), patch.object(release, "image_manifest", side_effect=self.image), \
                patch.object(release.time, "sleep"):
            yield release.Coordinator(self.gh)

    def test_partial_release_only_dispatches_the_unfinished_publisher(self):
        with self.services() as coordinator:
            coordinator.publish(self.plan, 99)
        self.assertEqual(len(self.dispatches), 1)
        path, request = self.dispatches[0]
        self.assertEqual(path, "actions/workflows/crates-release.yml/dispatches")
        self.assertEqual(request["ref"], f"v{self.version}")
        self.assertTrue(request["return_run_details"])
        self.assertEqual(request["inputs"]["candidate_run_id"], str(self.run_ids["crates"]))
        self.assertEqual(request["inputs"]["container_candidate_run_id"], str(self.run_ids["container"]))
        self.assertEqual(request["inputs"]["bootstrap"], "false")

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
        # The container preparation run's artifacts expire first.
        self.assertTrue(coordinator.messages[-1].endswith(f"before 2026-10-{self.run_ids['container']}T00:00:00Z"))

    def test_known_failure_on_main_stops_before_release_tags_exist(self):
        self.missing_tags = True
        self.failed_gate = "ci"
        with self.services() as coordinator, self.assertRaisesRegex(ReleaseError, "before reserving release tags"):
            coordinator.candidate(self.version, self.root / "candidate-plan.json")
        self.assertEqual(self.created, [])

    def test_different_published_bytes_stop_before_any_publisher_is_dispatched(self):
        self.bad_asset = True
        with self.services() as coordinator, self.assertRaisesRegex(ReleaseError, "asset differs"):
            coordinator.publish(self.plan, 99)
        self.assertEqual(self.dispatches, [])

    def test_partial_draft_with_different_bytes_stops_before_any_publisher_is_dispatched(self):
        self.published.clear()
        self.draft_assets = 2
        self.bad_asset = True
        with self.services() as coordinator, self.assertRaisesRegex(ReleaseError, "asset differs"):
            coordinator.publish(self.plan, 99)
        self.assertEqual(self.dispatches, [])

    def test_matching_partial_draft_is_finished_by_the_binary_publisher(self):
        self.published.discard("binaries")
        self.draft_assets = 2
        with self.services() as coordinator:
            coordinator.publish(self.plan, 99)
        self.assertEqual([path.split('/')[2] for path, _ in self.dispatches], ["release.yml", "crates-release.yml"])

    def test_registry_lag_after_publication_is_awaited(self):
        self.published.discard("sdk")
        self.partial_crates = set(CRATES)
        self.sdk_lag = 2
        with self.services() as coordinator:
            coordinator.publish(self.plan, 99)
        self.assertEqual([path.split('/')[2] for path, _ in self.dispatches], ["sdk-release.yml"])

    def test_newer_failed_validation_blocks_publication(self):
        self.failed_gate = "security"
        with self.services() as coordinator, self.assertRaisesRegex(ReleaseError, "latest security validation must pass"):
            coordinator.publish(self.plan, 99)
        self.assertEqual(self.dispatches, [])

    def test_release_commit_outside_main_is_rejected(self):
        self.compare_status = "diverged"
        with self.services() as coordinator, self.assertRaisesRegex(ReleaseError, "must be on main"):
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


class PreparationPullRequest(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.changed_on_main = []
        self.payload = {"schema": 1, "version": "0.1.0-rc.2", "base_sha": "a" * 40,
                        "files": {"Cargo.toml": "version\n", "Cargo.lock": "lock\n", "CHANGELOG.md": "changes\n"}}

    def api(self, path, data=None, missing=False):
        self.requests.append((path, data))
        responses = {"commits/main": {"sha": "b" * 40, "commit": {"tree": {"sha": "c" * 40}}},
                     "git/trees": {"sha": "d" * 40}, "git/commits": {"sha": "e" * 40}, "git/refs": {},
                     "pulls": {"html_url": "https://github.com/example/leani/pull/1"}}
        if path in responses:
            return responses[path]
        if path.startswith("compare/"):
            return {"status": "ahead", "files": [{"filename": name} for name in self.changed_on_main]}
        if path.startswith("git/ref/heads/"):
            return None
        if path.startswith("pulls?"):
            return []
        raise AssertionError(f"unexpected GitHub API request: {path}")

    def open_pr(self):
        gh = GitHub("example/leani")
        with patch.object(gh, "api", side_effect=self.api):
            release.Coordinator(gh).open_pr(self.payload)

    def writes(self):
        return [path for path, data in self.requests if data is not None]

    def test_main_advancing_elsewhere_bases_the_release_on_current_main(self):
        self.changed_on_main = ["crates/node/src/main.rs"]
        self.open_pr()
        commit = next(data for path, data in self.requests if path == "git/commits")
        self.assertEqual(commit["parents"], ["b" * 40])

    def test_main_changing_a_prepared_file_stops_before_any_write(self):
        self.changed_on_main = ["Cargo.lock"]
        with self.assertRaisesRegex(ReleaseError, "rerun prepare"):
            self.open_pr()
        self.assertEqual(self.writes(), [])

    def test_files_outside_the_release_allowlist_are_refused_before_any_write(self):
        self.payload["files"][".github/workflows/ci.yml"] = "on: push\n"
        with self.assertRaisesRegex(ReleaseError, "unexpected file"):
            self.open_pr()
        self.assertEqual(self.writes(), [])


if __name__ == "__main__":
    unittest.main()
