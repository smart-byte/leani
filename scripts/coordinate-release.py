#!/usr/bin/env python3
"""Coordinate existing release workflows and verify resumable publication."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
import uuid

from lib.release_common import CRATES, SDK, GitHub, ReleaseError, command, file_hashes, git, metadata, sha256, verify_sums, version

WORKFLOWS = {"ci": "ci.yml", "security": "security.yml", "site": "site.yml", "container": "container.yml",
             "binaries": "release.yml", "crates": "crates-release.yml", "sdk": "sdk-release.yml"}
TARGETS = ("x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "x86_64-apple-darwin", "aarch64-apple-darwin")
GATES = ("ci", "security", "site")


def registry_json(url):
    try:
        with urlopen(Request(url, headers={"User-Agent": "leani-release-coordinator (github.com/smart-byte/leani)"}), timeout=60) as response:
            return json.loads(response.read(4 * 1024 * 1024))
    except HTTPError as error:
        if error.code == 404:
            return None
        raise ReleaseError(f"registry returned HTTP {error.code}") from error
    except (URLError, ValueError) as error:
        raise ReleaseError("unable to verify registry publication") from error


def missing_crates(directory, release):
    root = Path(directory)
    expected = {f"{name}-{release}.crate" for name in CRATES}
    if verify_sums(root) != expected:
        raise ReleaseError("Rust candidate must contain exactly the four authoring libraries")
    missing = []
    for name in CRATES:
        published = registry_json(f"https://crates.io/api/v1/crates/{name}/{release}")
        if published is None:
            missing.append(name)
        elif published.get("version", {}).get("checksum") != sha256(root / f"{name}-{release}.crate"):
            raise ReleaseError(f"crates.io already has different bytes for {name}@{release}")
    return missing


def sdk_published(directory, release):
    root = Path(directory)
    filename = f"smart-byte-leani-sdk-{release}.tgz"
    if verify_sums(root) != {filename}:
        raise ReleaseError("SDK candidate must contain one matching npm archive")
    published = registry_json(f"https://registry.npmjs.org/{quote(SDK, safe='')}/{release}")
    if published is None:
        return False
    with open(root / filename, "rb") as stream:
        integrity = "sha512-" + base64.b64encode(hashlib.file_digest(stream, "sha512").digest()).decode()
    if published.get("version") != release or published.get("dist", {}).get("integrity") != integrity:
        raise ReleaseError(f"npm already has different bytes for {SDK}@{release}")
    return True


def image_manifest(ref):
    result = subprocess.run(["docker", "buildx", "imagetools", "inspect", ref, "--raw"],
                            text=True, capture_output=True, timeout=180)
    if result.returncode:
        error = result.stderr.lower()
        if "not found" in error or "manifest unknown" in error:
            return None
        if "unauthorized" in error or "denied" in error:
            raise ReleaseError("GHCR refused anonymous access; make the leani package public (docs/contributing/releasing.md)")
        raise ReleaseError("unable to verify GHCR image; check registry access")
    return json.loads(result.stdout)


def container_published(expected, repository, release):
    image = f"ghcr.io/{repository.lower()}"
    for arch, identity in expected.items():
        existing = image_manifest(f"{image}:v{release}-{arch}")
        if existing is not None and existing.get("config", {}).get("digest") != identity:
            raise ReleaseError(f"GHCR already has a different {arch} image for v{release}")
    index = image_manifest(f"{image}:v{release}")
    if index is None:
        return False
    manifests = index.get("manifests", [])
    if len(manifests) != 2:
        raise ReleaseError("release image must contain exactly two architecture manifests")
    found = set()
    for item in manifests:
        platform = item.get("platform", {})
        arch = platform.get("architecture")
        digest = item.get("digest", "")
        if platform.get("os") != "linux" or arch not in expected or arch in found or not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            raise ReleaseError("unexpected platform in the release image")
        manifest = image_manifest(f"{image}@{digest}")
        if manifest is None or manifest.get("config", {}).get("digest") != expected[arch]:
            raise ReleaseError(f"GHCR release image does not match the verified {arch} candidate")
        found.add(arch)
    return found == set(expected)


def github_release(gh, tag):
    # releases/tags/{tag} hides drafts, and a retry must find a partial draft.
    # ponytail: newest 100 releases; a retry follows its draft closely.
    matches = [item for item in gh.api("releases?per_page=100") if item["tag_name"] == tag]
    if len(matches) > 1:
        raise ReleaseError(f"several GitHub releases use {tag}; delete the extra drafts")
    return matches[0] if matches else None


def asset_digest(gh, asset):
    if asset.get("digest"):
        return asset["digest"]
    result = subprocess.run(["gh", "api", "-H", "Accept: application/octet-stream", f"{gh.prefix}/releases/assets/{asset['id']}"],
                            capture_output=True, timeout=600)
    if result.returncode:
        raise ReleaseError(f"unable to download GitHub release asset {asset['name']}")
    return "sha256:" + hashlib.sha256(result.stdout).hexdigest()


def release_published(gh, expected, release):
    existing = github_release(gh, f"v{release}")
    if existing is None:
        return False
    assets = {item["name"]: item for item in existing["assets"]}
    if any(name not in expected for name in assets):
        raise ReleaseError("GitHub release contains unexpected assets")
    for name, asset in assets.items():
        if asset_digest(gh, asset) != "sha256:" + expected[name]:
            raise ReleaseError(f"GitHub release asset differs from the candidate: {name}")
    if existing["draft"]:
        return False
    if set(assets) != set(expected) or bool(existing["prerelease"]) != ("-rc." in release):
        raise ReleaseError("published GitHub release is incomplete or has incorrect prerelease status")
    return True


class Coordinator:
    def __init__(self, gh, timeout=14400):
        self.gh = gh
        self.deadline = time.monotonic() + timeout
        self.messages = []

    def report(self, message):
        self.messages.append(message)
        print(message, flush=True)
        summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary, "a") as stream:
                stream.write(f"- {message}\n")

    def pause(self):
        if time.monotonic() >= self.deadline:
            raise ReleaseError("release coordination timed out; rerun with the same candidate plan to resume")
        time.sleep(15)

    def runs(self, workflow, commit):
        result = self.gh.api(f"actions/workflows/{workflow}/runs?head_sha={commit}&per_page=100")
        return sorted((run for run in result["workflow_runs"] if run["head_sha"] == commit
                       and run["event"] in ("push", "workflow_dispatch")), key=lambda run: run["id"], reverse=True)

    def dispatch(self, workflow, ref, inputs, request_prefix=None):
        # String values are valid for every workflow_dispatch input type.
        inputs = {key: str(value).lower() if isinstance(value, bool) else str(value) for key, value in inputs.items()}
        if request_prefix:
            # The unique run name lets a retry rejoin this publication run.
            inputs["release_request"] = f"{request_prefix}-{uuid.uuid4().hex}"
        result = self.gh.api(f"actions/workflows/{workflow}/dispatches",
                             {"ref": ref, "inputs": inputs, "return_run_details": True})
        if not result or not result.get("workflow_run_id"):
            raise ReleaseError(f"GitHub did not return the {workflow} run it started")
        return result["workflow_run_id"]

    def resume_publication(self, stage, candidate_run_id, commit):
        prefix = f"leani-publish-{candidate_run_id}-{stage}"
        for run in self.runs(WORKFLOWS[stage], commit):
            marker = r"\[" + re.escape(prefix) + r"-[a-f0-9]{32}\]$"
            if run["status"] != "completed" and re.search(marker, run.get("display_title", "")):
                self.report(f"{stage}: resuming its existing publication run #{run['id']}")
                return run["id"], prefix
        return None, prefix

    def wait(self, run_id):
        previous = None
        while True:
            run = self.gh.run(run_id)
            state = run["status"]
            if state != previous:
                self.report(f"[{run['name']} #{run_id}]({run['html_url']}): {state}")
                previous = state
            if state == "completed":
                if run["conclusion"] != "success":
                    raise ReleaseError(f"run {run_id} finished with {run['conclusion']}; completed publications are preserved")
                return run
            self.pause()

    def verify_tag(self, name, commit, create=False):
        ref = self.gh.api(f"git/ref/tags/{name}", missing=True)
        if ref is None:
            if not create:
                raise ReleaseError(f"missing annotated tag {name}")
            tag = self.gh.api("git/tags", {"tag": name, "message": f"Leani {name}", "object": commit, "type": "commit"})
            try:
                self.gh.api("git/refs", {"ref": f"refs/tags/{name}", "sha": tag["sha"]})
            except ReleaseError as error:
                raise ReleaseError("tag creation was refused; configure RELEASE_CONTROL_TOKEN in release-control for a release maintainer allowed by the tag ruleset") from error
            self.report(f"Created annotated tag {name} on {commit}")
            return
        if ref["object"]["type"] != "tag":
            raise ReleaseError(f"{name} must be an annotated tag")
        tag = self.gh.api(f"git/tags/{ref['object']['sha']}")
        if tag["object"]["type"] != "commit" or tag["object"]["sha"] != commit:
            raise ReleaseError(f"{name} points to another commit; tags are never moved")

    def release_commit(self, release, commit):
        if not re.fullmatch(r"[a-f0-9]{40}", commit) or metadata(commit) != release:
            raise ReleaseError("release commit and package versions do not match")
        comparison = self.gh.api(f"compare/{commit}...main")
        if comparison["status"] not in ("ahead", "identical"):
            raise ReleaseError("release commit must be on main")

    def artifact_names(self, stage, release):
        return {"binaries": [f"leani-release-candidate-v{release}"],
                "container": ["leani-container-amd64", "leani-container-arm64"],
                "sdk": ["leani-sdk-candidate"], "crates": ["leani-rust-library-candidate"]}.get(stage, [])

    def collect(self, plan, directory, compare=False):
        inventory = {}
        for stage, run_id in plan["runs"].items():
            names = self.artifact_names(stage, plan["version"])
            if not names:
                continue
            available = self.gh.artifacts(run_id)
            inventory[stage] = {}
            for name in names:
                if name not in available:
                    raise ReleaseError(f"run {run_id} lacks unexpired artifact {name}; prepare new candidates")
                destination = Path(directory) / stage / name
                destination.mkdir(parents=True)
                command("gh", "run", "download", str(run_id), "--repo", self.gh.repository,
                        "--name", name, "--dir", str(destination))
                hashes = file_hashes(destination)
                if not hashes or any("/" in filename for filename in hashes):
                    raise ReleaseError(f"{name} must contain flat release files")
                inventory[stage][name] = hashes
                listed = verify_sums(destination)
                if stage == "binaries":
                    archives = {f"leani-v{plan['version']}-{target}.tar.gz" for target in TARGETS}
                    if not archives <= hashes.keys() or not {"leani.cdx.json", "THIRD_PARTY_LICENSES.txt", "leani.rb"} <= hashes.keys():
                        raise ReleaseError("binary candidate is missing archives, notices, formula, or node SBOM")
                elif stage == "container":
                    if listed != {f"{name}.tar.gz", f"{name}.cdx.json", "image.id"}:
                        raise ReleaseError("container candidate inventory is incomplete")
                elif stage == "crates":
                    if listed != {f"{crate}-{plan['version']}.crate" for crate in CRATES}:
                        raise ReleaseError("unexpected Rust candidate packages")
                elif stage == "sdk":
                    filename = f"smart-byte-leani-sdk-{plan['version']}.tgz"
                    if listed != {filename}:
                        raise ReleaseError("unexpected SDK candidate archive")
                    with tarfile.open(destination / filename) as archive:
                        stream = archive.extractfile("package/package.json")
                        manifest = json.load(stream) if stream else {}
                    if manifest.get("name") != SDK or manifest.get("version") != plan["version"]:
                        raise ReleaseError("SDK archive version does not match the release")
        if compare and inventory != plan.get("artifacts"):
            raise ReleaseError("release candidate files differ from the recorded plan")
        return inventory

    def require_passing_main_checks(self, commit):
        # Release tags are never moved, so a failure already known on this
        # commit must stop the release before its tags exist.
        for stage in (*GATES, "container"):
            runs = self.runs(WORKFLOWS[stage], commit)
            while runs and runs[0]["status"] != "completed":
                self.pause()
                runs = self.runs(WORKFLOWS[stage], commit)
            if runs and runs[0]["conclusion"] != "success":
                raise ReleaseError(f"the latest {stage} run failed on {commit}; rerun or fix it before reserving release tags")

    def candidate(self, release, output, commit=None):
        release = version(release)
        coordinator_commit = git("rev-parse", "HEAD")
        commit = commit or coordinator_commit
        self.release_commit(release, commit)
        tags = (f"v{release}", f"sdk-v{release}")
        if any(self.gh.api(f"git/ref/tags/{name}", missing=True) is None for name in tags):
            self.require_passing_main_checks(commit)
        for name in tags:
            self.verify_tag(name, commit, create=True)
        plan = {"schema": 1, "repository": self.gh.repository, "version": release, "commit": commit,
                "coordinator_commit": coordinator_commit, "state": "preparing", "runs": {}, "artifacts": {}}
        for stage, workflow in WORKFLOWS.items():
            inputs = {"publish": False} if stage not in GATES else {}
            if stage == "binaries":
                inputs["version"] = f"v{release}"
            elif stage == "container":
                inputs["image_tag"] = f"v{release}"
            ref = f"sdk-v{release}" if stage == "sdk" else f"v{release}"
            runs = self.runs(workflow, commit)
            selected = None
            if stage in GATES:
                if runs and (runs[0]["status"] != "completed" or runs[0]["conclusion"] == "success"):
                    selected = runs[0]["id"]
            else:
                for run in runs:
                    if run["status"] == "completed" and run["conclusion"] == "success" and set(self.artifact_names(stage, release)) <= self.gh.artifacts(run["id"]).keys():
                        selected = run["id"]
                        break
            plan["runs"][stage] = selected or self.dispatch(workflow, ref, inputs)
            Path(output).write_text(json.dumps(plan, indent=2) + "\n")
        for stage, run_id in plan["runs"].items():
            self.wait(run_id)
            self.gh.successful_run(run_id, WORKFLOWS[stage], commit, ("push", "workflow_dispatch"))
        with tempfile.TemporaryDirectory(prefix="leani-candidates-") as directory:
            plan["artifacts"] = self.collect(plan, directory)
        plan["state"] = "verified"
        Path(output).write_text(json.dumps(plan, indent=2) + "\n")
        # Reused preparation runs can expire well before the plan's own 14 days.
        deadline = min(self.gh.artifacts(run_id)[name]["expires_at"] for stage, run_id in plan["runs"].items()
                       for name in self.artifact_names(stage, release))
        self.report(f"Verified all candidates for v{release} at {commit}; publish using this coordinator run ID before {deadline}")

    def validate_plan(self, plan, candidate_run_id):
        if (plan.get("schema") != 1 or plan.get("state") != "verified" or plan.get("repository") != self.gh.repository
                or set(plan.get("runs", {})) != set(WORKFLOWS)):
            raise ReleaseError("invalid or incomplete release plan")
        release = version(plan["version"])
        commit = plan["commit"]
        self.release_commit(release, commit)
        source = plan.get("coordinator_commit", "")
        if not re.fullmatch(r"[a-f0-9]{40}", source):
            raise ReleaseError("invalid coordinator source commit")
        run = self.gh.successful_run(candidate_run_id, "release-coordinate.yml", source)
        if run.get("head_branch") != "main" or self.gh.api(f"compare/{source}...main")["status"] not in ("ahead", "identical"):
            raise ReleaseError("candidate plan must come from reviewed release control on main")
        for name in (f"v{release}", f"sdk-v{release}"):
            self.verify_tag(name, commit)
        for stage, run_id in plan["runs"].items():
            self.gh.successful_run(run_id, WORKFLOWS[stage], commit, ("push", "workflow_dispatch"))
        # Keep the latest-run rule for actual CI/security/site results. An
        # older successful run cannot override a newer failed validation.
        for stage in GATES:
            latest = self.runs(WORKFLOWS[stage], commit)
            if not latest or latest[0]["status"] != "completed" or latest[0]["conclusion"] != "success":
                raise ReleaseError(f"latest {stage} validation must pass on the release commit")
        return release, commit

    def publish(self, plan, candidate_run_id, promote_site=False):
        release, commit = self.validate_plan(plan, candidate_run_id)
        with tempfile.TemporaryDirectory(prefix="leani-publish-") as directory:
            self.collect(plan, directory, compare=True)
            root = Path(directory)
            binaries = plan["artifacts"]["binaries"][f"leani-release-candidate-v{release}"]
            crates = root / "crates" / "leani-rust-library-candidate"
            sdk = root / "sdk" / "leani-sdk-candidate"
            images = {arch: (root / "container" / f"leani-container-{arch}" / "image.id").read_text().strip()
                      for arch in ("amd64", "arm64")}
            if not all(re.fullmatch(r"sha256:[a-f0-9]{64}", identity) for identity in images.values()):
                raise ReleaseError("invalid candidate image identity")
            checks = {"binaries": lambda: release_published(self.gh, binaries, release),
                      "crates": lambda: not missing_crates(crates, release),
                      "sdk": lambda: sdk_published(sdk, release),
                      "container": lambda: container_published(images, self.gh.repository, release)}
            # Check every destination before the first write. Existing versions
            # with different bytes are an error, never a reason to overwrite.
            done = {stage: check() for stage, check in checks.items()}
            for stage in ("binaries", "crates", "sdk", "container"):
                if done[stage]:
                    self.report(f"{stage}: already published; verified candidate bytes")
                    continue
                inputs = {"publish": True, "container_candidate_run_id": str(plan["runs"]["container"])}
                ref = f"sdk-v{release}" if stage == "sdk" else f"v{release}"
                if stage == "binaries":
                    inputs.update(version=f"v{release}", candidate_run_id=str(plan["runs"][stage]))
                elif stage in ("crates", "sdk"):
                    inputs.update(bootstrap=False, candidate_run_id=str(plan["runs"][stage]))
                else:
                    inputs = {"publish": True, "image_tag": f"v{release}", "candidate_run_id": str(plan["runs"][stage])}
                pending, prefix = self.resume_publication(stage, candidate_run_id, commit)
                self.wait(pending or self.dispatch(WORKFLOWS[stage], ref, inputs, prefix))
                # Registries and their CDNs can briefly lag a finished publisher.
                # ponytail: 20 checks 15 s apart; raise the count if npm needs longer.
                for _ in range(20):
                    if checks[stage]():
                        break
                    self.pause()
                else:
                    raise ReleaseError(f"{stage} publication finished but the verified artifacts are not available")
                self.report(f"{stage}: published and verified")
            if promote_site:
                branch = self.gh.api("git/ref/heads/site-production", missing=True)
                if branch and branch["object"]["sha"] == commit:
                    self.report("site: already promoted to the release commit")
                else:
                    self.wait(self.dispatch("site-promote.yml", f"v{release}", {"release_tag": f"v{release}"}))
                    branch = self.gh.api("git/ref/heads/site-production")
                    if branch["object"]["sha"] != commit:
                        raise ReleaseError("site-production did not reach the release commit")
                    self.report("site: promoted; Cloudflare deployment follows through its Git integration")
        self.report(f"Release v{release} is fully published")

    def open_pr(self, payload):
        release = version(payload["version"])
        if payload.get("schema") != 1 or not re.fullmatch(r"[a-f0-9]{40}", payload["base_sha"]):
            raise ReleaseError("invalid release preparation")
        base = payload["base_sha"]
        files = payload["files"]
        for name, contents in files.items():
            if (not isinstance(contents, str) or name.startswith("/") or ".." in Path(name).parts
                    or not (name in ("Cargo.toml", "Cargo.lock", "CHANGELOG.md", "README.md", "packages/sdk/package.json", "packages/sdk/src/index.ts",
                                     "packages/sdk/README.md", "skills/leani/references/processors.md")
                            or re.fullmatch(r"crates/[^/]+/Cargo.toml", name) or re.fullmatch(r"examples/[^/]+/Cargo.toml", name)
                            or re.fullmatch(r"docs/[\w./-]+\.mdx?", name))):
                raise ReleaseError("unexpected file in the release preparation")
        if not files:
            raise ReleaseError("empty release preparation")
        current = self.gh.api("commits/main")
        if current["sha"] != base:
            # The prepare job waits for approval while main moves on. Its files
            # still apply when main left every one of them untouched.
            comparison = self.gh.api(f"compare/{base}...{current['sha']}")
            changed = {item["filename"] for item in comparison.get("files", [])}
            # The compare API lists at most 300 files, so a list that long may be truncated.
            if comparison["status"] != "ahead" or len(changed) >= 300 or changed & files.keys():
                raise ReleaseError("main changed the prepared files during preparation; rerun prepare")
        tree = self.gh.api("git/trees", {"base_tree": current["commit"]["tree"]["sha"],
                                       "tree": [{"path": name, "mode": "100644", "type": "blob", "content": contents}
                                                for name, contents in sorted(files.items())]})
        branch = f"release/{release}"
        existing = self.gh.api(f"git/ref/heads/{branch}", missing=True)
        if existing:
            previous = self.gh.api(f"git/commits/{existing['object']['sha']}")
            if previous["tree"]["sha"] != tree["sha"] or [item["sha"] for item in previous["parents"]] != [current["sha"]]:
                raise ReleaseError(f"{branch} already contains different work; it will not be overwritten")
        else:
            commit = self.gh.api("git/commits", {"message": f"chore(release): prepare {release}", "tree": tree["sha"], "parents": [current["sha"]]})
            self.gh.api("git/refs", {"ref": f"refs/heads/{branch}", "sha": commit["sha"]})
        pulls = self.gh.api(f"pulls?state=open&head={self.gh.repository.split('/')[0]}:{branch}&base=main")
        if pulls:
            self.report(f"Release preparation PR: {pulls[0]['html_url']}")
            return
        body = (f"Prepare the coordinated Leani {release} release. Updates workspace and SDK versions, exact internal crate requirements, "
                "lockfile versions, documented installs, and the release changelog.\n\n"
                "Review the upgrade instructions and migration notes before merging. After CI passes and this PR merges, "
                f"run Coordinate release with action=candidate and version={release}; then publish its verified candidate run.\n\n"
                "This PR publishes no packages or release artifacts.")
        pull = self.gh.api("pulls", {"title": f"chore(release): prepare {release}", "head": branch, "base": "main", "body": body})
        self.report(f"Release preparation PR: {pull['html_url']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    prepare = sub.add_parser("open-pr")
    prepare.add_argument("--prepared", required=True)
    candidate = sub.add_parser("candidate")
    candidate.add_argument("version")
    candidate.add_argument("--output", required=True)
    candidate.add_argument("--commit")
    for action in ("verify-plan", "publish"):
        command_parser = sub.add_parser(action)
        command_parser.add_argument("--plan", required=True)
        command_parser.add_argument("--candidate-run-id", required=True)
        command_parser.add_argument("--version", required=True)
        if action == "publish":
            command_parser.add_argument("--promote-site", action="store_true")
    crates = sub.add_parser("missing-crates")
    crates.add_argument("--candidate", required=True)
    crates.add_argument("--version", required=True)
    args = parser.parse_args()
    coordinator = Coordinator(GitHub())
    try:
        if args.action == "open-pr":
            coordinator.open_pr(json.loads(Path(args.prepared).read_text()))
        elif args.action == "candidate":
            coordinator.candidate(args.version, args.output, args.commit)
        elif args.action == "verify-plan":
            plan = json.loads(Path(args.plan).read_text())
            if plan.get("version") != version(args.version):
                raise ReleaseError("release plan does not match the requested version")
            coordinator.validate_plan(plan, args.candidate_run_id)
            coordinator.report("Candidate plan matches its successful preparation run, release tags, and passing CI")
        elif args.action == "publish":
            plan = json.loads(Path(args.plan).read_text())
            if plan.get("version") != version(args.version):
                raise ReleaseError("release plan does not match the requested version")
            coordinator.publish(plan, args.candidate_run_id, args.promote_site)
        else:
            names = missing_crates(args.candidate, version(args.version))
            print(" ".join(f"-p {name}" for name in names))
    except (ReleaseError, KeyError, ValueError, OSError, tarfile.TarError) as error:
        print(f"release coordination: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
