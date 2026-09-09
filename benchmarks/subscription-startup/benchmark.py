#!/usr/bin/env python3
"""Compare cold and warm embedded block-subscription startup."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import signal
import sqlite3
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BLOCK_LINE = re.compile(r"\bblock=\d+\b.*\btxs=\d+\b")
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--candidate-binary", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--baseline-label", default="baseline")
    parser.add_argument("--candidate-label", default="candidate")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--cutoff-seconds", type=float, default=90.0)
    parser.add_argument("--shutdown-grace-seconds", type=float, default=5.0)
    parser.add_argument("--inter-run-seconds", type=float, default=2.0)
    return parser.parse_args()


def fail(message: str) -> None:
    raise SystemExit(f"error: {message}")


def validate_label(label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", label):
        fail(f"invalid build label: {label!r}")
    return label


def validate_binary(path: Path) -> Path:
    path = path.expanduser().resolve()
    if not path.is_file() or not os.access(path, os.X_OK):
        fail(f"binary is not executable: {path}")
    return path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def command_output(command: list[str]) -> str | None:
    try:
        return subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def binary_metadata(path: Path) -> dict[str, str | None]:
    return {
        "path": str(path),
        "sha256": sha256(path),
        "version": command_output([str(path), "--version"]),
    }


def manifest_for(
    builds: dict[str, Path], args: argparse.Namespace
) -> dict[str, Any]:
    git_status = command_output(["git", "status", "--porcelain"])
    return {
        "schema_version": 1,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "repository": {
            "root": str(REPOSITORY_ROOT),
            "commit": command_output(["git", "rev-parse", "HEAD"]),
            "dirty": bool(git_status) if git_status is not None else None,
        },
        "host": {
            "platform": platform.platform(),
            "python": sys.version.split()[0],
        },
        "builds": {label: binary_metadata(path) for label, path in builds.items()},
        "settings": {
            "runs": args.runs,
            "cutoff_seconds": args.cutoff_seconds,
            "shutdown_grace_seconds": args.shutdown_grace_seconds,
            "inter_run_seconds": args.inter_run_seconds,
            "command": [
                "subscribe",
                "blocks",
                "--mode",
                "embedded",
                "--format",
                "pretty",
                "--once",
                "--yes",
                "--data-dir",
                "<data-directory>",
            ],
        },
    }


def prepare_manifest(path: Path, expected: dict[str, Any]) -> None:
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            fail(f"cannot read existing manifest {path}: {error}")
        for field in ("builds", "settings"):
            if existing.get(field) != expected[field]:
                fail(f"existing manifest has different {field}: {path}")
        return
    path.write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n")


def load_completed(path: Path) -> tuple[set[tuple[str, int, str]], list[dict[str, Any]]]:
    completed: set[tuple[str, int, str]] = set()
    results: list[dict[str, Any]] = []
    if not path.exists():
        return completed, results
    try:
        lines = path.read_text().splitlines()
    except OSError as error:
        fail(f"cannot read existing results {path}: {error}")
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            result = json.loads(line)
            key = (result["build"], int(result["pair"]), result["phase"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            fail(f"invalid result at {path}:{line_number}: {error}")
        if key in completed:
            fail(f"duplicate result for {key} in {path}")
        completed.add(key)
        results.append(result)
    return completed, results


def candidate_count(data_directory: Path) -> tuple[int | None, str | None]:
    sqlite_path = data_directory / "execution-network.sqlite"
    if sqlite_path.exists():
        try:
            with sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True) as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM execution_peer_candidates WHERE chain_id = 1"
                ).fetchone()[0]
            return int(count), None
        except (OSError, sqlite3.Error) as error:
            return None, f"{sqlite_path.name}: {error}"

    json_path = data_directory / "execution-peers.json"
    if json_path.exists():
        try:
            records = json.loads(json_path.read_text())
            if isinstance(records, list):
                return len(records), None
            if isinstance(records, dict):
                candidates = records.get("candidates", records)
                return len(candidates), None
            return None, f"{json_path.name}: unsupported JSON shape"
        except (OSError, json.JSONDecodeError) as error:
            return None, f"{json_path.name}: {error}"
    return 0, None


def append_result(path: Path, result: dict[str, Any]) -> None:
    with path.open("a") as output:
        output.write(json.dumps(result, sort_keys=True) + "\n")
        output.flush()
        os.fsync(output.fileno())


def stop_process(process: subprocess.Popen[str], grace_seconds: float) -> None:
    try:
        os.killpg(process.pid, signal.SIGINT)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def run_once(
    label: str,
    binary: Path,
    pair: int,
    phase: str,
    output_directory: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    data_directory = output_directory / "runs" / label / f"pair-{pair:02d}" / "data"
    log_path = output_directory / "runs" / label / f"pair-{pair:02d}-{phase}.log"
    if phase == "cold" and data_directory.exists() and any(data_directory.iterdir()):
        fail(f"cold data directory is not empty; choose a new output directory: {data_directory}")
    if phase == "warm" and not data_directory.is_dir():
        fail(f"warm run has no completed cold-state directory: {data_directory}")
    data_directory.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    command = [
        str(binary),
        "subscribe",
        "blocks",
        "--mode",
        "embedded",
        "--format",
        "pretty",
        "--once",
        "--yes",
        "--data-dir",
        str(data_directory),
    ]
    started = time.monotonic()
    first_data: list[float] = []
    with log_path.open("w", buffering=1) as log:
        process = subprocess.Popen(
            command,
            cwd=REPOSITORY_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            bufsize=1,
            start_new_session=True,
        )

        def read_output() -> None:
            assert process.stdout is not None
            for line in process.stdout:
                elapsed = time.monotonic() - started
                log.write(f"[{elapsed:09.3f}s] {line}")
                if not first_data and BLOCK_LINE.search(line):
                    first_data.append(elapsed)

        reader = threading.Thread(target=read_output, daemon=True)
        reader.start()
        timed_out = False
        try:
            process.wait(timeout=args.cutoff_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            stop_process(process, args.shutdown_grace_seconds)
        except KeyboardInterrupt:
            stop_process(process, args.shutdown_grace_seconds)
            reader.join(timeout=2.0)
            raise
        reader.join(timeout=2.0)

    count, count_error = candidate_count(data_directory)
    result = {
        "build": label,
        "pair": pair,
        "phase": phase,
        "first_data_seconds": round(first_data[0], 3) if first_data else None,
        "process_seconds": round(time.monotonic() - started, 3),
        "timed_out": timed_out,
        "exit_code": process.returncode,
        "candidate_count": count,
        "candidate_count_error": count_error,
        "log": str(log_path),
        "data_directory": str(data_directory),
    }
    observed = (
        f"{result['first_data_seconds']:.3f}s"
        if result["first_data_seconds"] is not None
        else f">{args.cutoff_seconds:g}s"
    )
    print(
        f"{label:12s} pair={pair:02d} {phase:4s} first_data={observed:>8s} "
        f"exit={process.returncode:4d} candidates={count}",
        flush=True,
    )
    return result


def write_summary(
    path: Path,
    results: list[dict[str, Any]],
    labels: list[str],
    cutoff_seconds: float,
) -> None:
    lines = [
        "# Subscription startup benchmark",
        "",
        "Times are measured from process start to the first block-summary output. "
        f"Missing observations are censored at {cutoff_seconds:g} seconds.",
        "",
        "| Build | Start | Censored median | Observed | Within cutoff |",
        "|---|---|---:|---:|---:|",
    ]
    for label in labels:
        for phase in ("cold", "warm"):
            group = [
                result
                for result in results
                if result["build"] == label and result["phase"] == phase
            ]
            values = [
                result["first_data_seconds"]
                if result["first_data_seconds"] is not None
                else cutoff_seconds
                for result in group
            ]
            observed = sum(result["first_data_seconds"] is not None for result in group)
            median = f"{statistics.median(values):.3f} s" if values else "n/a"
            lines.append(
                f"| {label} | {phase.title()} | {median} | {observed}/{len(group)} | "
                f"{sum(value < cutoff_seconds for value in values)}/{len(group)} |"
            )
    lines.extend(["", "Raw observations use `null` for censored runs in `results.ndjson`.", ""])
    temporary = path.with_suffix(".tmp")
    temporary.write_text("\n".join(lines))
    temporary.replace(path)


def main() -> None:
    args = arguments()
    if os.name != "posix":
        fail("this network-process benchmark currently requires POSIX process groups")
    if args.runs < 1 or args.cutoff_seconds <= 0 or args.shutdown_grace_seconds <= 0:
        fail("runs and timeout values must be positive")
    if args.inter_run_seconds < 0:
        fail("inter-run seconds cannot be negative")

    labels = [validate_label(args.baseline_label), validate_label(args.candidate_label)]
    if labels[0] == labels[1]:
        fail("baseline and candidate labels must differ")
    builds = {
        labels[0]: validate_binary(args.baseline_binary),
        labels[1]: validate_binary(args.candidate_binary),
    }
    output_directory = args.output_directory.expanduser().resolve()
    output_directory.mkdir(parents=True, exist_ok=True)
    prepare_manifest(output_directory / "manifest.json", manifest_for(builds, args))
    results_path = output_directory / "results.ndjson"
    completed, results = load_completed(results_path)

    for pair in range(1, args.runs + 1):
        order = labels if pair % 2 else list(reversed(labels))
        for label in order:
            for phase in ("cold", "warm"):
                key = (label, pair, phase)
                if key in completed:
                    print(f"resume: keeping {label} pair={pair:02d} {phase}")
                    continue
                result = run_once(
                    label, builds[label], pair, phase, output_directory, args
                )
                append_result(results_path, result)
                results.append(result)
                completed.add(key)
                write_summary(
                    output_directory / "summary.md", results, labels, args.cutoff_seconds
                )
                if args.inter_run_seconds:
                    time.sleep(args.inter_run_seconds)
    write_summary(output_directory / "summary.md", results, labels, args.cutoff_seconds)


if __name__ == "__main__":
    main()
