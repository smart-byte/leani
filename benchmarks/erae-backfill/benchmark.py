#!/usr/bin/env python3
"""Alternate baseline/candidate EraE backfills and verify delivered-output digests."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PROCESSORS = ["block-summary", "blobs-money", "uniswap-observations"]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--candidate-binary", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=ROOT / "config/benchmarks/real-source.toml")
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--processors", nargs="+", choices=PROCESSORS, default=PROCESSORS)
    parser.add_argument("--from-block", type=int, default=19_426_589)
    parser.add_argument("--to-block", type=int, default=19_430_684)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args()
    if args.runs < 1 or args.timeout_seconds < 1 or not 0 <= args.from_block <= args.to_block:
        parser.error("runs and timeout must be positive, and the block range must be ordered")
    for field in ["baseline_binary", "candidate_binary", "config", "output_directory"]:
        setattr(args, field, getattr(args, field).expanduser().resolve())
    for binary in [args.baseline_binary, args.candidate_binary]:
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(f"binary is not executable: {binary}")
    args.processors = list(dict.fromkeys(args.processors))
    return args


def prepare(args: argparse.Namespace) -> dict:
    identity = {
        "schema": "leani.erae-backfill-comparison.v1",
        "binaries": {
            label: {"path": str(binary), "sha256": sha256(binary)}
            for label, binary in [("baseline", args.baseline_binary), ("candidate", args.candidate_binary)]
        },
        "config": {"path": str(args.config), "sha256": sha256(args.config)},
        "processors": args.processors,
        "range": {"start": args.from_block, "end": args.to_block},
        "runs": args.runs,
        "timeout_seconds": args.timeout_seconds,
        "host": {"platform": platform.platform(), "machine": platform.machine(), "cpus": os.cpu_count()},
    }
    args.output_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    manifest = args.output_directory / "manifest.json"
    if manifest.exists():
        previous = json.loads(manifest.read_text())
        if previous["identity"] != identity:
            raise SystemExit("binary, configuration, host, or run settings changed; choose a new output directory")
    else:
        with manifest.open("x") as output:
            json.dump({"started_utc": datetime.now(timezone.utc).isoformat(), "identity": identity}, output, indent=2)
            output.write("\n")
    return identity


def measure(args: argparse.Namespace, label: str, processor: str, run: int, expected: str | None) -> dict:
    name = f"{run:02d}-{processor}-{label}"
    report_path = args.output_directory / f"{name}.json"
    data_path = args.output_directory / f"{name}-data"
    if not report_path.exists():
        if data_path.exists():
            raise SystemExit(f"incomplete run at {data_path}; inspect it and choose a new output directory")
        binary = args.baseline_binary if label == "baseline" else args.candidate_binary
        command = [
            str(binary), "--config", str(args.config), "benchmark", "real-source",
            "--processor", processor, "--source-policy", "erae-only",
            "--from-block", str(args.from_block), "--to-block", str(args.to_block),
            "--data-dir", str(data_path), "--report", str(report_path),
            "--timeout-seconds", str(args.timeout_seconds),
        ]
        if expected:
            command += ["--expected-output-digest", expected]
        print(f"run: {name}", flush=True)
        log_path = args.output_directory / f"{name}.log"
        with log_path.open("x") as log:
            result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                    timeout=args.timeout_seconds + 60, check=False)
        if result.returncode:
            raise SystemExit(f"benchmark failed ({result.returncode}); inspect {log_path}")
    else:
        print(f"resume: {name}", flush=True)
    report = json.loads(report_path.read_text())
    if report.get("status") != "passed" or not report.get("correctnessPassed"):
        raise SystemExit(f"benchmark correctness failed: {report_path}")
    if report["range"] != {"start": args.from_block, "end": args.to_block} or report["sourcePolicy"] != "erae_only":
        raise SystemExit(f"unexpected benchmark range or source: {report_path}")
    if processor in ["block-summary", "blobs-money"] and report["delivery"]["domainEvents"] != args.to_block - args.from_block + 1:
        raise SystemExit(f"benchmark must hash one delivered domain event per block: {report_path}")
    if expected and report["outputDigest"] != expected:
        raise SystemExit(f"delivered-output digest differs: {report_path}")
    metrics = [source["metrics"] for source in report["sources"]]
    return {
        "run": run, "processor": processor, "label": label, "report": report_path.name,
        "elapsed_seconds": report["elapsedMilliseconds"] / 1000,
        "blocks_per_second": report["blocksPerSecondMilli"] / 1000,
        "first_frame_ms": report["timeToFirstSourceFrameMs"],
        "peak_rss_bytes": report["peakRssBytes"],
        "physical_reads": sum(source["physical_reads"] for source in metrics),
        "fetched_bytes": sum(source["fetched_bytes"] for source in metrics),
        "normalized_bytes": sum(source["normalized_bytes"] for source in metrics),
        "output_digest": report["outputDigest"],
    }


def summarize(args: argparse.Namespace, results: list[dict]) -> None:
    lines = ["# EraE backfill comparison", "",
             f"Range: {args.from_block:,}–{args.to_block:,}. Each cell uses the median of completed runs.", "",
             "| Processor | Baseline seconds | Candidate seconds | Speedup |",
             "| --- | ---: | ---: | ---: |"]
    for processor in args.processors:
        groups = {label: [row for row in results if row["processor"] == processor and row["label"] == label]
                  for label in ["baseline", "candidate"]}
        if not all(groups.values()):
            continue
        medians = {label: statistics.median(row["elapsed_seconds"] for row in rows)
                   for label, rows in groups.items()}
        speedup = medians["baseline"] / medians["candidate"]
        lines.append(f"| {processor} | {medians['baseline']:.3f} | {medians['candidate']:.3f} | {speedup:.2f}× |")
    lines += ["", "All completed runs passed pipeline correctness and matched their processor's baseline output digest.", "",
              "Public-mirror cache state and bandwidth are uncontrolled. See manifest.json for binary/configuration hashes, "
              "results.json for per-run metrics, and the individual reports for delivery and resource measurements.", ""]
    (args.output_directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    (args.output_directory / "summary.md").write_text("\n".join(lines))


def main() -> None:
    args = arguments()
    prepare(args)
    expected: dict[str, str] = {}
    results = []
    for run in range(1, args.runs + 1):
        order = ["baseline", "candidate"] if run % 2 else ["candidate", "baseline"]
        for processor in args.processors:
            for label in order:
                result = measure(args, label, processor, run, expected.get(processor))
                expected.setdefault(processor, result["output_digest"])
                results.append(result)
                summarize(args, results)
                print(f"{label} / {processor}: {result['elapsed_seconds']:.3f}s, "
                      f"{result['blocks_per_second']:.1f} blocks/s; output digest verified", flush=True)


if __name__ == "__main__":
    main()
