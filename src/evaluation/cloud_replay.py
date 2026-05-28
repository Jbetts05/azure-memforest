"""Scaffold for Azure-MemForest cloud benchmark replay artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Iterable

REQUIRED_COLUMNS = [
    "method",
    "model_size",
    "question_type",
    "qid",
    "question",
    "gold_answer",
]


def scaffold_candidate_csvs(*, source_paths: Iterable[Path], output_dir: Path, method: str) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for source in source_paths:
        target = output_dir / Path(source).name
        with Path(source).open(encoding="utf-8", newline="") as src, target.open("w", encoding="utf-8", newline="") as dst:
            reader = csv.DictReader(src)
            if not reader.fieldnames:
                raise ValueError(f"Benchmark CSV has no header: {source}")
            missing = [column for column in REQUIRED_COLUMNS if column not in reader.fieldnames]
            if missing:
                raise ValueError(f"Benchmark CSV {source} is missing required columns: {missing}")
            fieldnames = REQUIRED_COLUMNS + [f"answer_{i}" for i in range(1, 9)] + [f"judge_{i}" for i in range(1, 9)]
            writer = csv.DictWriter(dst, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                out = {column: row.get(column, "") for column in REQUIRED_COLUMNS}
                out["method"] = method
                for i in range(1, 9):
                    out[f"answer_{i}"] = ""
                    out[f"judge_{i}"] = ""
                writer.writerow(out)
        written.append(target)
    return written


def write_replay_manifest(*, output_dir: Path, source_paths: Iterable[Path], method: str, notes: str = "") -> Path:
    manifest = {
        "method": method,
        "source_files": [Path(path).as_posix() for path in source_paths],
        "notes": notes,
        "status": "scaffolded",
    }
    path = output_dir / "cloud_replay_manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Scaffold cloud replay candidate CSVs with benchmark-compatible columns.")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--method", default="memforest-cloud")
    parser.add_argument("--notes", default="")
    parser.add_argument("source_paths", nargs="+", type=Path)
    args = parser.parse_args(argv)

    written = scaffold_candidate_csvs(source_paths=args.source_paths, output_dir=args.output_dir, method=args.method)
    manifest = write_replay_manifest(output_dir=args.output_dir, source_paths=args.source_paths, method=args.method, notes=args.notes)
    print(json.dumps({"written": [path.as_posix() for path in written], "manifest": manifest.as_posix()}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
