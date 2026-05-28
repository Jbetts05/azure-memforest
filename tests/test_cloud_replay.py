from __future__ import annotations

import csv
from pathlib import Path

from src.evaluation.cloud_replay import scaffold_candidate_csvs, write_replay_manifest


def test_scaffold_candidate_csvs_preserves_shape_with_empty_answers(tmp_path: Path) -> None:
    source = tmp_path / "bench.csv"
    source.write_text(
        "method,model_size,question_type,qid,question,gold_answer,answer_1,judge_1\n"
        "memforest,4b,type,q1,Where?,Gold,Old,1\n",
        encoding="utf-8",
    )

    output_dir = tmp_path / "out"
    written = scaffold_candidate_csvs(source_paths=[source], output_dir=output_dir, method="memforest-cloud")

    with written[0].open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["method"] == "memforest-cloud"
    assert rows[0]["qid"] == "q1"
    assert rows[0]["answer_1"] == ""
    assert rows[0]["judge_8"] == ""


def test_write_replay_manifest(tmp_path: Path) -> None:
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    path = write_replay_manifest(output_dir=output_dir, source_paths=[Path("benchmark/file.csv")], method="cloud", notes="note")

    text = path.read_text(encoding="utf-8")
    assert '"method": "cloud"' in text
    assert '"status": "scaffolded"' in text
