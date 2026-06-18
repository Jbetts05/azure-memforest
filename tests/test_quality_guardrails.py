from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_PATHS = [
    ROOT / "src" / "build",
    ROOT / "src" / "cloud",
    ROOT / "src" / "config",
    ROOT / "src" / "extraction",
    ROOT / "src" / "forest",
    ROOT / "src" / "prompt",
    ROOT / "src" / "query",
    ROOT / "src" / "service",
    ROOT / "src" / "worker",
]
OFFLINE_ALLOWED = {
    ROOT / "src" / "extraction" / "runner.py",
}
FORBIDDEN_RUNTIME_MARKERS = {
    "gold_answer",
    "gold answer",
    "gold date",
}


def test_runtime_code_has_no_gold_answer_markers() -> None:
    offenders: list[str] = []
    for base in RUNTIME_PATHS:
        for path in base.rglob("*.py"):
            if path in OFFLINE_ALLOWED:
                continue
            text = path.read_text(encoding="utf-8").lower()
            hits = [marker for marker in FORBIDDEN_RUNTIME_MARKERS if marker in text]
            if hits:
                rel = path.relative_to(ROOT)
                offenders.append(f"{rel}: {', '.join(sorted(hits))}")

    assert offenders == []
