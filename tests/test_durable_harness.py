from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path

from scripts.package_durable_orchestrator import build_package


def test_durable_orchestrator_package_contains_function_root_and_src(tmp_path: Path) -> None:
    package = build_package(output=tmp_path / "durable.zip")

    assert package.exists()
    with zipfile.ZipFile(package) as archive:
        names = set(archive.namelist())

    assert "function_app.py" in names
    assert "host.json" in names
    assert "requirements.txt" in names
    assert "src/worker/durable_activity.py" in names
    assert "src/service/jobs.py" in names
    assert "functions.metadata" not in names
    assert not any(name.endswith("function.json") for name in names)
    assert not any("__pycache__" in name for name in names)
    assert not any(name.endswith(".pyc") for name in names)


def test_durable_orchestrator_requirements_are_curated() -> None:
    requirements = Path("durable_write_orchestrator/requirements.txt").read_text(encoding="utf-8").lower()

    assert "azure-functions==" in requirements
    assert "azure-functions-durable==" in requirements
    assert "azure-cosmos==" in requirements
    assert "azure-search-documents==" in requirements
    assert "faiss-cpu==" in requirements
    assert "fastapi" not in requirements
    assert "uvicorn" not in requirements
    assert "pytest" not in requirements


def test_durable_activity_import_does_not_load_api_or_faiss_stack() -> None:
    code = """
import sys
import src.worker.durable_activity
forbidden = ['fastapi', 'faiss', 'uvicorn', 'src.service.app']
loaded = [name for name in forbidden if name in sys.modules]
if loaded:
    raise SystemExit('forbidden imports loaded: ' + ','.join(loaded))
"""
    subprocess.run([sys.executable, "-c", code], check=True)
