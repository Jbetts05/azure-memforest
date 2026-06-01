from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "artifacts" / "durable_write_orchestrator.zip"
EXCLUDED_DIR_NAMES = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"}
EXCLUDED_SUFFIXES = {".pyc", ".pyo"}


def build_package(*, output: Path = DEFAULT_OUTPUT) -> Path:
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="memforest-durable-package-") as tmp:
        staging = Path(tmp) / "package"
        staging.mkdir()
        _copy_function_root(staging)
        _copy_src(staging / "src")
        _write_zip(staging, output)
    return output


def deploy_package(*, package: Path, resource_group: str, function_app: str) -> None:
    _run(["az", "functionapp", "show", "--resource-group", resource_group, "--name", function_app, "--query", "name", "-o", "tsv"])
    _run(
        [
            "az",
            "functionapp",
            "deploy",
            "--resource-group",
            resource_group,
            "--name",
            function_app,
            "--src-path",
            str(package),
            "--type",
            "zip",
            "--build-remote",
            "true",
        ]
    )


def _copy_function_root(staging: Path) -> None:
    source = ROOT / "durable_write_orchestrator"
    for path in source.iterdir():
        target = staging / path.name
        if path.is_dir():
            shutil.copytree(path, target, ignore=_ignore)
        else:
            shutil.copy2(path, target)


def _copy_src(target: Path) -> None:
    shutil.copytree(ROOT / "src", target, ignore=_ignore)


def _write_zip(staging: Path, output: Path) -> None:
    if output.exists():
        output.unlink()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(staging).as_posix())


def _ignore(_directory: str, names: list[str]) -> set[str]:
    ignored = set()
    for name in names:
        path = Path(name)
        if name in EXCLUDED_DIR_NAMES or path.suffix in EXCLUDED_SUFFIXES:
            ignored.add(name)
    return ignored


def _run(command: list[str]) -> None:
    subprocess.run(command, check=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Package the Durable write orchestrator Function App.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--deploy", action="store_true", help="Deploy the zip after packaging. Requires --resource-group and --function-app.")
    parser.add_argument("--resource-group")
    parser.add_argument("--function-app")
    args = parser.parse_args(argv)
    package = build_package(output=args.output)
    print(package)
    if args.deploy:
        if not args.resource_group or not args.function_app:
            parser.error("--deploy requires --resource-group and --function-app")
        deploy_package(package=package, resource_group=args.resource_group, function_app=args.function_app)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
