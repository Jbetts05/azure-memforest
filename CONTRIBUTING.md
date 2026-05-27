# Contributing to Azure-MemForest

## Python environment

Always use a repository-local virtual environment. Do not install dependencies into local/global Python.

```powershell
python -m venv .venv
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest -q
```

Use the `.venv` interpreter explicitly in commands and automation. The `.venv/` directory is intentionally ignored by git.

## Running the local service

The HTTP service factory is intentionally side-effect-light and does not load model clients or user state at import time.

```powershell
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m uvicorn src.service.app:create_app --factory --host 127.0.0.1 --port 8000
```

## Azure deployment safety

Do not create, modify, or deploy Azure resources until the deployment decisions in `docs/azure-decisions.md` are confirmed.
