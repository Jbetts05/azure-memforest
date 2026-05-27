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
$env:HOST = "127.0.0.1"
.\.venv\Scripts\python.exe -m src.service
```

The service container can be built locally without pushing to a registry:

```powershell
docker build --pull --tag azure-memforest-api:local .
docker run --rm -p 8000:8000 azure-memforest-api:local
```

`/health/live` should return 200. `/health/ready` intentionally returns 503 until readiness checks are wired through `create_app(readiness_checks=...)`. The service supports sanitized `x-request-id` correlation and parses W3C `traceparent` for log correlation, but it does not log request or response bodies by default.

## Azure deployment safety

Do not create, modify, or deploy Azure resources until the deployment decisions in `docs/azure-decisions.md` are confirmed.
