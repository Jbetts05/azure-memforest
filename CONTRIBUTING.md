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

## Public repository hygiene

Deployment names, account identifiers, hosts, and firewall addresses in committed
examples and historical experiment artifacts are sanitized examples, not live
configuration. Set your own endpoints and identity values before running a cloud
harness or following a deployment command. Historical image and commit references
are provenance records, not deployment instructions.

Keep credentials and telemetry connection strings in environment variables,
managed configuration, or a secret store. Never commit Azure CLI settings dumps,
access tokens, signed URLs, private keys, or unsanitized user-memory exports.
Local `.env` files, Functions `local.settings.json`, and private-key containers
are ignored; `.env.example` must contain placeholders only.

Before sharing a branch, scan both its files and its complete Git history for
secrets. Removing a value in a later commit does not remove its earlier copies.
If a real credential was committed, revoke or rotate it before coordinating a
history rewrite. Keep recovery bundles and unsanitized experiment outputs outside
the repository; do not push recovery refs or restore old history into a sanitized
branch.
