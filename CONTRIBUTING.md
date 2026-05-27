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

## Azure deployment safety

Do not create, modify, or deploy Azure resources until the deployment decisions in `docs/azure-decisions.md` are confirmed.
