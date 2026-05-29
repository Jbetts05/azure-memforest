# Azure-MemForest UI demo guide

## Hosted dev UI

The dev UI is hosted at:

```text
https://memforest-ui.example.invalid
```

The API CORS allowlist is configured for this origin when `enableUiFeature=true`.

## Authentication

The UI uses MSAL in normal mode and requests an access token for the Azure-MemForest API scope.

Required Vite build variables:

```text
VITE_API_BASE_URL=https://memforest-api.example.invalid
VITE_ENTRA_TENANT_ID=11111111-1111-4111-8111-111111111111
VITE_ENTRA_CLIENT_ID=33333333-3333-4333-8333-333333333333
VITE_ENTRA_SCOPE=api://33333333-3333-4333-8333-333333333333/access_as_user
```

The Entra app registration includes the Static Web App origin as a SPA redirect URI.

Development mode is available only for local/smoke work:

```text
VITE_UI_DEV_MODE=true
VITE_DEV_TENANT_ID=smoke-tenant
VITE_DEV_ACTOR_ID=smoke-user
```

Do not use dev mode for production demos.

## Demo flow

1. Open the hosted UI.
2. Sign in with Entra.
3. Confirm the shell loads with Sessions, Forest, Trace, Facts, and Ops sections.
4. For a user with committed memory, verify:
   - sessions load;
   - latest forest and roots load;
   - query returns answer and facts;
   - trace panel shows event summaries;
   - fact inspector shows selected fact and locations when available.
5. Submit an ingest from the Chat Workspace.
6. Wait for the job status to reach `succeeded`.
7. Confirm sessions and forest version refresh.

## Current MVP limitations

- The UI visualizes root-level tree data and trace event summaries; pan/zoom and deep lazy expansion are future work.
- Session selection is minimal.
- Ops status uses API/manifest/auth state; full Azure metrics dashboards remain in `docs/production-readiness.md`.
- Dev seed data belongs to `smoke-tenant/smoke-user`; Entra users see only their own OID-scoped memory unless granted admin behavior.

## Validation commands

```powershell
Push-Location ui
npm install
npm run test
npm run build
Pop-Location
```

Backend/API validation:

```powershell
$env:PIP_REQUIRE_VIRTUALENV = "true"
.\.venv\Scripts\python.exe -m pytest -q
az bicep build --file infra\main.bicep --stdout > $null
```

