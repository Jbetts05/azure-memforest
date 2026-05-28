# Authentication plan

Azure-MemForest service routes fail closed unless an auth provider is configured.

## Current providers

- `DevHeaderAuthProvider`: local/test/dev only; never use in production.
- `EntraJwtAuthProvider`: validates Microsoft Entra ID JWT access tokens.

## Entra configuration

The Entra provider is enabled only when these environment variables are present:

```text
ENTRA_TENANT_ID=<tenant-guid>
ENTRA_AUDIENCES=<comma-separated accepted API audiences>
```

`ENTRA_TENANT_ID` must be the tenant GUID, not a domain name.

The provider validates:

- Bearer token format;
- RS256 signature using the tenant JWKS;
- issuer from the tenant v2.0 metadata document;
- audience in `ENTRA_AUDIENCES`;
- `tid` claim equals the configured tenant;
- `ver` claim equals `2.0`;
- `exp`, `nbf`, `iat`, `aud`, and `iss` claims.

Actor identity is `oid` when present, otherwise `sub`. For the current API contract, `{user_id}` path values must use this actor ID unless the token includes the `memory.admin` role.

## App registration

An Entra app registration has not been created by automation yet. Creating it should be an explicit operator action because it is a tenant-level security change.

When created, configure the API app registration so access tokens are v2 tokens. In the app manifest this means setting `accessTokenAcceptedVersion` to `2`. Then set the accepted audience values in `ENTRA_AUDIENCES`.
