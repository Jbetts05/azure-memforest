import { PublicClientApplication } from '@azure/msal-browser';
import type { ApiConfig } from './api';

const devMode = import.meta.env.VITE_UI_DEV_MODE === 'true';

export async function getApiConfig(): Promise<ApiConfig> {
  const baseUrl = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000';
  const userId = import.meta.env.VITE_USER_ID ?? import.meta.env.VITE_DEV_ACTOR_ID ?? 'smoke-user';
  if (devMode) {
    return {
      baseUrl,
      userId,
      devMode: true,
      devTenantId: import.meta.env.VITE_DEV_TENANT_ID ?? 'smoke-tenant',
      devActorId: import.meta.env.VITE_DEV_ACTOR_ID ?? userId
    };
  }

  const clientId = import.meta.env.VITE_ENTRA_CLIENT_ID;
  const tenantId = import.meta.env.VITE_ENTRA_TENANT_ID;
  const scope = import.meta.env.VITE_ENTRA_SCOPE;
  if (!clientId || !tenantId || !scope) throw new Error('Missing Entra UI configuration.');
  const msal = new PublicClientApplication({
    auth: {
      clientId,
      authority: `https://login.microsoftonline.com/${tenantId}`,
      redirectUri: window.location.origin
    }
  });
  await msal.initialize();
  const accounts = msal.getAllAccounts();
  const account = accounts[0] ?? (await msal.loginPopup({ scopes: [scope] })).account;
  if (!account) throw new Error('No signed-in account.');
  const token = await msal.acquireTokenSilent({ account, scopes: [scope] }).catch(() => msal.acquireTokenPopup({ account, scopes: [scope] }));
  return { baseUrl, userId: account.localAccountId, accessToken: token.accessToken };
}
