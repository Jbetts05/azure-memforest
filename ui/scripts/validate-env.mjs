const devMode = process.env.VITE_UI_DEV_MODE === 'true';

const required = devMode
  ? ['VITE_API_BASE_URL', 'VITE_DEV_TENANT_ID', 'VITE_DEV_ACTOR_ID']
  : ['VITE_API_BASE_URL', 'VITE_ENTRA_TENANT_ID', 'VITE_ENTRA_CLIENT_ID', 'VITE_ENTRA_SCOPE'];

const missing = required.filter((name) => !process.env[name]?.trim());

if (missing.length) {
  console.error(`Missing required UI build environment variables: ${missing.join(', ')}`);
  process.exit(1);
}

if (!devMode && process.env.VITE_API_BASE_URL?.includes('localhost')) {
  console.error('Production UI build cannot target localhost API.');
  process.exit(1);
}
