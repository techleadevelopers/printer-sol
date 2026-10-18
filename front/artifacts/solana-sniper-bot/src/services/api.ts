import { setBaseUrl } from '@workspace/api-client-react';

const configuredBase = import.meta.env.VITE_API_BASE_URL?.trim();
setBaseUrl(configuredBase || null);

export const hasConfiguredApiBase = Boolean(configuredBase);
export const apiConnectionLabel = configuredBase ? 'Destino da API: remoto configurado' : 'Destino da API: mesmo domínio';
