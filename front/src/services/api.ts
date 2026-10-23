import { setBaseUrl } from '@/api-client';

export const apiBaseUrl = import.meta.env.VITE_API_BASE_URL?.trim().replace(/\/$/, '') || 'http://54.196.187.210:8080';
const configuredBase = apiBaseUrl;
setBaseUrl(apiBaseUrl);

export const hasConfiguredApiBase = Boolean(configuredBase);
export const apiConnectionLabel = configuredBase ? 'Destino da API: remoto configurado' : 'Destino da API: mesmo domínio';
