// front/src/lib/sol-format.ts

let cachedSolPrice: number | null = null;
let lastFetchTime = 0;

// Função auxiliar para buscar o preço do SOL (pode ser chamada pelo App)
export async function fetchSolPrice(): Promise<number> {
  const now = Date.now();
  // Cache de 1 minuto para não estourar a API
  if (cachedSolPrice && (now - lastFetchTime) < 60000) {
    return cachedSolPrice;
  }
  try {
    const res = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd');
    const data = await res.json();
    cachedSolPrice = data.solana.usd;
    lastFetchTime = now;
    return cachedSolPrice;
  } catch (error) {
    console.error("Erro ao buscar cotação do SOL:", error);
    return cachedSolPrice || 119; // Fallback para 119 se a API falhar
  }
}

export function formatSol(value: number | null | undefined, digits = 9): string {
  if (value == null || !Number.isFinite(value)) return 'Indisponível';
  return value.toLocaleString('pt-BR', {
    maximumFractionDigits: digits,
    minimumFractionDigits: 0,
  });
}

// Nova função para formatar o valor em Dólar
export function formatUsdFromSol(value: number | null | undefined, solPrice: number = 119): string {
  if (value == null || !Number.isFinite(value)) return 'Indisponível';
  const usdValue = value * solPrice;
  return usdValue.toLocaleString('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 2,
    minimumFractionDigits: 2,
  });
}