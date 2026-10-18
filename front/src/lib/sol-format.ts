export function formatSol(value: number | null | undefined, digits = 9): string {
  if (value == null || !Number.isFinite(value)) return 'Indisponível';
  return value.toLocaleString('pt-BR', {
    maximumFractionDigits: digits,
    minimumFractionDigits: 0,
  });
}
