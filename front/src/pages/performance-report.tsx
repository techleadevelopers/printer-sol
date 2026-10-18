import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { Activity, CheckCircle2, RefreshCw, XCircle } from 'lucide-react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { getGetDashboardQueryKey, useGetDashboard } from '@/api-client';
import { customFetch } from '@/api-client/custom-fetch';
import { formatSol } from '@/lib/sol-format';

type ReportSummary = {
  startedAt: string | null;
  balanceStartSol: number | null;
  balanceNowSol: number | null;
  totalTrades: number;
  wins: number;
  losses: number;
  winRate: number;
  avgWinSol: number;
  avgLossSol: number;
  grossPnlSol: number;
  feesPaidSol: number;
  allFeesPaidSol: number;
  netPnlSol: number;
  avgHoldSeconds: number;
  maxDrawdownSol: number;
  evPerTradeSol: number;
  sharpeRatio: number | null;
  profitFactor: number | null;
  exitReasons: Record<string, number>;
  longestWinStreak: number;
  longestLossStreak: number;
};

type ReportTrade = {
  id: string;
  mint: string;
  side: string;
  entrySol: number;
  exitSol: number;
  grossPnlSol: number;
  feesSol: number;
  netPnlSol: number;
  holdSec: number;
  exitReason: string;
  confirmedAt: string;
};

type ReadinessCheck = {
  key: string;
  label: string;
  passed: boolean;
  actual: string;
  required: string;
};

type PaperReadiness = {
  available: boolean;
  ready: boolean;
  checks?: ReadinessCheck[];
  blockers?: string[];
  error?: string;
};

type PerformanceReport = {
  mode: 'simulation' | 'real';
  period: { from: string; to: string };
  summary: ReportSummary;
  trades: ReportTrade[];
  cumulativeCurve: { timestamp: string; cumulativePnlSol: number }[];
  byHour: { hour: string; trades: number; netPnlSol: number }[];
  allFeesPaidSol: number;
  openPnlSol: number | null;
  openPnlComplete?: boolean;
  unmarkedOpenPositions?: number;
  openPnlBasis: string;
  feeModel: string;
  paperReadiness: PaperReadiness;
};

const number = (value: number | null | undefined, digits = 4) =>
  value == null || !Number.isFinite(value)
    ? 'Indisponível'
    : value.toLocaleString('pt-BR', { maximumFractionDigits: digits });
const numberSol = formatSol;
const date = (value?: string | null) =>
  value ? new Intl.DateTimeFormat('pt-BR', { dateStyle: 'medium', timeStyle: 'short' }).format(new Date(value)) : 'Indisponível';
const reasons: Record<string, string> = {
  take_profit: 'Take profit',
  trailing_stop: 'Trailing stop',
  stop_loss: 'Stop loss',
  max_hold: 'Tempo máximo',
  other: 'Outros',
};

function dateBoundary(value: string, endExclusive: boolean) {
  if (!value) return '';
  const boundary = new Date(`${value}T00:00:00.000Z`);
  if (endExclusive) boundary.setUTCDate(boundary.getUTCDate() + 1);
  return boundary.toISOString();
}

function Metric({ label, value, suffix = 'SOL', tone }: { label: string; value: string; suffix?: string; tone?: string }) {
  return <div className="panel metric p-4"><div className="eyebrow">{label}</div><div className={`metric-value mt-3 ${tone || ''}`}>{value}<span className="ml-1 text-xs font-normal muted">{suffix}</span></div></div>;
}

export default function PerformanceReportPage() {
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const currentDashboard = useGetDashboard({
    query: { queryKey: getGetDashboardQueryKey(), refetchInterval: 5_000 },
  });
  const fromIso = dateBoundary(from, false);
  const toIso = dateBoundary(to, true);
  const query = new URLSearchParams();
  if (fromIso) query.set('from', fromIso);
  if (toIso) query.set('to', toIso);
  const queryString = query.toString();
  const report = useQuery({
    queryKey: ['performance-report', from, to],
    queryFn: ({ signal }) => customFetch<PerformanceReport>(`/api/performance/report${queryString ? `?${queryString}` : ''}`, { signal }),
    refetchInterval: 5_000,
  });
  const data = report.data;
  const summary = data?.summary;
  const exitRows = Object.entries(summary?.exitReasons ?? {});
  const line = data?.cumulativeCurve ?? [];
  const hours = data?.byHour.slice(-12) ?? [];
  const readiness = data?.paperReadiness;
  const closedFees = summary?.feesPaidSol ?? 0;
  const currentTotalPnl = currentDashboard.data?.totalEstimatedPnlSol ?? currentDashboard.data?.netPnlSol;

  return <div className="page-wrap enter">
    <div className="mb-6">
      <div className="eyebrow">CENTRO DE OPERAÇÕES</div>
      <h1 className="display mb-1 mt-2 text-2xl font-semibold">Relatório de desempenho</h1>
      <p className="m-0 text-sm muted">Resultado fechado, custos, curva acumulada e critérios paper de liberação para live.</p>
    </div>

    <section className="panel mb-4 grid gap-3 p-4 sm:grid-cols-[1fr_1fr_auto] sm:items-end">
      <label className="text-xs muted">De<input aria-label="Data inicial do relatório" type="date" className="input mt-1" value={from} onChange={event => setFrom(event.target.value)} /></label>
      <label className="text-xs muted">Até<input aria-label="Data final do relatório" type="date" className="input mt-1" value={to} onChange={event => setTo(event.target.value)} /></label>
      <button className="button button-small" onClick={() => void report.refetch()} disabled={report.isFetching}><RefreshCw size={14} /> Atualizar</button>
      <p className="m-0 text-xs muted sm:col-span-3">
        {data ? `${data.mode === 'simulation' ? 'SIMULAÇÃO PAPER' : 'SNIPER REAL · DADOS ON-CHAIN'} · ${date(data.period.from)} — ${date(data.period.to)} · atualizado às ${new Date(report.dataUpdatedAt).toLocaleTimeString('pt-BR')}` : 'Sem período carregado'}
      </p>
    </section>

    {report.isLoading && <div className="panel p-8 text-sm muted" aria-live="polite">Carregando relatório da API…</div>}
    {report.isError && <div className="panel flex items-center justify-between gap-4 p-5" role="alert"><div><div className="font-medium">Relatório indisponível</div><p className="m-0 mt-1 text-sm muted">A API não retornou os dados do período selecionado.</p></div><button className="button button-small" onClick={() => void report.refetch()}><RefreshCw size={14} /> Tentar novamente</button></div>}

    {currentDashboard.data && <section className="mb-4">
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
        <h2 className="m-0 text-sm font-semibold">Consolidado operacional em tempo real</h2>
        <span className="text-[10px] muted">{currentDashboard.data.mode === 'simulation' ? 'SIMULAÇÃO PAPER' : 'SNIPER REAL'} · atualizado às {new Date(currentDashboard.dataUpdatedAt).toLocaleTimeString('pt-BR')}</span>
      </div>
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
        <Metric label="Total estimado · realizado + aberto" value={numberSol(currentTotalPnl)} tone={currentTotalPnl == null ? undefined : currentTotalPnl >= 0 ? 'positive' : 'negative'} note={currentDashboard.data.totalEstimatedPnlComplete === false ? 'Parcial: há posição(ões) sem marca de preço.' : 'P&L realizado mais estimativa das posições abertas.'} />
        <Metric label={currentDashboard.data.mode === 'simulation' ? 'Realizado · paper' : 'Realizado · on-chain'} value={numberSol(currentDashboard.data.realizedPnlSol)} tone={currentDashboard.data.realizedPnlSol == null ? undefined : currentDashboard.data.realizedPnlSol >= 0 ? 'positive' : 'negative'} />
        <Metric label="Aberto estimado" value={numberSol(currentDashboard.data.estimatedOpenPnlSol)} note={currentDashboard.data.estimatedOpenPnlComplete === false ? `Cobertura parcial: ${number(currentDashboard.data.unmarkedOpenPositions, 0)} posição(ões) sem marca.` : 'Market cap; não é venda confirmada.'} />
      </div>
    </section>}

    {data && summary && <>
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3 rounded-md border border-[hsl(var(--border))] p-3">
        <div className="flex items-center gap-2 text-sm"><Activity size={16} className={data.mode === 'simulation' ? 'technical' : 'warning'} /><strong>{data.mode === 'simulation' ? 'Modo simulação' : 'Modo real'}</strong></div>
        <span className="text-xs muted">{data.mode === 'real' ? 'P&L fechado usa deltas confirmados da carteira; posições abertas são estimativas por market cap.' : 'P&L fechado e taxas são modelados em paper; posições abertas são estimativas por market cap.'}</span>
      </div>

      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Metric label="P&L líquido fechado" value={numberSol(summary.netPnlSol)} tone={summary.netPnlSol >= 0 ? 'positive' : 'negative'} />
        <Metric label="P&L bruto fechado" value={numberSol(summary.grossPnlSol)} />
        <Metric label="Taxas das operações fechadas" value={numberSol(closedFees)} tone="warning" />
        <Metric label="Taxas totais no período" value={numberSol(data.allFeesPaidSol)} tone="warning" />
        <Metric label="Operações encerradas" value={number(summary.totalTrades, 0)} suffix="trades" />
        <Metric label="Taxa de acerto" value={number(summary.winRate, 2)} suffix="%" tone={summary.winRate >= 45 ? 'positive' : 'warning'} />
        <Metric label="Expectativa por operação" value={numberSol(summary.evPerTradeSol)} tone={summary.evPerTradeSol > 0 ? 'positive' : 'negative'} />
        <Metric label="Drawdown máximo" value={numberSol(summary.maxDrawdownSol)} tone="warning" />
      </div>

      <div className="mb-4 grid gap-3 xl:grid-cols-[1.4fr_.8fr]">
        <section className="panel">
          <div className="flex items-center justify-between border-b border-[hsl(var(--border))] px-4 py-3.5"><h2 className="m-0 text-sm font-semibold">P&L realizado acumulado</h2><span className="eyebrow">SOL · OPERAÇÕES FECHADAS</span></div>
          {line.length > 1 ? <div className="p-4"><ResponsiveContainer width="100%" height={250}><AreaChart data={line}><CartesianGrid strokeDasharray="3 3" stroke="hsl(210 12% 24%)" /><XAxis dataKey="timestamp" tickFormatter={value => new Date(String(value)).toLocaleDateString('pt-BR')} tick={{ fill: '#8997a1', fontSize: 10 }} /><YAxis tick={{ fill: '#8997a1', fontSize: 10 }} /><Tooltip formatter={value => [`${numberSol(Number(value))} SOL`, 'P&L realizado']} labelFormatter={value => date(String(value))} /><Area type="monotone" dataKey="cumulativePnlSol" name="P&L realizado" stroke="#55d99a" fill="#55d99a" fillOpacity={0.14} dot={{ r: 2 }} /></AreaChart></ResponsiveContainer></div> : <div className="px-5 py-12 text-center text-sm muted">A curva aparece após pelo menos uma operação fechada.</div>}
        </section>
        <section className="panel">
          <div className="border-b border-[hsl(var(--border))] px-4 py-3.5"><h2 className="m-0 text-sm font-semibold">Métricas da estratégia</h2></div>
          <dl className="grid grid-cols-2 gap-x-4 gap-y-4 p-4 text-xs">
            <div><dt className="muted">Média dos ganhos</dt><dd className="mono mt-1 positive">{numberSol(summary.avgWinSol)} SOL</dd></div>
            <div><dt className="muted">Média das perdas</dt><dd className="mono mt-1 negative">{numberSol(summary.avgLossSol)} SOL</dd></div>
            <div><dt className="muted">Profit factor</dt><dd className="mono mt-1">{summary.profitFactor == null ? 'Sem perdas / indisponível' : number(summary.profitFactor, 3)}</dd></div>
            <div><dt className="muted">Sharpe não anualizado</dt><dd className="mono mt-1">{number(summary.sharpeRatio, 3)}</dd></div>
            <div><dt className="muted">Retenção média</dt><dd className="mono mt-1">{number(summary.avgHoldSeconds, 1)} s</dd></div>
            <div><dt className="muted">Sequência máx. · ganho/perda</dt><dd className="mono mt-1">{summary.longestWinStreak} / {summary.longestLossStreak}</dd></div>
            <div><dt className="muted">Saldo inicial</dt><dd className="mono mt-1">{numberSol(summary.balanceStartSol)} SOL</dd></div>
            <div><dt className="muted">Saldo atual / patrimônio</dt><dd className="mono mt-1">{numberSol(summary.balanceNowSol)} SOL</dd></div>
            <div className="col-span-2"><dt className="muted">Aberto · estimativa</dt><dd className="mono mt-1">{numberSol(data.openPnlSol)} SOL</dd><div className="mt-1 text-[10px] muted">{data.openPnlComplete === false ? `Cobertura parcial: ${number(data.unmarkedOpenPositions, 0)} posição(ões) sem marca de preço.` : data.mode === 'real' ? 'Estimativa por variação do market cap; não é valor confirmado de venda.' : 'Estimativa por variação do market cap paper; ainda não é resultado fechado.'}</div></div>
          </dl>
        </section>
      </div>

      <div className="mb-4 grid gap-3 xl:grid-cols-2">
        <section className="panel">
          <div className="border-b border-[hsl(var(--border))] px-4 py-3.5"><h2 className="m-0 text-sm font-semibold">Saídas por motivo</h2></div>
          {exitRows.length ? <div className="divide-y divide-[hsl(var(--border))] px-4">{exitRows.map(([key, count]) => <div className="flex justify-between py-3 text-xs" key={key}><span>{reasons[key] || key}</span><span className="mono">{number(count, 0)}</span></div>)}</div> : <p className="p-4 text-sm muted">Sem saídas encerradas no período.</p>}
        </section>
        <section className="panel">
          <div className="border-b border-[hsl(var(--border))] px-4 py-3.5"><h2 className="m-0 text-sm font-semibold">Resultado por hora · últimas 12</h2></div>
          {hours.length ? <div className="table-wrap"><table><thead><tr><th>Hora UTC</th><th>Saídas</th><th>P&L líquido</th></tr></thead><tbody>{hours.map(row => <tr key={row.hour}><td className="mono text-xs">{row.hour}</td><td className="mono text-xs">{row.trades}</td><td className={`mono text-xs ${row.netPnlSol >= 0 ? 'positive' : 'negative'}`}>{numberSol(row.netPnlSol)} SOL</td></tr>)}</tbody></table></div> : <p className="p-4 text-sm muted">Sem operações encerradas no período.</p>}
        </section>
      </div>

      <section className="panel mb-4">
        <div className="flex items-center justify-between border-b border-[hsl(var(--border))] px-4 py-3.5"><h2 className="m-0 text-sm font-semibold">Operações fechadas</h2><span className="eyebrow">{data.trades.length} REGISTOS</span></div>
        {data.trades.length ? <div className="table-wrap"><table><thead><tr><th>Confirmação</th><th>Mint</th><th>Entrada</th><th>Saída</th><th>Taxas</th><th>P&L líquido</th><th>Retenção</th><th>Motivo</th></tr></thead><tbody>{data.trades.map(trade => <tr key={trade.id}><td className="whitespace-nowrap text-xs muted">{date(trade.confirmedAt)}</td><td className="mono text-xs" title={trade.mint}>{trade.mint.slice(0, 6)}…{trade.mint.slice(-5)}</td><td className="mono text-xs">{numberSol(trade.entrySol)}</td><td className="mono text-xs">{numberSol(trade.exitSol)}</td><td className="mono text-xs warning">{numberSol(trade.feesSol)}</td><td className={`mono text-xs ${trade.netPnlSol >= 0 ? 'positive' : 'negative'}`}>{numberSol(trade.netPnlSol)}</td><td className="mono text-xs">{number(trade.holdSec, 1)} s</td><td className="text-xs">{trade.exitReason || 'outros'}</td></tr>)}</tbody></table></div> : <div className="px-5 py-12 text-center text-sm muted">Não existem operações encerradas no período.</div>}
      </section>

      <section className="panel">
        <div className="flex items-center justify-between border-b border-[hsl(var(--border))] px-4 py-3.5">
          <h2 className="m-0 text-sm font-semibold">GO / NO-GO · evidência paper</h2>
          {readiness?.available && (readiness.ready
            ? <span className="positive flex items-center gap-1 text-xs"><CheckCircle2 size={14} /> GO</span>
            : <span className="negative flex items-center gap-1 text-xs"><XCircle size={14} /> NO-GO</span>)}
        </div>
        {!readiness?.available ? <p className="p-4 text-sm muted">{readiness?.error || 'Evidência paper indisponível.'}</p> : <>
          <p className="px-4 pt-3 text-xs muted">Todos os critérios devem passar antes de liberar o modo real. A leitura não inicia nem altera operações.</p>
          <div className="divide-y divide-[hsl(var(--border))] px-4">{(readiness.checks ?? []).map(check => <div className="flex flex-wrap items-center justify-between gap-2 py-3 text-xs" key={check.key}><span className="flex items-center gap-2">{check.passed ? <CheckCircle2 size={14} className="positive" /> : <XCircle size={14} className="negative" />}{check.label}</span><span className="mono muted">{check.actual} · requisito {check.required}</span></div>)}</div>
        </>}
      </section>
      <p className="mb-8 mt-3 text-[10px] muted">Cálculo informado pela API. {data.feeModel}</p>
    </>}
  </div>;
}
