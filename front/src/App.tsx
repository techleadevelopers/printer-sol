// TODO(auth): Clerk routes below are intentionally unmounted because the Go API
// validates the master-password HMAC bearer token, not Clerk tokens. Revisit
// only as part of a coordinated API authentication migration.
import { createContext, useContext, useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from 'react';
import { ClerkProvider, Show, SignIn, SignUp, useAuth, useClerk } from '@clerk/react';
import { publishableKeyFromHost } from '@clerk/react/internal';
import { QueryClient, QueryClientProvider, useQueryClient } from '@tanstack/react-query';
import {
  Activity, ArrowRight, BarChart3, Bell, Check, CirclePause, CirclePlay, Copy, ExternalLink,
  FileText, Layers3, LayoutDashboard, ListFilter, LockKeyhole, LogOut, Menu,
  Radio, RefreshCw, Search, Server, Settings2, ShieldCheck, WalletCards, X,
} from 'lucide-react';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip as ChartTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  getGetDashboardQueryKey, getGetHealthQueryKey, getGetLogsQueryKey,
  getGetPositionsQueryKey, getGetRpcsQueryKey, getGetTokensQueryKey, getGetTradesQueryKey,
  getGetWalletQueryKey, useGetConfig, useGetDashboard, useGetHealth, useGetLogs,
  useGetPositions, useGetRpcs, useGetTokens, useGetTrades, useGetWallet,
  usePauseBot, useRequestPositionSell, useResumeBot, useSelectRpc,
  useStartBot, useStopBot, useTestRpc, setAuthTokenGetter,
  type GetLogsParams, type GetPositionsParams, type GetTokensParams, type GetTradesParams,
  type RpcEndpoint,
} from '@/api-client';
import { Link, Redirect, Route, Router as WouterRouter, Switch, useLocation } from 'wouter';
import { apiBaseUrl, apiConnectionLabel, hasConfiguredApiBase } from '@/services/api';
import { createEventSocket, type BotEvent, type EventState } from '@/services/websocket';
import { customFetch } from '@/api-client/custom-fetch';
import { formatSol } from '@/lib/sol-format';
import NotFound from '@/pages/not-found';
import PerformanceReportPage from '@/pages/performance-report';
import './index.css';

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: 1, staleTime: 5000, refetchOnWindowFocus: true } } });
const basePath = import.meta.env.BASE_URL.replace(/\/$/, '');
const clerkPubKey = publishableKeyFromHost(window.location.hostname, import.meta.env.VITE_CLERK_PUBLISHABLE_KEY);
const clerkProxyUrl = import.meta.env.VITE_CLERK_PROXY_URL;
const stripBase = (path: string) => basePath && path.startsWith(basePath) ? path.slice(basePath.length) || '/' : path;
const clerkAppearance = {};
type Notice = { text: string; kind?: 'success' | 'error' | 'info' };
let notifyGlobal: (notice: Notice) => void = () => undefined;
const notify = (notice: Notice) => notifyGlobal(notice);

type AuthState = { token: string | null; signOut: (_?: unknown) => void };
const AuthContext = createContext<AuthState>({ token: null, signOut: () => undefined });
const authStorageKey = 'solana-sniper-admin-session';
const apiBase = apiBaseUrl;
function useMasterAuth() { return useContext(AuthContext); }
const EventMonitorContext = createContext<{ state: EventState; latest: BotEvent | null }>({ state: 'connecting', latest: null });
const LIVE_DATA_REFRESH_MS = 5000;

const navItems = [
  { href: '/user-portal', label: 'Visão geral', icon: LayoutDashboard },
  { href: '/user-portal/reports', label: 'Relatório de desempenho', icon: BarChart3 },
  { href: '/user-portal/tokens', label: 'Tokens detetados', icon: Search },
  { href: '/user-portal/positions', label: 'Posições', icon: Layers3 },
  { href: '/user-portal/trades', label: 'Operações', icon: BarChart3 },
  { href: '/user-portal/rpcs', label: 'Infraestrutura RPC', icon: Server },
  { href: '/user-portal/strategy', label: 'Estratégia', icon: Settings2 },
  { href: '/user-portal/wallet', label: 'Carteira', icon: WalletCards },
  { href: '/user-portal/logs', label: 'Registos', icon: FileText },
];
const navShort = navItems.slice(0, 5);
const locale = 'pt-BR';
const fmt = (value: number | null | undefined, digits = 3) => value == null || !Number.isFinite(value) ? 'Indisponível' : value.toLocaleString(locale, { maximumFractionDigits: digits, minimumFractionDigits: 0 });
const fmtSol = formatSol;
const fmtDate = (date?: string | null) => date ? new Intl.DateTimeFormat(locale, { dateStyle: 'medium', timeStyle: 'short' }).format(new Date(date)) : 'Indisponível';
const shortKey = (value?: string | null) => value ? `${value.slice(0, 5)}…${value.slice(-5)}` : 'Indisponível';
const titleMap: Record<string, string> = {
  '/user-portal': 'Visão geral', '/user-portal/reports': 'Relatório de desempenho', '/user-portal/tokens': 'Tokens detetados', '/user-portal/positions': 'Posições',
  '/user-portal/trades': 'Histórico de operações', '/user-portal/rpcs': 'Infraestrutura RPC',
  '/user-portal/strategy': 'Configuração da estratégia', '/user-portal/wallet': 'Carteira', '/user-portal/logs': 'Registos do sistema',
};

function StatusTag({ value, positive = [], negative = [], warning = [] }: { value?: string | null; positive?: string[]; negative?: string[]; warning?: string[] }) {
  if (!value) return <span className="muted">Indisponível</span>;
  const normalized = value.toLowerCase();
  const klass = positive.includes(normalized) ? 'positive' : negative.includes(normalized) ? 'negative' : warning.includes(normalized) ? 'warning' : 'technical';
  return <span className={`inline-flex items-center gap-2 text-xs ${klass}`}><i className="status-dot" />{value.replaceAll('_', ' ')}</span>;
}

function SkeletonBlock({ count = 3 }: { count?: number }) {
  return <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">{Array.from({ length: count }, (_, i) => <div className="panel metric p-4" key={i}><div className="skeleton mb-4 h-2 w-24" /><div className="skeleton h-7 w-28" /></div>)}</div>;
}
function QueryState({ loading, error, onRetry }: { loading: boolean; error: boolean; onRetry: () => void }) {
  if (loading) return <div className="panel flex min-h-40 flex-col justify-center gap-3 p-6" aria-live="polite"><div className="skeleton h-3 w-32" /><div className="skeleton h-3 w-2/3" /><div className="skeleton h-3 w-1/2" /></div>;
  if (error) return <div className="panel flex min-h-36 flex-col items-start justify-center gap-2 p-6" role="alert"><div className="font-semibold">Aguardando conexão com o back-end</div><p className="m-0 text-sm muted">Os dados continuam indisponíveis até o serviço responder.</p><button className="button button-small mt-2" onClick={onRetry}><RefreshCw size={14} /> Tentar novamente</button></div>;
  return null;
}
function EmptyRows({ children = 'Ainda não existem registos disponíveis.' }: { children?: string }) {
  return <div className="px-5 py-12 text-center"><div className="mx-auto mb-3 grid h-10 w-10 place-items-center rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--muted))]"><Activity size={17} className="muted" /></div><p className="m-0 text-sm muted">{children}</p></div>;
}
function PanelTitle({ title, aside }: { title: string; aside?: ReactNode }) {
  return <div className="flex items-center justify-between gap-3 border-b border-[hsl(var(--border))] px-4 py-3.5"><h2 className="m-0 text-sm font-semibold">{title}</h2>{aside}</div>;
}
function Metric({ label, value, suffix, tone, note }: { label: string; value: string; suffix?: string; tone?: string; note?: string }) {
  return <section className="panel metric p-4"><div className="eyebrow">{label}</div><div className={`metric-value mt-3 ${tone || ''}`}>{value}<span className="ml-1 text-xs font-normal muted">{suffix}</span></div>{note && <div className="mt-2 text-xs muted">{note}</div>}</section>;
}
function DataPageFrame({ children, eyebrow = 'CENTRO DE OPERAÇÕES', description }: { children: ReactNode; eyebrow?: string; description?: string }) {
  const [path] = useLocation();
  return <div className="page-wrap enter"><div className="mb-6"><div className="eyebrow">{eyebrow}</div><h1 className="display mb-1 mt-2 text-2xl font-semibold">{titleMap[path] || 'Operações'}</h1>{description && <p className="m-0 text-sm muted">{description}</p>}</div>{children}</div>;
}
function useEventMonitor() {
  const [state, setState] = useState<EventState>('connecting');
  const [latest, setLatest] = useState<BotEvent | null>(null);
  const { token } = useMasterAuth();
  const qc = useQueryClient();
  useEffect(() => createEventSocket({
    getAccessToken: () => token,
    onState: next => {
      setState(next);
      if (next === 'connected') {
        // A reconnect can follow a server restart in the opposite operating
        // mode. Remove the previous ledger before refetching its replacement.
        qc.removeQueries();
        void qc.invalidateQueries();
      }
    },
    onEvent: (event) => {
      setLatest(event);
      const type = event.type.replaceAll('_', '.');
      const invalidate = (queryKey: readonly unknown[]) => {
        void qc.invalidateQueries({ queryKey });
      };
      if (type.startsWith('bot.') || type === 'health') {
        invalidate(getGetDashboardQueryKey());
        invalidate(getGetHealthQueryKey());
      }
      if (type.startsWith('token.')) {
        invalidate(getGetTokensQueryKey());
        invalidate(getGetDashboardQueryKey());
      }
      if (type.startsWith('position.')) {
        invalidate(getGetPositionsQueryKey());
        invalidate(getGetDashboardQueryKey());
        invalidate(['performance-report']);
      }
      if (['trade.', 'buy.', 'sell.', 'entry.', 'exit.'].some(prefix => type.startsWith(prefix))) {
        invalidate(getGetTradesQueryKey());
        invalidate(getGetPositionsQueryKey());
        invalidate(getGetDashboardQueryKey());
        invalidate(getGetWalletQueryKey());
        invalidate(['performance-report']);
        invalidate(getGetLogsQueryKey());
      }
      if (type.startsWith('rpc.')) {
        invalidate(getGetRpcsQueryKey());
        invalidate(getGetHealthQueryKey());
        invalidate(getGetDashboardQueryKey());
      }
      if (type.startsWith('risk.') || type.startsWith('operational.')) {
        invalidate(getGetLogsQueryKey());
        invalidate(getGetDashboardQueryKey());
      }
    },
  }), [token, qc]);
  return { state, latest };
}
function AppShell({ children }: { children: ReactNode }) {
  const [path] = useLocation();
  const { signOut } = useMasterAuth();
  const eventMonitor = useEventMonitor();
  const user: { primaryEmailAddress?: { emailAddress?: string } } | null = null;
  const health = useGetHealth({ query: { queryKey: getGetHealthQueryKey(), refetchInterval: 15000 } });
  const liveDashboard = useGetDashboard({ query: { queryKey: getGetDashboardQueryKey(), refetchInterval: LIVE_DATA_REFRESH_MS } });
  const stream = health.data?.eventMonitorStatus;
  const [mobileMenu, setMobileMenu] = useState(false);
  const current = titleMap[path] || 'Centro de operações';
  return <EventMonitorContext.Provider value={eventMonitor}><div className="shell flex">
    <aside className="sidebar hidden min-h-[100dvh] shrink-0 flex-col px-3 py-4 md:flex">
      <Link href="/user-portal" className="mb-8 flex items-center gap-3 px-2 no-underline">
        <img src={`${basePath}/logo.svg`} alt="Marca Solana Sniper" className="h-9 w-9 rounded-lg" />
        <span className="brand-label"><strong className="block text-sm tracking-tight">SOLANA SNIPER</strong><small className="eyebrow">CONSOLE · 01</small></span>
      </Link>
      <div className="eyebrow mb-2 px-3">WORKSPACE</div>
      <nav className="flex flex-1 flex-col gap-1" aria-label="Navegação principal">{navItems.map(({ href, label, icon: Icon }) => <Link href={href} key={href} className={`nav-link ${path === href ? 'active' : ''}`} aria-current={path === href ? 'page' : undefined} data-testid={`link-nav-${href.split('/').at(-1) || 'dashboard'}`}><Icon size={17} /><span className="nav-label">{label}</span></Link>)}</nav>
      <div className="side-foot mt-6 border-t border-[hsl(var(--border))] px-3 pt-4">
        <div className="flex items-center gap-2 text-xs"><span className={`status-dot ${stream === 'connected' ? 'positive' : stream === 'disconnected' ? 'warning' : 'technical'}`} /> Monitor de eventos <span className="ml-auto muted">{stream === 'connected' ? 'Ligado' : stream === 'disconnected' ? 'Desligado' : 'Indisponível'}</span></div>
        <div className="mt-2 text-[10px] muted">{apiConnectionLabel}</div>
      </div>
    </aside>
    <main className="main-area">
      <header className="topbar flex items-center justify-between px-4 sm:px-6">
         <div className="flex items-center gap-3"><button className="button button-small md:hidden" aria-label="Abrir navegação" onClick={() => setMobileMenu(!mobileMenu)}><Menu size={16} /></button><span className="text-sm font-medium">{current}</span><span className="flex items-center gap-2 rounded border border-[hsl(var(--border))] px-2 py-1 text-[10px] mono sm:inline-flex"><span className="eyebrow">MODO</span><span className={liveDashboard.data?.mode === 'simulation' ? 'positive' : liveDashboard.data?.mode === 'real' ? 'warning' : 'muted'}>{liveDashboard.data?.mode === 'simulation' ? 'SIMULAÇÃO PAPER' : liveDashboard.data?.mode === 'real' ? 'SNIPER REAL' : 'INDISPONÍVEL'}</span></span></div>
        <div className="flex items-center gap-3"><div className="hidden items-center gap-2 text-xs muted sm:flex"><span className={`status-dot ${stream === 'connected' ? 'positive' : stream === 'disconnected' ? 'warning' : 'technical'}`} />{stream === 'connected' ? 'Fluxo ligado' : stream === 'disconnected' ? 'Fluxo desligado' : 'Fluxo indisponível'}</div><button className="button button-small" title="Notificações indisponíveis" aria-label="Notificações indisponíveis" disabled><Bell size={15} /></button><div className="h-7 w-px bg-[hsl(var(--border))]" /><span className="hidden max-w-36 truncate text-xs muted sm:inline">{user?.primaryEmailAddress?.emailAddress || 'Sessão autenticada'}</span><button className="button button-small" onClick={() => signOut({ redirectUrl: basePath || '/' })} aria-label="Terminar sessão"><LogOut size={14} /><span className="hide-mobile">Sair</span></button></div>
      </header>
      {mobileMenu && <nav className="grid grid-cols-2 gap-1 border-b border-[hsl(var(--border))] bg-[hsl(var(--sidebar))] p-2 md:hidden">{navItems.map(({ href, label, icon: Icon }) => <Link href={href} key={href} className={`nav-link ${path === href ? 'active' : ''}`} onClick={() => setMobileMenu(false)}><Icon size={16} />{label}</Link>)}</nav>}
      {children}
      <nav className="mobile-nav md:hidden" aria-label="Navegação móvel">{navShort.map(({ href, label, icon: Icon }) => <Link href={href} key={href} className={path === href ? 'active' : ''}><Icon size={17} /><span>{label.split(' ')[0]}</span></Link>)}</nav>
    </main>
  </div></EventMonitorContext.Provider>;
}
function HomePage() {
  return <main className="min-h-[100dvh] overflow-hidden">
    <header className="flex h-16 items-center justify-between border-b border-[hsl(var(--border))] px-5 sm:px-10"><div className="flex items-center gap-3"><img src={`${basePath}/logo.svg`} className="h-9 w-9 rounded-lg" alt="Marca Solana Sniper" /><div><strong className="display text-sm">SOLANA SNIPER</strong><div className="eyebrow">OPERATIONS CONSOLE</div></div></div><div className="flex gap-2"><Link href="/sign-in" className="button">Iniciar sessão</Link><Link href="/sign-up" className="button button-primary">Criar conta <ArrowRight size={15} /></Link></div></header>
    <div className="page-wrap grid min-h-[calc(100dvh-64px)] items-center gap-12 py-16 lg:grid-cols-[1.1fr_.9fr]">
      <div className="max-w-2xl"><div className="eyebrow mb-6 flex items-center gap-2"><span className="status-dot positive" /> MONITORIZAÇÃO SOLANA · INTERFACE OPERACIONAL</div><h1 className="display m-0 text-5xl font-semibold leading-[1.03] sm:text-7xl">Sinal claro.<br /><span className="positive">Controlo total.</span></h1><p className="mt-6 max-w-xl text-base leading-7 muted">Uma consola segura para observar tokens, acompanhar posições e gerir a configuração do bot. As decisões e transações pertencem sempre ao serviço independente.</p><div className="mt-8 flex flex-wrap gap-3"><Link href="/sign-up" className="button button-primary px-5 py-3">Aceder ao console <ArrowRight size={16} /></Link><Link href="/sign-in" className="button px-5 py-3">Já tenho conta</Link></div><div className="mt-12 flex flex-wrap gap-x-8 gap-y-4 border-t border-[hsl(var(--border))] pt-5 text-xs muted"><span className="flex items-center gap-2"><LockKeyhole size={14} className="positive" /> Sem chaves privadas no browser</span><span className="flex items-center gap-2"><ShieldCheck size={14} className="technical" /> Execução delegada à API</span></div></div>
      <div className="relative mx-auto w-full max-w-xl"><div className="absolute -inset-16 rounded-full bg-[radial-gradient(circle,hsl(151_60%_30%_/_0.12),transparent_65%)]" /><div className="panel relative p-5 sm:p-7"><div className="mb-6 flex items-center justify-between border-b border-[hsl(var(--border))] pb-4"><div><div className="eyebrow">SISTEMA · INTEGRAÇÃO</div><div className="mt-2 flex items-center gap-2 text-sm font-semibold"><span className="status-dot technical" /> Aguardando conexão com o back-end</div></div><span className="rounded border border-[hsl(var(--border))] px-2 py-1 text-[10px] mono muted">CONTRATO API</span></div><div className="grid grid-cols-2 gap-3"><div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-4"><div className="eyebrow">MONITORIZAÇÃO</div><div className="mt-3 text-2xl display">Tokens</div><div className="mt-1 text-xs muted">Deteção e estado de análise</div></div><div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-4"><div className="eyebrow">INFRAESTRUTURA</div><div className="mt-3 text-2xl display">RPC</div><div className="mt-1 text-xs muted">Estado reportado pelo serviço</div></div><div className="col-span-2 rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-4"><div className="flex items-center justify-between"><div><div className="eyebrow">FRONTEIRA DE SEGURANÇA</div><div className="mt-2 text-sm font-medium">O browser não assina nem envia transações.</div></div><ShieldCheck className="positive" size={21} /></div></div></div></div></div>
    </div>
    <footer className="border-t border-[hsl(var(--border))] px-5 py-5 text-center text-xs muted">Consola de operações Solana Sniper · Dados e ações sujeitos à disponibilidade do back-end.</footer>
  </main>;
}

function DashboardPage() {
  const dash = useGetDashboard({ query: { queryKey: getGetDashboardQueryKey(), refetchInterval: LIVE_DATA_REFRESH_MS } });
  const health = useGetHealth({ query: { queryKey: getGetHealthQueryKey(), refetchInterval: 15000 } });
  const { state: eventState, latest } = useContext(EventMonitorContext);
  const start = useStartBot(), stop = useStopBot(), pause = usePauseBot(), resume = useResumeBot();
  const busy = start.isPending || stop.isPending || pause.isPending || resume.isPending;
  const backendAvailable = health.data?.status === 'ok';
  const qc = useQueryClient();
  const [resetOpen, setResetOpen] = useState(false);
  const [resetBalance, setResetBalance] = useState('');
  const [resetPending, setResetPending] = useState(false);
  const [resetError, setResetError] = useState('');
  const runAction = (fn: () => Promise<unknown>, label: string) => void fn().then((result: any) => {
    if (result?.accepted) { notify({ text: `${label}: pedido aceite pelo back-end.`, kind: 'success' }); void qc.invalidateQueries({ queryKey: getGetDashboardQueryKey() }); }
    else notify({ text: result?.message || 'O back-end não confirmou o pedido.', kind: 'error' });
  }).catch(() => notify({ text: 'Aguardando conexão com o back-end', kind: 'error' }));
  const d = dash.data;
  const defaultResetBalance = d?.simulationBalanceSol ?? 1;
  const resetTargetBalance = resetBalance.trim() ? Number(resetBalance) : defaultResetBalance;
  const resetPaperSimulation = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const hasCustomBalance = resetBalance.trim() !== '';
    const balanceSol = hasCustomBalance ? Number(resetBalance) : defaultResetBalance;
    if (!Number.isFinite(balanceSol) || balanceSol <= 0) {
      setResetError('Informe um saldo SOL finito e maior que zero.');
      return;
    }
    setResetPending(true);
    setResetError('');
    try {
      await customFetch('/api/simulation/reset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(hasCustomBalance ? { balance_sol: balanceSol } : {}),
      });
      setResetOpen(false);
      setResetBalance('');
      notify({ text: 'Simulação paper resetada.', kind: 'success' });
      void qc.invalidateQueries();
    } catch (cause) {
      setResetError(cause instanceof Error ? cause.message : 'Não foi possível resetar a simulação.');
    } finally {
      setResetPending(false);
    }
  };
  const totalEstimatedPnl = d?.totalEstimatedPnlSol ?? d?.netPnlSol;
  const balancePoints = (d?.balanceHistory || []).filter(p => p.value != null);
  const resultChartData = (d?.cumulativeResult || []).filter(p => p.value != null).map(p => ({ timestamp: p.timestamp, value: p.value as number }));
  const tradesByPeriod = d?.tradesByPeriod ?? [];
  const recentTrades = d?.recentTrades ?? [];
  const tradeChartData = tradesByPeriod.filter(p => p.total != null || p.positive != null || p.negative != null).map(p => ({ period: p.period, total: p.total, positive: p.positive, negative: p.negative }));
  const outcomeData = tradesByPeriod.length > 0 && tradesByPeriod.every(p => p.positive != null && p.negative != null)
    ? [{ name: 'Positivas', value: tradesByPeriod.reduce((sum, p) => sum + (p.positive ?? 0), 0) }, { name: 'Negativas', value: tradesByPeriod.reduce((sum, p) => sum + (p.negative ?? 0), 0) }]
    : [];
  const line = balancePoints.length > 1 ? balancePoints.map((p, i) => `${i ? 'L' : 'M'}${i * 100 / (balancePoints.length - 1)},${100 - (p.value! - Math.min(...balancePoints.map(x => x.value!))) / Math.max(0.0001, Math.max(...balancePoints.map(x => x.value!)) - Math.min(...balancePoints.map(x => x.value!))) * 85}`).join(' ') : '';
  return <DataPageFrame description="Estado consolidado reportado pelo serviço de operações.">
    <QueryState loading={dash.isLoading} error={dash.isError} onRetry={() => void dash.refetch()} />
    {!dash.isLoading && !dash.isError && d && <>
      {!backendAvailable && <div className="mb-4 rounded-md border border-amber-400/30 bg-amber-400/5 p-3 text-sm warning" role="alert">
        {health.isLoading ? 'Verificando conexão com o back-end. Ações operacionais bloqueadas.' : 'Conexão com o back-end indisponível ou degradada. Ações operacionais bloqueadas até confirmação.'}
      </div>}
      {d.walletBalanceLow === true && <div className="mb-4 rounded-md border border-amber-400/30 bg-amber-400/5 p-3 text-sm warning" role="alert">Saldo da carteira baixo, conforme sinalizado pelo back-end.</div>}
      {d.mode === 'simulation' && d.staleMarks && d.staleMarks.pctFlat > 80 && <div className="mb-4 rounded-md border border-amber-400/30 bg-amber-400/5 p-3 text-sm warning" role="alert">Feed de preço suspeito: {fmt(d.staleMarks.pctFlat, 1)}% das marcas paper continuam no valor de entrada ({fmt(d.staleMarks.flat, 0)} de {fmt(d.staleMarks.total, 0)}).</div>}
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-3"><span className="rounded border border-[hsl(var(--border))] px-3 py-2"><span className="eyebrow mr-2">BOT</span><StatusTag value={d.botStatus} positive={['running']} warning={['paused', 'starting', 'stopping']} negative={['error']} /></span><span className="rounded border border-[hsl(var(--border))] px-3 py-2"><span className="eyebrow mr-2">MODO</span><StatusTag value={d.mode} warning={['real']} positive={['simulation']} /></span></div>
        <div className="flex flex-wrap gap-2">
          {(d.botStatus === 'stopped' || d.botStatus === 'error') && <button className="button button-primary button-small" disabled={busy || !backendAvailable} onClick={() => runAction(() => start.mutateAsync(), 'Iniciar')}><CirclePlay size={14} /> Iniciar</button>}
          {d.botStatus === 'running' && <><button className="button button-small" disabled={busy || !backendAvailable} onClick={() => runAction(() => pause.mutateAsync(), 'Pausar')}><CirclePause size={14} /> Pausar novas compras</button><button className="button button-danger button-small" disabled={busy || !backendAvailable} onClick={() => runAction(() => stop.mutateAsync(), 'Parar')}><X size={14} /> Parar novas operações</button></>}
          {d.botStatus === 'paused' && <><button className="button button-primary button-small" disabled={busy || !backendAvailable} onClick={() => runAction(() => resume.mutateAsync(), 'Retomar')}><CirclePlay size={14} /> Retomar estratégia</button><button className="button button-danger button-small" disabled={busy || !backendAvailable} onClick={() => runAction(() => stop.mutateAsync(), 'Parar')}><X size={14} /> Parar novas operações</button></>}
          {d.mode === 'simulation' && <button className="button button-danger button-small" disabled={!backendAvailable || resetPending} onClick={() => { setResetError(''); setResetOpen(true); }}>Limpar operações paper</button>}
        </div>
      </div>
      <p className="mb-3 mt-0 text-right text-[10px] muted" role="status">
        Atualizado às {new Date(dash.dataUpdatedAt).toLocaleTimeString('pt-BR')} · {eventState === 'connected' ? 'eventos ao vivo + consulta de segurança a cada 5 s' : 'consulta de segurança a cada 5 s; fluxo de eventos reconectando'}
      </p>
      {d.botStatus === 'running' && <p className="mb-4 mt-0 text-xs warning">Parar interrompe novas operações; não fecha posições existentes.</p>}
      <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Metric label="Tokens detetados" value={fmt(d.detectedTokens, 0)} />
        <Metric label="Tokens analisados" value={fmt(d.analyzedTokens, 0)} />
        <Metric label="Operações encerradas" value={fmt(d.closedTrades ?? d.tradesCount, 0)} note="Ciclos de compra e venda concluídos" />
        <Metric label="Posições abertas" value={fmt(d.openPositions, 0)} />
        <Metric label={d.mode === 'simulation' ? 'Saldo disponível paper' : 'Saldo da carteira'} value={fmtSol(d.walletBalanceSol)} suffix="SOL" note={d.walletBalanceAvailable === false ? 'Leitura indisponível; zero não será presumido.' : undefined} />
        {d.mode === 'simulation' && <Metric label="Patrimônio virtual paper" value={fmtSol(d.virtualEquitySol)} suffix="SOL" note={d.estimatedOpenPnlComplete === false ? 'Estimativa parcial: falta marca em uma ou mais posições.' : 'Inclui posições abertas pela última marca disponível.'} />}
        <Metric label="Reserva de taxas" value={fmtSol(d.feesReserveSol)} suffix="SOL" />
        <Metric label={d.mode === 'simulation' ? 'P&L realizado · paper' : 'P&L realizado · on-chain'} value={fmtSol(d.realizedPnlSol)} suffix="SOL" tone={d.realizedPnlSol == null ? undefined : d.realizedPnlSol >= 0 ? 'positive' : 'negative'} />
        <Metric label="P&L aberto estimado" value={fmtSol(d.estimatedOpenPnlSol)} suffix="SOL" tone={d.estimatedOpenPnlSol == null ? undefined : d.estimatedOpenPnlSol >= 0 ? 'technical' : 'warning'} note={d.estimatedOpenPnlComplete === false ? `${fmt(d.unmarkedOpenPositions, 0)} posição(ões) sem marca; soma parcial.` : d.mode === 'simulation' ? 'Estimativa paper por market cap; ainda não é resultado fechado.' : 'Estimativa por market cap; apenas vendas confirmadas são realizadas.'} />
      </div>
      <div className="mb-4 grid gap-3 lg:grid-cols-[1.4fr_.8fr]">
        <section className="panel"><PanelTitle title={d.mode === 'simulation' ? 'Evolução do patrimônio paper' : 'Histórico do saldo SOL'} aside={<span className="eyebrow">SOL · API</span>} />
          {line ? <div className="px-4 py-5"><svg viewBox="0 0 100 100" preserveAspectRatio="none" className="h-44 w-full overflow-visible" role="img" aria-label={d.mode === 'simulation' ? 'Evolução do património paper' : 'Histórico do saldo SOL'}><defs><linearGradient id="chart-fill" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stopColor="#55d99a" stopOpacity=".22" /><stop offset="1" stopColor="#55d99a" stopOpacity="0" /></linearGradient></defs><path d={`${line} L100 100 L0 100 Z`} className="chart-area" /><path d={line} className="chart-line" /></svg><div className="mt-3 flex justify-between text-[10px] mono muted"><span>{fmtDate(balancePoints[0]?.timestamp)}</span><span>{fmtDate(balancePoints.at(-1)?.timestamp)}</span></div></div> : <EmptyRows>{d.mode === 'real' ? 'O saldo SOL atual é exibido acima; histórico de saldo live não é persistido. Consulte abaixo a curva de P&L confirmado.' : 'Aguardando pontos de patrimônio paper.'}</EmptyRows>}
        </section>
        <section className="panel"><PanelTitle title="Resultado total estimado" /><div className="grid grid-cols-2 gap-3 p-4"><Metric label="Realizado + aberto estimado" value={fmtSol(totalEstimatedPnl)} suffix="SOL" tone={totalEstimatedPnl == null ? undefined : totalEstimatedPnl >= 0 ? 'positive' : 'negative'} note={d.totalEstimatedPnlComplete === false ? 'Parcial: há posições sem marca de preço.' : 'A parcela aberta é estimada, não uma venda confirmada.'} /><Metric label="Custos" value={fmtSol(d.costsSol)} suffix="SOL" tone={d.costsSol == null ? undefined : 'warning'} /><div className="col-span-2 rounded-md border border-[hsl(var(--border))] p-3"><div className="eyebrow mb-3">OPERAÇÕES POR PERÍODO</div>{tradesByPeriod.length ? tradesByPeriod.map((p) => <div key={p.period} className="flex justify-between border-t border-[hsl(var(--border))] py-2 text-xs"><span>{p.period}</span><span><b className={p.positive == null ? 'muted' : 'positive'}>{fmt(p.positive, 0)}</b><span className="mx-2 muted">/</span><b className={p.negative == null ? 'muted' : 'negative'}>{fmt(p.negative, 0)}</b><span className="ml-2 muted">de {fmt(p.total, 0)}</span></span></div>) : <div className="text-xs muted">Indisponível</div>}</div></div></section>
      </div>
      <div className="mb-4 grid gap-3 xl:grid-cols-2">
        <section className="panel"><PanelTitle title="P&L realizado acumulado" aside={<span className="eyebrow">SOL · OPERAÇÕES FECHADAS</span>} />{resultChartData.length ? <div className="p-4"><ResponsiveContainer width="100%" height={210}><AreaChart data={resultChartData}><CartesianGrid strokeDasharray="3 3" stroke="hsl(210 12% 24%)" /><XAxis dataKey="timestamp" tickFormatter={value => new Date(String(value)).toLocaleDateString('pt-BR')} tick={{ fill: '#8997a1', fontSize: 10 }} /><YAxis tick={{ fill: '#8997a1', fontSize: 10 }} /><ChartTooltip formatter={value => [fmtSol(Number(value)), 'SOL']} labelFormatter={value => fmtDate(String(value))} /><Area type="monotone" dataKey="value" name="P&L realizado" stroke="#55d99a" fill="#55d99a" fillOpacity={0.14} connectNulls={false} dot={{ r: 3, fill: '#55d99a' }} /></AreaChart></ResponsiveContainer></div> : <EmptyRows>Série de resultado realizado indisponível.</EmptyRows>}</section>
        <section className="panel"><PanelTitle title="Operações por período" aside={<span className="eyebrow">CONTAGEM · API</span>} />{tradeChartData.length ? <div className="p-4"><ResponsiveContainer width="100%" height={210}><BarChart data={tradeChartData}><CartesianGrid strokeDasharray="3 3" stroke="hsl(210 12% 24%)" /><XAxis dataKey="period" tick={{ fill: '#8997a1', fontSize: 10 }} /><YAxis allowDecimals={false} tick={{ fill: '#8997a1', fontSize: 10 }} /><ChartTooltip /><Bar dataKey="total" name="Total" fill="#66b2ff" radius={[3, 3, 0, 0]} /><Bar dataKey="positive" name="Positivas" fill="#55d99a" radius={[3, 3, 0, 0]} /><Bar dataKey="negative" name="Negativas" fill="#ef7772" radius={[3, 3, 0, 0]} /></BarChart></ResponsiveContainer></div> : <EmptyRows>Contagem por período indisponível.</EmptyRows>}</section>
        <section className="panel"><PanelTitle title="Distribuição dos resultados" aside={<span className="eyebrow">OPERAÇÕES · API</span>} />{outcomeData.length && outcomeData.some(entry => entry.value > 0) ? <div className="flex items-center justify-center gap-5 p-4"><ResponsiveContainer width="65%" height={190}><PieChart><Pie data={outcomeData} dataKey="value" nameKey="name" innerRadius={48} outerRadius={72} paddingAngle={3}>{outcomeData.map((entry, index) => <Cell key={entry.name} fill={index === 0 ? '#55d99a' : '#ef7772'} />)}</Pie><ChartTooltip /></PieChart></ResponsiveContainer><div className="space-y-3 text-xs"><div><span className="positive">● Positivas</span><div className="mono mt-1">{fmt(outcomeData[0]?.value, 0)}</div></div><div><span className="negative">● Negativas</span><div className="mono mt-1">{fmt(outcomeData[1]?.value, 0)}</div></div></div></div> : <EmptyRows>Distribuição indisponível ou sem operações classificadas.</EmptyRows>}</section>
        <section className="panel"><PanelTitle title="Histórico do saldo" aside={<span className="eyebrow">SOL · API</span>} />{balancePoints.length ? <div className="p-4"><ResponsiveContainer width="100%" height={210}><AreaChart data={balancePoints}><CartesianGrid strokeDasharray="3 3" stroke="hsl(210 12% 24%)" /><XAxis dataKey="timestamp" tickFormatter={value => new Date(String(value)).toLocaleDateString('pt-BR')} tick={{ fill: '#8997a1', fontSize: 10 }} /><YAxis tick={{ fill: '#8997a1', fontSize: 10 }} /><ChartTooltip formatter={value => [fmtSol(Number(value)), 'SOL']} labelFormatter={value => fmtDate(String(value))} /><Area type="monotone" dataKey="value" name="Saldo" stroke="#66b2ff" fill="#66b2ff" fillOpacity={0.14} dot={{ r: 3, fill: '#66b2ff' }} /></AreaChart></ResponsiveContainer></div> : <EmptyRows>Histórico de saldo indisponível ou sem pontos válidos.</EmptyRows>}</section>
      </div>
      <div className="grid gap-3 lg:grid-cols-2">
        <section className="panel"><PanelTitle title="Saúde dos serviços" /><div className="space-y-3 p-4 text-sm"><div className="flex justify-between"><span className="muted">API</span><StatusTag value={health.data?.status || (health.isError ? 'unavailable' : undefined)} positive={['ok']} warning={['degraded']} negative={['unavailable']} /></div><div className="flex justify-between"><span className="muted">Monitor de eventos</span><StatusTag value={health.data?.eventMonitorStatus || eventState} positive={['connected']} warning={['unknown', 'connecting']} negative={['disconnected', 'unavailable']} /></div><div className="flex justify-between text-xs"><span className="muted">Verificado</span><span className="mono">{fmtDate(health.data?.checkedAt)}</span></div></div></section>
        <section className="panel"><PanelTitle title="Monitor de eventos" aside={<span className="text-xs muted">{eventState === 'connected' ? 'Ligado' : eventState === 'connecting' ? 'A ligar' : 'Desligado'}</span>} /><div className="p-4">{latest ? <div className="flex items-start gap-3"><Radio className="technical mt-0.5" size={16} /><div><div className="mono text-xs">{latest.type}</div><div className="mt-1 text-xs muted">{fmtDate(latest.at ?? latest.timestamp)}</div></div></div> : <p className="m-0 text-xs muted">Aguardando conexão com o back-end</p>}</div></section>
      </div>
      <section className="panel mt-4"><PanelTitle title="Operações recentes" aside={<Link href="/user-portal/trades" className="text-xs technical no-underline">Ver histórico <ArrowRight size={13} className="inline" /></Link>} />{recentTrades.length ? <TradesTable items={recentTrades} /> : <EmptyRows />}</section>
      {resetOpen && d.mode === 'simulation' && <div className="modal-backdrop" role="presentation"><form className="modal" role="dialog" aria-modal="true" aria-labelledby="reset-paper-title" onSubmit={resetPaperSimulation}>
        <div className="eyebrow">AÇÃO DESTRUTIVA · SOMENTE PAPER</div>
        <h2 id="reset-paper-title" className="display mt-2 text-xl">Limpar operações e resetar</h2>
        <p className="mt-3 text-sm muted">Isso apaga as operações paper, posições, marcas, eventos de tokens e avaliações de risco. A ação não pode ser desfeita. Se houver operação aguardando reconciliação, o servidor recusará o reset.</p>
        <label className="mt-4 block text-xs muted" htmlFor="reset-paper-balance">Saldo inicial novo em SOL (opcional)</label>
        <input id="reset-paper-balance" className="input mt-1 w-full" type="number" min="0.000000001" step="0.000000001" value={resetBalance} onChange={event => setResetBalance(event.target.value)} placeholder={fmt(defaultResetBalance, 9)} />
        <p className="mt-2 text-xs muted">Saldo do novo ciclo: {Number.isFinite(resetTargetBalance) && resetTargetBalance > 0 ? fmt(resetTargetBalance, 9) : 'inválido'} SOL. O estado atual do bot será preservado durante o reset.</p>
        {resetError && <p className="mt-3 text-sm negative" role="alert">{resetError}</p>}
        <div className="mt-5 flex justify-end gap-2"><button type="button" className="button" disabled={resetPending} onClick={() => setResetOpen(false)}>Cancelar</button><button type="submit" className="button button-danger" disabled={resetPending}>{resetPending ? 'A resetar…' : 'Confirmar reset'}</button></div>
      </form></div>}
    </>}
  </DataPageFrame>;
}

function TradesTable({ items }: { items: any[] }) {
  return <div className="table-wrap"><table><thead><tr><th>Data</th><th>Token / Mint</th><th>Lado</th><th>Quantidade</th><th>Valor SOL</th><th>Taxas</th><th>Slippage</th><th>P&L bruto do ciclo</th><th>P&L líquido do ciclo</th><th>Motivo</th><th>Estado</th><th>Explorer / ID</th></tr></thead><tbody>{items.map((t) => {
    const transactionId = t.transactionId || t.signature;
    const pnlDisplay = (value: number | null | undefined) => value == null ? (t.side === 'buy' ? '—' : 'Indisponível') : fmtSol(value);
    return <tr key={t.id}>
      <td className="whitespace-nowrap text-xs muted">{fmtDate(t.timestamp)}</td>
      <td><div className="text-xs font-medium">{t.symbol || t.tokenName || 'Token sem nome'}</div><div className="mono mt-1 text-[10px] muted">{shortKey(t.mint)}</div></td>
      <td><span className={t.side === 'buy' ? 'technical' : 'warning'}>{t.side === 'buy' ? 'Compra' : 'Venda'}</span></td>
      <td className="mono text-xs">{fmt(t.quantity)}</td>
      <td className="mono text-xs">{fmtSol(t.side === 'buy' ? t.buyValueSol : t.sellValueSol)}</td>
      <td className="mono text-xs">{fmtSol(t.feesSol)}</td>
      <td className="mono text-xs">{t.slippage == null ? '—' : `${fmt(t.slippage, 2)}%`}</td>
      <td className={`mono text-xs ${t.grossPnlSol == null ? 'muted' : t.grossPnlSol >= 0 ? 'positive' : 'negative'}`}>{pnlDisplay(t.grossPnlSol)}</td>
      <td className={`mono text-xs ${t.netPnlSol == null ? 'muted' : t.netPnlSol >= 0 ? 'positive' : 'negative'}`}>{pnlDisplay(t.netPnlSol)}</td>
      <td className="text-xs">{t.side === 'sell' ? t.exitReason || 'outros' : '—'}</td>
      <td><StatusTag value={t.status} positive={['confirmed']} negative={['failed']} warning={['pending', 'submitted']} /></td>
      <td><div className="flex items-center gap-2">{isSignature(t.signature) && <a className="technical text-xs" href={`https://solscan.io/tx/${encodeURIComponent(t.signature)}`} target="_blank" rel="noreferrer" aria-label="Ver assinatura no Solscan">Abrir <ExternalLink className="inline" size={12} /></a>}{transactionId ? <><span className="mono text-xs muted">{shortKey(transactionId)}</span><button type="button" className="button button-small" aria-label="Copiar identificador da transação" onClick={() => void copyValue(transactionId, 'Identificador da transação')}><Copy size={12} /></button></> : <span className="muted">—</span>}</div></td>
    </tr>;
  })}</tbody></table></div>;
}
function isSignature(value: unknown): value is string { return typeof value === 'string' && /^[1-9A-HJ-NP-Za-km-z]{64,90}$/.test(value); }
async function copyValue(value: string, label: string) {
  try {
    await navigator.clipboard.writeText(value);
    notify({ text: `${label} copiado.`, kind: 'success' });
  } catch {
    notify({ text: `Não foi possível copiar ${label.toLowerCase()}.`, kind: 'error' });
  }
}
function elapsedSince(value: string | null | undefined) {
  if (!value) return 'Indisponível';
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) return 'Indisponível';
  const minutes = Math.floor(Math.max(0, Date.now() - timestamp) / 60000);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} h`;
  return `${Math.floor(hours / 24)} d ${hours % 24} h`;
}

function TokensPage() {
  const [filter, setFilter] = useState('');
  const [mint, setMint] = useState('');
  const [detectedFrom, setDetectedFrom] = useState('');
  const [detectedTo, setDetectedTo] = useState('');
  const [cursor, setCursor] = useState<string | undefined>();
  const params = useMemo<GetTokensParams>(() => ({ limit: 30, ...(filter ? { status: filter as GetTokensParams['status'] } : {}), ...(mint ? { mint } : {}), ...(detectedFrom ? { detectedFrom: new Date(detectedFrom).toISOString() } : {}), ...(detectedTo ? { detectedTo: new Date(detectedTo).toISOString() } : {}), ...(cursor ? { cursor } : {}) }), [filter, mint, detectedFrom, detectedTo, cursor]);
  const page = useGetTokens(params, { query: { refetchInterval: LIVE_DATA_REFRESH_MS } });
  return <DataPageFrame description="Fluxo de descoberta e triagem reportado pela API.">
    <section className="panel mb-4 p-4"><div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4"><label className="text-xs muted">Estado<select className="select mt-1" value={filter} onChange={e => { setFilter(e.target.value); setCursor(undefined); }} aria-label="Filtrar por estado de análise" data-testid="select-token-status"><option value="">Todos os estados</option>{['detected','analyzing','approved','rejected','buy_submitted','buy_confirmed','buy_failed','position_open','sell_submitted','sell_confirmed'].map(x => <option value={x} key={x}>{x.replaceAll('_',' ')}</option>)}</select></label><label className="text-xs muted">Endereço mint<input className="input mt-1 mono" placeholder="Filtrar pelo mint" value={mint} onChange={e => { setMint(e.target.value); setCursor(undefined); }} /></label><label className="text-xs muted">Detetado a partir de<input type="datetime-local" className="input mt-1" value={detectedFrom} onChange={e => { setDetectedFrom(e.target.value); setCursor(undefined); }} /></label><label className="text-xs muted">Detetado até<input type="datetime-local" className="input mt-1" value={detectedTo} onChange={e => { setDetectedTo(e.target.value); setCursor(undefined); }} /></label></div></section>
    <QueryState loading={page.isLoading} error={page.isError} onRetry={() => void page.refetch()} />
     {!page.isLoading && !page.isError && <section className="panel"><PanelTitle title="Deteções" aside={<span className="eyebrow">{page.data?.items.length ?? 0} REGISTOS</span>} />{page.data?.items.length ? <div className="table-wrap"><table><thead><tr><th>Detetado</th><th>Token</th><th>Mint / Criador</th><th>Análise</th><th>Liquidez</th><th>Trades</th><th>Risco / motivo</th><th>Operação</th></tr></thead><tbody>{page.data.items.map(t => <tr key={t.id}><td className="whitespace-nowrap text-xs muted">{fmtDate(t.detectedAt)}</td><td><div className="text-xs font-medium">{t.name || t.symbol || 'Token sem nome'}</div><div className="mono mt-1 text-[10px] muted">{t.symbol || 'Símbolo indisponível'}</div></td><td><div className="mono text-xs">{shortKey(t.mint)}</div><div className="mt-1 flex items-center gap-2"><span className="text-[10px] muted">Criador: {shortKey(t.creatorWallet)}</span><button type="button" className="button button-small" aria-label="Copiar endereço mint" onClick={() => void copyValue(t.mint, 'Endereço mint')}><Copy size={12} /></button></div></td><td><StatusTag value={t.analysisStatus} positive={['approved']} negative={['rejected', 'buy_failed']} warning={['detected', 'analyzing']} /></td><td className="mono text-xs">{fmtSol(t.liquiditySol)}</td><td className="mono text-xs">{fmt(t.tradeCount, 0)}</td><td><div className="text-xs">{t.riskResult || 'Indisponível'}</div><div className="mt-1 max-w-52 text-xs muted">{t.decisionReason || 'Indisponível'}</div></td><td><StatusTag value={t.operationStatus} positive={['buy_confirmed', 'position_open', 'sell_confirmed']} negative={['buy_failed']} warning={['buy_submitted', 'sell_submitted']} /></td></tr>)}</tbody></table></div> : <EmptyRows>Não existem tokens para os filtros selecionados.</EmptyRows>}<Pagination nextCursor={page.data?.nextCursor} setCursor={setCursor} cursor={cursor} /></section>}
  </DataPageFrame>;
}
function Pagination({ nextCursor, setCursor, cursor }: { nextCursor?: string | null; setCursor: (v?: string) => void; cursor?: string }) {
  return <div className="flex justify-between border-t border-[hsl(var(--border))] px-4 py-3"><button className="button button-small" onClick={() => setCursor(undefined)} disabled={!cursor}>Primeira página</button><button className="button button-small" onClick={() => setCursor(nextCursor || undefined)} disabled={!nextCursor}>Seguinte <ArrowRight size={13} /></button></div>;
}

function PositionsPage() {
  const [status, setStatus] = useState('');
  const [cursor, setCursor] = useState<string | undefined>();
  const params = useMemo<GetPositionsParams>(() => ({ limit: 30, ...(status ? { status: status as GetPositionsParams['status'] } : {}), ...(cursor ? { cursor } : {}) }), [status, cursor]);
  const page = useGetPositions(params, { query: { refetchInterval: LIVE_DATA_REFRESH_MS } });
  const sell = useRequestPositionSell();
  const qc = useQueryClient();
  const [sellId, setSellId] = useState<string | null>(null);
  const [reason, setReason] = useState('');
  const confirmSell = (e: FormEvent) => { e.preventDefault(); if (!sellId || sell.isPending) return; sell.mutate({ positionId: sellId, data: { reason: reason || null } }, { onSuccess: r => { if (r.accepted) { notify({ text: 'Pedido de venda aceite pelo back-end.', kind: 'success' }); void qc.invalidateQueries({ queryKey: getGetPositionsQueryKey() }); void qc.invalidateQueries({ queryKey: getGetDashboardQueryKey() }); setSellId(null); setReason(''); } else notify({ text: r.message || 'O back-end não confirmou o pedido.', kind: 'error' }); }, onError: () => notify({ text: 'Aguardando conexão com o back-end', kind: 'error' }) }); };
  return <DataPageFrame description="Exposição e estados de venda enviados pelo back-end.">
    <div className="mb-4 flex items-center gap-2"><ListFilter size={15} className="muted" /><select className="select w-auto min-w-40" value={status} onChange={e => { setStatus(e.target.value); setCursor(undefined); }} aria-label="Filtrar posições"><option value="">Todos os estados</option>{['open','closing','closed','failed'].map(x => <option key={x} value={x}>{x}</option>)}</select></div>
    <QueryState loading={page.isLoading} error={page.isError} onRetry={() => void page.refetch()} />
     {!page.isLoading && !page.isError && <section className="panel">
       <PanelTitle title="Posições" aside={<span className="eyebrow">{page.data?.items.length ?? 0} REGISTOS</span>} />
       {page.data?.items.length ? <div className="table-wrap"><table>
         <thead><tr><th>Token / Mint</th><th>Capital investido</th><th>Taxas pagas</th><th>Valor estimado</th><th>P&L aberto estimado</th><th>P&L realizado líquido</th><th>Última marca</th><th>Aberta</th><th>Retenção</th><th>Estado / venda</th><th>Ação</th></tr></thead>
         <tbody>{page.data.items.map(p => {
           const markAge = p.markAgeSeconds;
           const markIsStale = markAge != null && markAge > 30;
           return <tr key={p.id}>
             <td><div className="text-xs font-medium">{p.symbol || p.tokenName || 'Token sem nome'}</div><div className="mono mt-1 text-[10px] muted">{shortKey(p.mint)}</div></td>
             <td className="mono text-xs">{fmtSol(p.buyValueSol)} SOL</td>
             <td className="mono text-xs warning">{fmtSol(p.costsSol)} SOL</td>
             <td className="mono text-xs">{fmtSol(p.estimatedValueSol)} SOL</td>
             <td className={`mono text-xs ${p.estimatedPnlSol == null ? 'muted' : p.estimatedPnlSol >= 0 ? 'positive' : 'negative'}`} title="Estimativa bruta pela variação do market cap; não inclui uma venda futura.">{fmtSol(p.estimatedPnlSol)} SOL</td>
             <td className={`mono text-xs ${p.realizedPnlSol == null ? 'muted' : p.realizedPnlSol >= 0 ? 'positive' : 'negative'}`}>{fmtSol(p.realizedPnlSol)} SOL</td>
             <td className="whitespace-nowrap text-xs">
               {p.markAvailable ? <><div className="mono">MC {fmtSol(p.currentMarketCapSol)}</div><div className={`mt-1 ${markIsStale ? 'warning' : 'muted'}`}>{markAge == null ? 'Idade indisponível' : `${markIsStale ? 'Marca antiga' : 'Atualizada'} · ${fmt(markAge, 0)} s`}</div></> : <span className="muted">Cotação indisponível</span>}
             </td>
             <td className="whitespace-nowrap text-xs muted">{fmtDate(p.openedAt)}</td>
             <td className="whitespace-nowrap text-xs muted">{elapsedSince(p.openedAt)}</td>
             <td><StatusTag value={p.status} positive={['open', 'closed']} warning={['closing']} negative={['failed']} /><div className="mt-1"><StatusTag value={p.sellStatus} positive={['confirmed']} warning={['submitted']} negative={['failed']} /></div></td>
             <td><button className="button button-small button-danger" disabled={p.status !== 'open' || sell.isPending} onClick={() => setSellId(p.id)}>Solicitar venda</button></td>
           </tr>;
         })}</tbody>
       </table></div> : <EmptyRows />}<Pagination nextCursor={page.data?.nextCursor} setCursor={setCursor} cursor={cursor} />
     </section>}
    {sellId && <div className="modal-backdrop" role="presentation"><form className="modal" onSubmit={confirmSell} aria-labelledby="sell-title"><div className="flex items-start justify-between"><div><div className="eyebrow">PEDIDO AO BACK-END</div><h2 id="sell-title" className="display mt-2 text-xl">Solicitar venda manual</h2></div><button className="button button-small" type="button" aria-label="Fechar" onClick={() => setSellId(null)}><X size={15} /></button></div><p className="text-sm muted">A venda só será confirmada quando a API aceitar. Esta ação não assina transações no browser.</p><label className="mb-1 block text-xs muted" htmlFor="sell-reason">Motivo (opcional)</label><input id="sell-reason" className="input" value={reason} onChange={e => setReason(e.target.value)} maxLength={240} /><div className="mt-5 flex justify-end gap-2"><button type="button" className="button" onClick={() => setSellId(null)}>Cancelar</button><button type="submit" className="button button-danger" disabled={sell.isPending}>{sell.isPending ? 'A enviar…' : 'Enviar pedido'}</button></div>{sell.isError && <p className="negative text-xs">Falha no pedido. Aguardando conexão com o back-end</p>}</form></div>}
  </DataPageFrame>;
}

function TradesPage() {
  const [mint, setMint] = useState(''), [result, setResult] = useState(''), [from, setFrom] = useState(''), [to, setTo] = useState('');
  const [cursor, setCursor] = useState<string | undefined>();
  const params = useMemo<GetTradesParams>(() => ({ limit: 30, ...(mint ? { mint } : {}), ...(result ? { result: result as GetTradesParams['result'] } : {}), ...(from ? { from: new Date(from).toISOString() } : {}), ...(to ? { to: new Date(to).toISOString() } : {}), ...(cursor ? { cursor } : {}) }), [mint, result, from, to, cursor]);
  const page = useGetTrades(params, { query: { refetchInterval: LIVE_DATA_REFRESH_MS } });
  return <DataPageFrame description="Resultados reportados, com P&L realizado separado das estimativas de posições."><section className="panel mb-4 p-4"><div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4"><label className="text-xs muted">Mint<input className="input mt-1 mono" placeholder="Filtrar pelo mint" value={mint} onChange={e => { setMint(e.target.value); setCursor(undefined); }} /></label><label className="text-xs muted">Resultado<select className="select mt-1" value={result} onChange={e => { setResult(e.target.value); setCursor(undefined); }}><option value="">Todos</option><option value="positive">Positivo</option><option value="negative">Negativo</option><option value="break_even">Sem variação</option><option value="unknown">Desconhecido</option></select></label><label className="text-xs muted">De<input type="date" className="input mt-1" value={from} onChange={e => { setFrom(e.target.value); setCursor(undefined); }} /></label><label className="text-xs muted">Até<input type="date" className="input mt-1" value={to} onChange={e => { setTo(e.target.value); setCursor(undefined); }} /></label></div></section><QueryState loading={page.isLoading} error={page.isError} onRetry={() => void page.refetch()} />{!page.isLoading && !page.isError && <section className="panel"><PanelTitle title="Histórico de operações" /><>{page.data?.items.length ? <TradesTable items={page.data.items} /> : <EmptyRows>Não existem operações para os filtros selecionados.</EmptyRows>}</><Pagination nextCursor={page.data?.nextCursor} setCursor={setCursor} cursor={cursor} /></section>}</DataPageFrame>;
}

function RpcsPage() {
  const q = useGetRpcs({ query: { refetchInterval: 15000 } });
  const test = useTestRpc(), select = useSelectRpc();
  const qc = useQueryClient();
  const [confirm, setConfirm] = useState<{ id: string; role: 'primary' | 'backup' | 'disabled' } | null>(null);
  const [testType, setTestType] = useState<'read' | 'send' | 'confirmation'>('read');
  const busy = test.isPending || select.isPending;
  const onTest = (id: string) => test.mutate({ data: { endpointId: id, testType } }, { onSuccess: r => { notify({ text: r.accepted ? 'Teste aceite pelo back-end.' : r.message || 'Teste não confirmado.', kind: r.accepted ? 'success' : 'error' }); void qc.invalidateQueries({ queryKey: getGetRpcsQueryKey() }); }, onError: () => notify({ text: 'Aguardando conexão com o back-end', kind: 'error' }) });
  const onSelect = () => { if (!confirm) return; select.mutate({ data: { endpointId: confirm.id, role: confirm.role } }, { onSuccess: r => { if (r.accepted) { notify({ text: 'Configuração RPC aceite pelo back-end.', kind: 'success' }); void qc.invalidateQueries({ queryKey: getGetRpcsQueryKey() }); } else notify({ text: r.message || 'O back-end não confirmou a alteração.', kind: 'error' }); setConfirm(null); }, onError: () => { notify({ text: 'Aguardando conexão com o back-end', kind: 'error' }); setConfirm(null); } }); };
  return <DataPageFrame description="Métricas e seleção dependem exclusivamente da API. Endpoints podem ter custos e limites próprios.">
    <div className="mb-4 flex flex-wrap items-center justify-between gap-3 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-3"><div className="flex gap-2"><Server size={16} className="technical" /><span className="text-xs">Disponibilidade, latência e limites não presumidos.</span></div><select className="select w-auto" value={testType} onChange={e => setTestType(e.target.value as typeof testType)} aria-label="Tipo de teste RPC"><option value="read">Teste de leitura</option><option value="send">Teste de envio</option><option value="confirmation">Teste de confirmação</option></select></div>
    <QueryState loading={q.isLoading} error={q.isError} onRetry={() => void q.refetch()} />
    {!q.isLoading && !q.isError && <section className="panel"><PanelTitle title="Endpoints RPC reportados" aside={<span className="eyebrow">{q.data?.length ?? 0} ENDPOINTS</span>} />{q.data?.length ? <div className="table-wrap"><table><thead><tr><th>Fornecedor</th><th>Estado</th><th>Papel</th><th>Latência</th><th>Sucesso</th><th>Falhas / limite</th><th>Read · Send · Confirmação</th><th>Último teste</th><th>Ações</th></tr></thead><tbody>{q.data.map((r: RpcEndpoint) => <tr key={r.id}><td><div className="text-xs font-medium">{r.label}</div><div className="mono mt-1 max-w-44 truncate text-[10px] muted">{redactUrl(r.url)}</div></td><td><StatusTag value={r.status} positive={['healthy']} warning={['degraded', 'untested']} negative={['unavailable', 'disabled']} /></td><td><StatusTag value={r.role} positive={['primary']} warning={['backup']} negative={['disabled']} /></td><td className="mono text-xs">{r.latencyMs == null ? 'Indisponível' : `${fmt(r.latencyMs, 0)} ms`}</td><td className="mono text-xs">{r.successRate == null ? 'Indisponível' : `${fmt(r.successRate, 1)}%`}</td><td className="mono text-xs">{fmt(r.failureCount, 0)} / {fmt(r.rateLimitErrors, 0)}</td><td className="text-xs"><StatusTag value={r.readStatus} positive={['passed']} negative={['failed']} warning={['untested']} /><div className="mt-1"><StatusTag value={r.sendTestStatus} positive={['passed']} negative={['failed']} warning={['untested']} /></div><div className="mt-1"><StatusTag value={r.confirmationTestStatus} positive={['passed']} negative={['failed']} warning={['untested']} /></div></td><td className="whitespace-nowrap text-xs muted">{fmtDate(r.lastTestAt)}</td><td><div className="flex flex-wrap gap-1"><button className="button button-small" disabled={busy} onClick={() => onTest(r.id)}>Testar</button><button className="button button-small" disabled={busy} onClick={() => setConfirm({ id: r.id, role: 'primary' })}>Primário</button><button className="button button-small" disabled={busy} onClick={() => setConfirm({ id: r.id, role: 'backup' })}>Reserva</button><button className="button button-small button-danger" disabled={busy} onClick={() => setConfirm({ id: r.id, role: 'disabled' })}>Desativar</button></div></td></tr>)}</tbody></table></div> : <EmptyRows>Não implementado: endpoints candidatos ainda não foram configurados pela API.</EmptyRows>}</section>}
    {confirm && <div className="modal-backdrop"><div className="modal"><div className="eyebrow">ALTERAÇÃO DE INFRAESTRUTURA</div><h2 className="display mt-2 text-xl">Confirmar {confirm.role === 'disabled' ? 'desativação' : confirm.role === 'primary' ? 'RPC primário' : 'RPC de reserva'}</h2><p className="text-sm muted">A solicitação será enviada ao back-end e só será aplicada se este a aceitar.</p><div className="mt-5 flex justify-end gap-2"><button className="button" disabled={select.isPending} onClick={() => setConfirm(null)}>Cancelar</button><button className="button button-primary" disabled={select.isPending} onClick={onSelect}>{select.isPending ? 'A enviar…' : 'Confirmar'}</button></div></div></div>}
  </DataPageFrame>;
}
function redactUrl(value: string) { try { const url = new URL(value); return `${url.protocol}//${url.host}${url.pathname}${url.search ? '?…' : ''}`; } catch { return 'Indisponível'; } }

type RuntimeConfigView = {
  simulation: boolean;
  liveTradingEnabled: boolean;
  maxHoldSeconds: number;
  effectiveMaxHoldSeconds: number;
};

function StrategyPage() {
  const q = useGetConfig({ query: { refetchInterval: 30000 } });
  const config = q.data as unknown as RuntimeConfigView | undefined;
  return <DataPageFrame description="Parâmetros efetivos reportados pelo serviço. Esta tela é somente leitura.">
    <QueryState loading={q.isLoading} error={q.isError} onRetry={() => void q.refetch()} />
    {!q.isLoading && !q.isError && config && <section className="panel">
      <PanelTitle title="Tempo máximo de retenção" aside={<StatusTag value={config.simulation ? 'simulation' : config.liveTradingEnabled ? 'live' : 'live disabled'} positive={['simulation', 'live']} warning={['live disabled']} />} />
      <div className="grid gap-4 p-4 sm:grid-cols-2">
        <div className="rounded-lg border border-border/70 p-4">
          <div className="eyebrow">MAX_HOLD_SECONDS · CONFIGURADO</div>
          <div className="display mt-2 text-2xl">{config.maxHoldSeconds == null ? 'Indisponível' : `${fmt(config.maxHoldSeconds, 0)} s`}</div>
        </div>
        <div className="rounded-lg border border-border/70 p-4">
          <div className="eyebrow">LIMITE EFETIVO NO MONITOR</div>
          <div className="display mt-2 text-2xl">{config.effectiveMaxHoldSeconds == null ? 'Indisponível' : `${fmt(config.effectiveMaxHoldSeconds, 0)} s`}</div>
        </div>
      </div>
      <p className="mb-0 px-4 pb-4 text-xs muted">Somente leitura. Para alterar, ajuste MAX_HOLD_SECONDS no ambiente do serviço e reinicie o backend.</p>
    </section>}
  </DataPageFrame>;
}

function WalletPage() {
  const q = useGetWallet({ query: { refetchInterval: LIVE_DATA_REFRESH_MS } });
  const tokenHoldings = q.data?.tokenHoldings ?? [];
  const [copied, setCopied] = useState(false);
  const copy = async () => { if (!q.data?.publicAddress) return; try { await navigator.clipboard.writeText(q.data.publicAddress); setCopied(true); setTimeout(() => setCopied(false), 1600); } catch { notify({ text: 'Não foi possível copiar o endereço.', kind: 'error' }); } };
  return <DataPageFrame description="Visualização pública e sincronização reportada. Nenhuma chave privada é apresentada.">
    <QueryState loading={q.isLoading} error={q.isError} onRetry={() => void q.refetch()} />
    {!q.isLoading && !q.isError && q.data && <>
      <div className="mb-4 grid gap-3 sm:grid-cols-3"><Metric label="Saldo" value={fmtSol(q.data.balanceSol)} suffix="SOL" /><Metric label="Reserva de taxas" value={fmtSol(q.data.feesReserveSol)} suffix="SOL" tone="warning" /><section className="panel metric p-4"><div className="eyebrow">SINCRONIZAÇÃO</div><div className="mt-3"><StatusTag value={q.data.syncStatus} positive={['synced']} warning={['syncing', 'stale']} negative={['unavailable']} /></div><div className="mt-2 text-xs muted">{fmtDate(q.data.updatedAt)}</div></section></div>
      <section className="panel mb-4"><PanelTitle title="Endereço público" aside={<StatusTag value={q.data.syncStatus} positive={['synced']} warning={['syncing', 'stale']} negative={['unavailable']} />} /><div className="flex flex-wrap items-center justify-between gap-3 p-4"><code className="mono break-all text-xs">{q.data.publicAddress || 'Indisponível'}</code><button className="button button-small" disabled={!q.data.publicAddress} onClick={() => void copy()}>{copied ? <Check size={14} /> : <Copy size={14} />}{copied ? 'Copiado' : 'Copiar endereço'}</button></div></section>
      <section className="panel"><PanelTitle title="Ativos monitorizados" aside={<span className="eyebrow">{tokenHoldings.length} ATIVOS</span>} />{tokenHoldings.length ? <div className="table-wrap"><table><thead><tr><th>Ativo</th><th>Mint</th><th>Quantidade</th><th>Explorer</th></tr></thead><tbody>{tokenHoldings.map(t => <tr key={t.mint}><td className="text-xs font-medium">{t.symbol || 'Símbolo indisponível'}</td><td className="mono text-xs">{shortKey(t.mint)}</td><td className="mono text-xs">{fmt(t.quantity)}</td><td><a className="technical text-xs" href={`https://solscan.io/account/${encodeURIComponent(t.mint)}`} target="_blank" rel="noreferrer">Solscan <ExternalLink className="inline" size={12} /></a></td></tr>)}</tbody></table></div> : <EmptyRows>Não existem ativos reportados.</EmptyRows>}</section>
    </>}
  </DataPageFrame>;
}

function LogsPage() {
  const [category, setCategory] = useState(''), [severity, setSeverity] = useState(''), [from, setFrom] = useState(''), [to, setTo] = useState('');
  const [cursor, setCursor] = useState<string | undefined>();
  const params = useMemo<GetLogsParams>(() => ({ limit: 40, ...(category ? { category } : {}), ...(severity ? { severity: severity as GetLogsParams['severity'] } : {}), ...(from ? { from: new Date(from).toISOString() } : {}), ...(to ? { to: new Date(to).toISOString() } : {}), ...(cursor ? { cursor } : {}) }), [category, severity, from, to, cursor]);
  const page = useGetLogs(params, { query: { refetchInterval: LIVE_DATA_REFRESH_MS } });
  return <DataPageFrame description="Eventos operacionais filtrados. Conteúdo sensível não deve ser devolvido pela API.">
    <section className="panel mb-4 p-4"><div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4"><label className="text-xs muted">Categoria<input className="input mt-1" placeholder="Todas as categorias" value={category} onChange={e => { setCategory(e.target.value); setCursor(undefined); }} /></label><label className="text-xs muted">Severidade<select className="select mt-1" value={severity} onChange={e => { setSeverity(e.target.value); setCursor(undefined); }}><option value="">Todas</option>{['debug','info','warning','error','critical'].map(x => <option key={x} value={x}>{x}</option>)}</select></label><label className="text-xs muted">De<input type="datetime-local" className="input mt-1" value={from} onChange={e => { setFrom(e.target.value); setCursor(undefined); }} /></label><label className="text-xs muted">Até<input type="datetime-local" className="input mt-1" value={to} onChange={e => { setTo(e.target.value); setCursor(undefined); }} /></label></div></section>
    <QueryState loading={page.isLoading} error={page.isError} onRetry={() => void page.refetch()} />
    {!page.isLoading && !page.isError && <section className="panel"><PanelTitle title="Eventos" aside={<span className="eyebrow">{page.data?.items.length ?? 0} REGISTOS</span>} />{page.data?.items.length ? <div className="divide-y divide-[hsl(var(--border))]">{page.data.items.map(log => <article key={log.id} className="flex gap-3 px-4 py-3"><div className="mt-1"><StatusTag value={log.severity} negative={['error','critical']} warning={['warning']} positive={['info']} /></div><div className="min-w-0 flex-1"><div className="flex flex-wrap items-center gap-x-3 gap-y-1"><span className="text-xs font-medium">{log.category}</span><span className="mono text-[10px] muted">{fmtDate(log.timestamp)}</span></div><p className="mb-0 mt-1 break-words text-sm">{sanitizeLog(log.message)}</p>{log.details && <details className="mt-2 text-xs"><summary className="cursor-pointer muted">Detalhes estruturados</summary><pre className="mt-2 max-h-56 overflow-auto rounded bg-[hsl(var(--background))] p-3 mono text-[10px] muted">{JSON.stringify(redactDetails(log.details), null, 2)}</pre></details>}</div></article>)}</div> : <EmptyRows>Não existem registos para os filtros selecionados.</EmptyRows>}<Pagination nextCursor={page.data?.nextCursor} setCursor={setCursor} cursor={cursor} /></section>}
  </DataPageFrame>;
}
function sanitizeLog(text: string) { return text.replace(/(authorization|token|secret|private.?key|api.?key)\s*[:=]\s*[^\s,;]+/gi, '$1=[REDACTED]'); }
function redactDetails(details: unknown): unknown {
  if (Array.isArray(details)) return details.map(redactDetails);
  if (details && typeof details === 'object') return Object.fromEntries(Object.entries(details).map(([key, value]) => [/token|secret|private.?key|api.?key|authorization/i.test(key) ? key : key, /token|secret|private.?key|api.?key|authorization/i.test(key) ? '[REDACTED]' : redactDetails(value)]));
  if (typeof details === 'string') return sanitizeLog(details);
  return details;
}

function PortalRoute({ children }: { children: ReactNode }) {
  return <><Show when="signed-in">{children}</Show><Show when="signed-out"><Redirect to="/" /></Show></>;
}
function HomeRedirect() { return <><Show when="signed-in"><Redirect to="/user-portal" /></Show><Show when="signed-out"><HomePage /></Show></>; }
function SignInPage() { return <main className="flex min-h-[100dvh] items-center justify-center bg-[hsl(var(--background))] px-4 py-8"><SignIn routing="path" path={`${basePath}/sign-in`} signUpUrl={`${basePath}/sign-up`} /></main>; }
function SignUpPage() { return <main className="flex min-h-[100dvh] items-center justify-center bg-[hsl(var(--background))] px-4 py-8"><SignUp routing="path" path={`${basePath}/sign-up`} signInUrl={`${basePath}/sign-in`} /></main>; }
function ToastHost() {
  const [notice, setNotice] = useState<Notice | null>(null);
  useEffect(() => { notifyGlobal = setNotice; return () => { notifyGlobal = () => undefined; }; }, []);
  useEffect(() => { if (!notice) return; const t = setTimeout(() => setNotice(null), 4500); return () => clearTimeout(t); }, [notice]);
  return notice ? <div className={`toast-message ${notice.kind === 'success' ? 'positive' : notice.kind === 'error' ? 'negative' : ''}`} role="status" aria-live="polite">{notice.text}</div> : null;
}
function ClerkQueryClientCacheInvalidator() {
  const { addListener } = useClerk();
  const qc = useQueryClient();
  const prevUserIdRef = useRef<string | null | undefined>(undefined);
  useEffect(() => {
    const unsubscribe = addListener(({ user }) => {
      const userId = user?.id ?? null;
      if (prevUserIdRef.current !== undefined && prevUserIdRef.current !== userId) qc.clear();
      prevUserIdRef.current = userId;
    });
    return unsubscribe;
  }, [addListener, qc]);
  return null;
}
function ApiAuthTokenBridge() {
  const { getToken, isSignedIn } = useAuth();
  useEffect(() => {
    setAuthTokenGetter(hasConfiguredApiBase ? () => isSignedIn ? getToken() : null : null);
    return () => setAuthTokenGetter(null);
  }, [getToken, isSignedIn]);
  return null;
}
function ClerkRoutes() {
  const [, setLocation] = useLocation();
  return <ClerkProvider
    publishableKey={clerkPubKey}
    proxyUrl={clerkProxyUrl}
    appearance={clerkAppearance}
    signInUrl={`${basePath}/sign-in`}
    signUpUrl={`${basePath}/sign-up`}
    localization={{
      signIn: { start: { title: 'Bem-vindo de volta', subtitle: 'Inicie sessão para aceder à consola.' } },
      signUp: { start: { title: 'Criar conta', subtitle: 'Aceda à consola de operações Solana.' } },
    }}
    routerPush={(to) => setLocation(stripBase(to))}
    routerReplace={(to) => setLocation(stripBase(to), { replace: true })}
  >
    <ApiAuthTokenBridge />
    <QueryClientProvider client={queryClient}>
      <ClerkQueryClientCacheInvalidator />
      <Switch>
        <Route path="/" component={HomeRedirect} />
        <Route path="/sign-in/*?" component={SignInPage} />
        <Route path="/sign-up/*?" component={SignUpPage} />
        <Route path="/user-portal"><PortalRoute><AppShell><DashboardPage /></AppShell></PortalRoute></Route>
        <Route path="/user-portal/reports"><PortalRoute><AppShell><PerformanceReportPage /></AppShell></PortalRoute></Route>
        <Route path="/user-portal/tokens"><PortalRoute><AppShell><TokensPage /></AppShell></PortalRoute></Route>
        <Route path="/user-portal/positions"><PortalRoute><AppShell><PositionsPage /></AppShell></PortalRoute></Route>
        <Route path="/user-portal/trades"><PortalRoute><AppShell><TradesPage /></AppShell></PortalRoute></Route>
        <Route path="/user-portal/rpcs"><PortalRoute><AppShell><RpcsPage /></AppShell></PortalRoute></Route>
        <Route path="/user-portal/strategy"><PortalRoute><AppShell><StrategyPage /></AppShell></PortalRoute></Route>
        <Route path="/user-portal/wallet"><PortalRoute><AppShell><WalletPage /></AppShell></PortalRoute></Route>
        <Route path="/user-portal/logs"><PortalRoute><AppShell><LogsPage /></AppShell></PortalRoute></Route>
        <Route component={NotFound} />
      </Switch>
      <ToastHost />
    </QueryClientProvider>
  </ClerkProvider>;
}
function MasterLogin() {
  const [, setLocation] = useLocation();
  const { token } = useMasterAuth();
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [pending, setPending] = useState(false);
  const signIn = useMasterSignIn();
  if (token) return <Redirect to="/user-portal" />;
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setPending(true); setError('');
    try {
      const response = await fetch(`${apiBase}/api/auth/login`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password }) });
      const body = await response.json();
      if (!response.ok || !body?.token) throw new Error('Senha mestra inválida.');
      signIn(body.token);
      setLocation('/user-portal');
    } catch (cause) { setError(cause instanceof Error ? cause.message : 'Não foi possível iniciar sessão.'); }
    finally { setPending(false); }
  };
  return <main className="flex min-h-[100dvh] items-center justify-center bg-[hsl(var(--background))] px-4"><form className="panel w-full max-w-md p-6 sm:p-8" onSubmit={submit}><div className="eyebrow">SOLANA SNIPER · ACESSO RESTRITO</div><h1 className="display mt-3 text-3xl">Entrar no console</h1><p className="mt-2 text-sm muted">Informe a senha mestra configurada no servidor.</p><label className="mt-6 block text-sm">Senha mestra<input className="input mt-2 w-full" type="password" value={password} onChange={event => setPassword(event.target.value)} autoFocus autoComplete="current-password" required /></label>{error && <p className="mt-3 text-sm negative" role="alert">{error}</p>}<button className="button button-primary mt-6 w-full justify-center" disabled={pending}>{pending ? 'Validando…' : 'Entrar'}</button></form></main>;
}
const MasterSignInContext = createContext<(token: string) => void>(() => undefined);
function useMasterSignIn() { return useContext(MasterSignInContext); }
function MasterPortalRoute({ children }: { children: ReactNode }) { return useMasterAuth().token ? <>{children}</> : <Redirect to="/sign-in" />; }
function MasterRoutes() {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem(authStorageKey));
  const signIn = (nextToken: string) => { localStorage.setItem(authStorageKey, nextToken); setToken(nextToken); };
  const signOut = () => { localStorage.removeItem(authStorageKey); setToken(null); window.location.assign(`${basePath || ''}/sign-in`); };
  useEffect(() => { setAuthTokenGetter(token ? () => token : null); return () => setAuthTokenGetter(null); }, [token]);
  const auth = useMemo(() => ({ token, signOut }), [token]);
  return <AuthContext.Provider value={auth}>
    <MasterSignInContext.Provider value={signIn}>
      <QueryClientProvider client={queryClient}>
        <Switch>
          <Route path="/" component={() => token ? <Redirect to="/user-portal" /> : <Redirect to="/sign-in" />} />
          <Route path="/sign-in" component={MasterLogin} />
          <Route path="/user-portal"><MasterPortalRoute><AppShell><DashboardPage /></AppShell></MasterPortalRoute></Route>
          <Route path="/user-portal/reports"><MasterPortalRoute><AppShell><PerformanceReportPage /></AppShell></MasterPortalRoute></Route>
          <Route path="/user-portal/tokens"><MasterPortalRoute><AppShell><TokensPage /></AppShell></MasterPortalRoute></Route>
          <Route path="/user-portal/positions"><MasterPortalRoute><AppShell><PositionsPage /></AppShell></MasterPortalRoute></Route>
          <Route path="/user-portal/trades"><MasterPortalRoute><AppShell><TradesPage /></AppShell></MasterPortalRoute></Route>
          <Route path="/user-portal/rpcs"><MasterPortalRoute><AppShell><RpcsPage /></AppShell></MasterPortalRoute></Route>
          <Route path="/user-portal/strategy"><MasterPortalRoute><AppShell><StrategyPage /></AppShell></MasterPortalRoute></Route>
          <Route path="/user-portal/wallet"><MasterPortalRoute><AppShell><WalletPage /></AppShell></MasterPortalRoute></Route>
          <Route path="/user-portal/logs"><MasterPortalRoute><AppShell><LogsPage /></AppShell></MasterPortalRoute></Route>
          <Route component={NotFound} />
        </Switch>
        <ToastHost />
      </QueryClientProvider>
    </MasterSignInContext.Provider>
  </AuthContext.Provider>;
}
function App() { return <WouterRouter base={basePath}><MasterRoutes /></WouterRouter>; }

export default App;
