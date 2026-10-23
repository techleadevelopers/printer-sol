import { useMemo, useState } from 'react';
import {
  AlertTriangle, Check, CircleHelp, FlaskConical, Play, RotateCcw, ShieldCheck,
  X,
} from 'lucide-react';

type LabInputs = {
  executionMode: 'paper' | 'live';
  websocketMarketCap: boolean;
  rpcAvailable: boolean;
  activePositionExists: boolean;
  previousBuyExists: boolean;
  creatorBlocked: boolean;
  queueAvailable: boolean;
  transactionOutcome: 'confirmed' | 'definitive-failure' | 'unknown';
  rugCheckFailed: boolean;
  realSolReserves: number;
  minReserves: number;
  maxReserves: number;
  marketCapSol: number;
  minMarketCap: number;
  maxMarketCap: number;
  ageAtDetection: number;
  entryDelay: number;
  networkDelayMs: number;
  maxTokenAge: number;
  quoteAgeMs: number;
  maxQuoteAgeMs: number;
  orderSol: number;
  virtualSolReserves: number;
  virtualTokenReserves: number;
  requestedSlippageBps: number;
  maxSlippageBps: number;
  maxCurveImpactBps: number;
  positionCostSol: number;
  currentPositionSol: number;
  peakPositionSol: number;
  heldSeconds: number;
  maxHoldSeconds: number;
  exitMarkFresh: boolean;
  takeProfitPct: number;
  stopLossPct: number;
  trailingStopPct: number;
  trailingArmPct: number;
  exitLiquidityVerified: boolean;
};

const DEFAULTS: LabInputs = {
  executionMode: 'live',
  websocketMarketCap: true,
  rpcAvailable: true,
  activePositionExists: false,
  previousBuyExists: false,
  creatorBlocked: false,
  queueAvailable: true,
  transactionOutcome: 'confirmed',
  rugCheckFailed: false,
  realSolReserves: 0.7,
  minReserves: 0,
  maxReserves: 1.5,
  marketCapSol: 35,
  minMarketCap: 20,
  maxMarketCap: 0,
  ageAtDetection: 1,
  entryDelay: 5,
  networkDelayMs: 180,
  maxTokenAge: 30,
  quoteAgeMs: 250,
  maxQuoteAgeMs: 1500,
  orderSol: 0.03,
  virtualSolReserves: 30,
  virtualTokenReserves: 100_000_000,
  requestedSlippageBps: 500,
  maxSlippageBps: 1500,
  maxCurveImpactBps: 1000,
  positionCostSol: 0.03,
  currentPositionSol: 0.03,
  peakPositionSol: 0.033,
  heldSeconds: 45,
  maxHoldSeconds: 300,
  exitMarkFresh: true,
  takeProfitPct: 50,
  stopLossPct: 25,
  trailingStopPct: 12,
  trailingArmPct: 10,
  exitLiquidityVerified: true,
};

type Check = { label: string; status: 'pass' | 'fail' | 'warn'; detail: string };
type Simulation = { checks: Check[]; buyAllowed: boolean; impactBps: number; expectedTokens: number; minimumTokens: number; exitReason: string; backendReserveGap: boolean; reservesPass: boolean };

const PRESETS: { label: string; description: string; patch: Partial<LabInputs> }[] = [
  { label: 'Fluxo saudável', description: 'Entrada elegível e cotação dentro dos limites.', patch: {} },
  { label: 'Feed/RPC indisponível', description: 'Falha fechada no modo que exige validação RPC.', patch: { websocketMarketCap: false, rpcAvailable: false } },
  { label: 'Entrada atrasada', description: 'A idade excede o limite após espera e latência.', patch: { ageAtDetection: 23, entryDelay: 5, networkDelayMs: 2500 } },
  { label: 'Reservas abaixo do mínimo', description: 'Exercita a faixa inclusiva da curva real em modo RPC.', patch: { websocketMarketCap: false, realSolReserves: 0.2, minReserves: 0.3 } },
  { label: 'Cotação vencida', description: 'A cotação envelhece antes do envio.', patch: { quoteAgeMs: 2200, maxQuoteAgeMs: 1500 } },
  { label: 'Impacto alto', description: 'Ordem grande para a reserva virtual disponível.', patch: { orderSol: 1.2, virtualSolReserves: 5, maxCurveImpactBps: 1000 } },
  { label: 'Mint já comprado', description: 'Impede duplicar entrada ou repetir compra persistida.', patch: { previousBuyExists: true } },
  { label: 'Saída desconhecida', description: 'Transação sem resultado bloqueia retry até reconciliação.', patch: { transactionOutcome: 'unknown' } },
];

function simulate(input: LabInputs): Simulation {
  const checks: Check[] = [];
  const elapsedAge = input.ageAtDetection + input.entryDelay + input.networkDelayMs / 1000;
  const effectiveMarketCap = input.marketCapSol;
  const reservesPass = input.realSolReserves >= input.minReserves && input.realSolReserves <= input.maxReserves;
  const validTiming = input.ageAtDetection >= 0 && input.entryDelay >= 0 && input.networkDelayMs >= 0
    && input.maxTokenAge >= 0 && Number.isFinite(elapsedAge);
  const validLimits = input.maxQuoteAgeMs >= 1 && input.maxQuoteAgeMs <= 120_000
    && input.requestedSlippageBps >= 0 && input.maxSlippageBps >= 1 && input.maxSlippageBps <= 10_000
    && input.maxCurveImpactBps >= 1 && input.maxCurveImpactBps <= 10_000;
  const marketCapPass = Number.isFinite(effectiveMarketCap) && effectiveMarketCap > 0
    && effectiveMarketCap >= input.minMarketCap
    && (input.maxMarketCap <= 0 || effectiveMarketCap <= input.maxMarketCap);

  checks.push({
    label: 'Parâmetros de cenário',
    status: validTiming && validLimits && input.orderSol > 0 ? 'pass' : 'fail',
    detail: !validTiming
      ? 'Idades e latências precisam ser não negativas; 0 desativa somente o filtro de idade do token'
      : !validLimits
        ? 'Limites inválidos: quote age 1–120000 ms, slippage e impacto entre 1 e 10000 bps'
        : input.orderSol <= 0
          ? 'O tamanho da ordem deve ser positivo'
          : 'Valores em faixas utilizáveis para este modelo',
  });
  checks.push({
    label: 'Idade do token após espera',
    status: !validTiming || input.maxTokenAge > 0 && elapsedAge >= input.maxTokenAge ? 'fail' : 'pass',
    detail: `${elapsedAge.toFixed(2)} s no gate${input.maxTokenAge > 0 ? ` · limite estrito < ${input.maxTokenAge} s` : ' · filtro desativado (limite 0)'}`,
  });
  checks.push({
    label: 'Reservas reais da bonding curve · política esperada',
    status: !input.rpcAvailable || !reservesPass ? 'fail' : 'pass',
    detail: !input.rpcAvailable
      ? 'RPC indisponível: reservas não verificáveis; política exige bloquear em qualquer modo'
      : `${input.realSolReserves.toFixed(3)} SOL · faixa inclusiva ${input.minReserves.toFixed(3)}–${input.maxReserves.toFixed(3)} SOL`,
  });
  checks.push({
    label: 'Duplicidade e posição anterior',
    status: input.activePositionExists || input.previousBuyExists ? 'fail' : 'pass',
    detail: input.activePositionExists ? 'Já existe posição não reconciliada para este mint'
      : input.previousBuyExists ? 'Já existe tentativa/compra persistida; nova compra bloqueada'
        : 'Sem posição ativa ou compra persistida para o mint',
  });
  checks.push({
    label: 'Blacklist do criador',
    status: input.creatorBlocked ? 'fail' : 'pass',
    detail: input.creatorBlocked ? 'Criador bloqueado após stop-loss; entrada rejeitada' : 'Criador não está bloqueado neste cenário',
  });
  checks.push({
    label: 'Deduplicação e capacidade da fila',
    status: input.queueAvailable ? 'pass' : 'fail',
    detail: input.queueAvailable ? 'Mint não está pendente e há capacidade para enfileirar' : 'Duplicado dentro do TTL ou fila cheia; entrada rejeitada',
  });
  checks.push({
    label: 'Market cap de entrada',
    status: !input.websocketMarketCap && !input.rpcAvailable ? 'fail' : marketCapPass ? 'pass' : 'fail',
    detail: `${effectiveMarketCap.toFixed(2)} SOL via ${input.websocketMarketCap ? 'WebSocket' : 'RPC'} · mínimo ${input.minMarketCap} SOL${input.maxMarketCap > 0 ? ` · máximo ${input.maxMarketCap} SOL` : ' · teto desativado'}`,
  });

  const validQuoteInputs = input.orderSol > 0 && input.virtualSolReserves > 0 && input.virtualTokenReserves > 0;
  const expectedTokens = validQuoteInputs ? input.virtualTokenReserves * input.orderSol / (input.virtualSolReserves + input.orderSol) : 0;
  const impactBps = validQuoteInputs ? input.orderSol / (input.virtualSolReserves + input.orderSol) * 10_000 : Number.POSITIVE_INFINITY;
  const minimumTokens = expectedTokens * (1 - Math.min(input.requestedSlippageBps, input.maxSlippageBps) / 10_000);
  checks.push({
    label: 'Cotação da curva',
    status: input.executionMode === 'paper' ? 'warn' : !validQuoteInputs || !input.rpcAvailable || input.quoteAgeMs > input.maxQuoteAgeMs ? 'fail' : 'pass',
    detail: input.executionMode === 'paper'
      ? 'Informativo: o fluxo paper não executa a preparação live que valida idade da cotação'
      : !validQuoteInputs
        ? 'Ordem e reservas virtuais devem ser positivas para gerar uma cotação válida'
        : !input.rpcAvailable
          ? 'RPC indisponível: sem cotação fresca da curva'
          : `idade ${input.quoteAgeMs} ms · máximo ${input.maxQuoteAgeMs} ms`,
  });
  checks.push({
    label: 'Slippage solicitado',
    status: input.requestedSlippageBps <= input.maxSlippageBps ? 'pass' : 'fail',
    detail: `${input.requestedSlippageBps} bps (${(input.requestedSlippageBps / 100).toFixed(2)}%) · máximo ${input.maxSlippageBps} bps`,
  });
  checks.push({
    label: 'Impacto estimado da curva',
    status: input.executionMode === 'paper' ? 'warn' : impactBps <= input.maxCurveImpactBps ? 'pass' : 'fail',
    detail: input.executionMode === 'paper'
      ? `${Number.isFinite(impactBps) ? `${impactBps.toFixed(1)} bps` : 'cotação inválida'} · apenas informativo no paper; o gate de impacto é da preparação live`
      : `${Number.isFinite(impactBps) ? impactBps.toFixed(1) : 'inválido'} bps · máximo ${input.maxCurveImpactBps} bps`,
  });

  const buyAllowed = checks.every(check => check.status !== 'fail');
  const positionChange = input.positionCostSol > 0
    ? (input.currentPositionSol - input.positionCostSol) / input.positionCostSol * 100
    : 0;
  const peakGain = input.positionCostSol > 0
    ? (input.peakPositionSol - input.positionCostSol) / input.positionCostSol * 100
    : 0;
  const drawdownFromPeak = input.peakPositionSol > 0
    ? (input.peakPositionSol - input.currentPositionSol) / input.peakPositionSol * 100
    : 0;
  let exitReason = 'Manter posição: nenhum limite de saída acionado.';
  if (!input.exitMarkFresh) exitReason = 'Sem saída: marca ausente ou desatualizada; posição permanece aberta.';
  else if (!input.exitLiquidityVerified) exitReason = 'Sem saída: liquidez não verificada.';
  else if (positionChange <= -input.stopLossPct) exitReason = 'Stop-loss: prioridade sobre os demais gatilhos.';
  else if (peakGain >= input.trailingArmPct && drawdownFromPeak >= input.trailingStopPct) exitReason = 'Trailing stop: armado pelo pico e retração atingida.';
  else if (positionChange >= input.takeProfitPct) exitReason = 'Take-profit: valorização atingiu o limite.';
  else if (input.heldSeconds >= input.maxHoldSeconds && positionChange <= 0) exitReason = 'Max hold: posição no zero ou negativa.';
  else if (input.heldSeconds >= input.maxHoldSeconds && positionChange > 0) exitReason = 'Max hold atingido, mas posição positiva permanece aberta.';

  const backendReserveGap = input.websocketMarketCap;
  return { checks, buyAllowed, impactBps, expectedTokens, minimumTokens, exitReason, backendReserveGap, reservesPass };
}

function Field({ label, value, unit, step = 1, onChange }: { label: string; value: number; unit?: string; step?: number; onChange: (value: number) => void }) {
  return <label className="block text-xs">
    <span className="muted">{label}</span>
    <span className="mt-1 flex items-center gap-2">
      <input className="input mono" type="number" min="0" step={step} value={value} onChange={event => onChange(Number(event.target.value))} />
      {unit && <span className="w-12 shrink-0 text-[10px] muted">{unit}</span>}
    </span>
  </label>;
}

function CheckRow({ check }: { check: Check }) {
  const Icon = check.status === 'pass' ? Check : check.status === 'fail' ? X : AlertTriangle;
  const color = check.status === 'pass' ? 'positive' : check.status === 'fail' ? 'negative' : 'warning';
  return <div className="flex items-start gap-3 border-b border-[hsl(var(--border))] py-3 last:border-0">
    <Icon size={15} className={`mt-0.5 shrink-0 ${color}`} />
    <div className="min-w-0"><div className="text-xs font-medium">{check.label}</div><div className="mt-1 text-[11px] leading-5 muted">{check.detail}</div></div>
  </div>;
}

export default function FlowLabPage() {
  const [input, setInput] = useState<LabInputs>(DEFAULTS);
  const [hasRun, setHasRun] = useState(false);
  const [activePreset, setActivePreset] = useState(0);
  const result = useMemo(() => simulate(input), [input]);
  const set = <K extends keyof LabInputs>(key: K, value: LabInputs[K]) => setInput(current => ({ ...current, [key]: value }));

  const applyPreset = (index: number) => {
    setInput({ ...DEFAULTS, ...PRESETS[index].patch });
    setActivePreset(index);
    setHasRun(false);
  };

  const curveRows = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5].map(order => ({
    order,
    impact: order / (input.virtualSolReserves + order) * 10_000,
    tokens: input.virtualTokenReserves * order / (input.virtualSolReserves + order),
  }));
  const transactionResult = input.transactionOutcome === 'confirmed'
    ? 'Confirmação recebida: pode avançar com a liquidação.'
    : input.transactionOutcome === 'definitive-failure'
      ? 'Falha definitiva: manter estado de falha e permitir nova tentativa somente com as salvaguardas de retry.'
      : 'Resultado desconhecido: bloquear nova compra/venda para este mint até reconciliar; não presumir falha.';

  return <div className="page-wrap enter">
    <div className="mb-6 flex flex-wrap items-start justify-between gap-4">
      <div><div className="eyebrow">AMBIENTE LOCAL · SEM ORDENS</div><h1 className="display mb-1 mt-2 text-2xl font-semibold">Laboratório de fluxo</h1>
        <p className="m-0 max-w-3xl text-sm leading-6 muted">Explore latência, idade de token, reservas, slippage e impacto antes de comparar o comportamento esperado com o bot.</p></div>
      <div className="flex items-center gap-2 rounded-md border border-[hsl(var(--border))] px-3 py-2 text-xs technical"><ShieldCheck size={15} /> Simulação isolada no navegador</div>
    </div>

    <div className="mb-4 flex items-start gap-3 rounded-lg border border-sky-400/25 bg-sky-400/5 p-3 text-xs leading-5 technical">
      <CircleHelp size={16} className="mt-0.5 shrink-0" />
      <p className="m-0"><strong>Importante:</strong> esta tela não chama cloud, RPC, PumpPortal ou API e não altera a configuração do bot. Os valores são editáveis e locais; o resultado é um modelo de cenário para análise, não uma cotação ou garantia de execução real.</p>
    </div>

    <div className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
      {PRESETS.map((preset, index) => <button key={preset.label} type="button" onClick={() => applyPreset(index)}
        className={`panel p-3 text-left transition-colors hover:border-[hsl(var(--primary)/.55)] ${activePreset === index ? 'border-[hsl(var(--primary)/.65)]' : ''}`}>
        <span className="block text-xs font-semibold">{preset.label}</span><span className="mt-1 block text-[11px] leading-4 muted">{preset.description}</span>
      </button>)}
    </div>

    <div className="mb-4 grid gap-4 xl:grid-cols-[1.05fr_.95fr]">
      <section className="panel">
        <div className="flex items-center justify-between border-b border-[hsl(var(--border))] px-4 py-3">
          <h2 className="m-0 flex items-center gap-2 text-sm font-semibold"><FlaskConical size={16} className="technical" /> Cenário de entrada</h2>
          <button className="button button-small" type="button" onClick={() => { setInput(DEFAULTS); setActivePreset(0); setHasRun(false); }}><RotateCcw size={13} /> Restaurar</button>
        </div>
        <div className="grid gap-4 p-4 sm:grid-cols-2">
          <label className="block text-xs"><span className="muted">Modo de execução modelado</span>
            <select className="select mt-1" value={input.executionMode} onChange={event => set('executionMode', event.target.value as LabInputs['executionMode'])}>
              <option value="paper">Paper / simulação</option><option value="live">Live / proteção de execução</option>
            </select>
          </label>
          <div className="flex items-center rounded-md border border-[hsl(var(--border))] p-3 text-[11px] leading-5 muted">
            No paper, slippage solicitado ainda passa pelo limite configurado; idade de cotação e impacto da curva são informativos, pois não há preparação de swap live.
          </div>
          <label className="flex items-center justify-between gap-3 rounded-md border border-[hsl(var(--border))] p-3 text-xs">
            <span><strong className="block">Market cap via WebSocket</strong><span className="mt-1 block muted">Desligado: gate RPC e reservas são obrigatórios.</span></span>
            <input aria-label="Usar market cap via WebSocket" type="checkbox" checked={input.websocketMarketCap} onChange={event => set('websocketMarketCap', event.target.checked)} />
          </label>
          <label className="flex items-center justify-between gap-3 rounded-md border border-[hsl(var(--border))] p-3 text-xs">
            <span><strong className="block">RPC disponível</strong><span className="mt-1 block muted">Falha fecha a entrada nos gates que exigem RPC.</span></span>
            <input aria-label="RPC disponível" type="checkbox" checked={input.rpcAvailable} onChange={event => set('rpcAvailable', event.target.checked)} />
          </label>
          <label className="flex items-center justify-between gap-3 rounded-md border border-[hsl(var(--border))] p-3 text-xs">
            <span><strong className="block">Posição não reconciliada</strong><span className="mt-1 block muted">Bloqueia nova entrada no mesmo mint.</span></span>
            <input type="checkbox" checked={input.activePositionExists} onChange={event => set('activePositionExists', event.target.checked)} />
          </label>
          <label className="flex items-center justify-between gap-3 rounded-md border border-[hsl(var(--border))] p-3 text-xs">
            <span><strong className="block">Compra já persistida</strong><span className="mt-1 block muted">Evita uma segunda tentativa de compra.</span></span>
            <input type="checkbox" checked={input.previousBuyExists} onChange={event => set('previousBuyExists', event.target.checked)} />
          </label>
          <label className="flex items-center justify-between gap-3 rounded-md border border-[hsl(var(--border))] p-3 text-xs">
            <span><strong className="block">Criador bloqueado</strong><span className="mt-1 block muted">Simula bloqueio após stop-loss.</span></span>
            <input type="checkbox" checked={input.creatorBlocked} onChange={event => set('creatorBlocked', event.target.checked)} />
          </label>
          <label className="flex items-center justify-between gap-3 rounded-md border border-[hsl(var(--border))] p-3 text-xs">
            <span><strong className="block">Fila disponível</strong><span className="mt-1 block muted">Desmarque para simular fila cheia/dedupe ativo.</span></span>
            <input type="checkbox" checked={input.queueAvailable} onChange={event => set('queueAvailable', event.target.checked)} />
          </label>
          <Field label="Reservas reais da curva" value={input.realSolReserves} unit="SOL" step={0.01} onChange={v => set('realSolReserves', v)} />
          <Field label="Faixa inclusiva de reservas" value={input.minReserves} unit="mín. SOL" step={0.01} onChange={v => set('minReserves', v)} />
          <Field label="Teto de reservas" value={input.maxReserves} unit="máx. SOL" step={0.01} onChange={v => set('maxReserves', v)} />
          <Field label="Market cap" value={input.marketCapSol} unit="SOL" step={1} onChange={v => set('marketCapSol', v)} />
          <Field label="Limites de market cap" value={input.minMarketCap} unit="mín. SOL" step={1} onChange={v => set('minMarketCap', v)} />
          <Field label="Teto de market cap (0 = desligado)" value={input.maxMarketCap} unit="máx. SOL" step={1} onChange={v => set('maxMarketCap', v)} />
          <Field label="Idade ao detectar" value={input.ageAtDetection} unit="segundos" step={0.1} onChange={v => set('ageAtDetection', v)} />
          <Field label="Espera antes de avaliar" value={input.entryDelay} unit="segundos" step={0.1} onChange={v => set('entryDelay', v)} />
          <Field label="Latência até avaliação" value={input.networkDelayMs} unit="ms" step={10} onChange={v => set('networkDelayMs', v)} />
          <Field label="Idade máxima do token (0 = desligado)" value={input.maxTokenAge} unit="segundos" step={1} onChange={v => set('maxTokenAge', v)} />
          <Field label="Tamanho da ordem de compra" value={input.orderSol} unit="SOL" step={0.005} onChange={v => set('orderSol', v)} />
          <Field label="Reservas virtuais de SOL" value={input.virtualSolReserves} unit="SOL" step={0.1} onChange={v => set('virtualSolReserves', v)} />
          <Field label="Reservas virtuais de tokens" value={input.virtualTokenReserves} unit="tokens" step={100000} onChange={v => set('virtualTokenReserves', v)} />
          <Field label="Idade da cotação no envio" value={input.quoteAgeMs} unit="ms" step={10} onChange={v => set('quoteAgeMs', v)} />
          <Field label="Idade máxima da cotação" value={input.maxQuoteAgeMs} unit="ms" step={10} onChange={v => set('maxQuoteAgeMs', v)} />
          <Field label="Slippage solicitado" value={input.requestedSlippageBps} unit="bps" step={10} onChange={v => set('requestedSlippageBps', v)} />
          <Field label="Limite máximo de slippage" value={input.maxSlippageBps} unit="bps" step={10} onChange={v => set('maxSlippageBps', v)} />
          <Field label="Impacto máximo da curva" value={input.maxCurveImpactBps} unit="bps" step={10} onChange={v => set('maxCurveImpactBps', v)} />
        </div>
        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-[hsl(var(--border))] px-4 py-3">
          <span className="text-[10px] muted">Reservas reais verificam elegibilidade; reservas virtuais estimam impacto da ordem.</span>
          <button className="button button-primary button-small" type="button" onClick={() => setHasRun(true)}><Play size={13} /> Executar cenário</button>
        </div>
      </section>

      <div className="space-y-4">
        <section className="panel">
          <div className="flex items-center justify-between border-b border-[hsl(var(--border))] px-4 py-3">
            <h2 className="m-0 text-sm font-semibold">Trilha de decisão</h2>
            <span className={`text-xs font-semibold ${result.buyAllowed ? 'positive' : 'negative'}`}>{result.buyAllowed ? 'COMPRA ELEGÍVEL' : 'COMPRA BLOQUEADA'}</span>
          </div>
          <div className="px-4 py-1">{result.checks.map(check => <CheckRow key={check.label} check={check} />)}</div>
          <div className="border-t border-[hsl(var(--border))] p-4">
            <div className="eyebrow">RESULTADO DO CENÁRIO</div>
            <p className="mb-0 mt-2 text-xs leading-5 muted">{hasRun ? `Cenário processado localmente. ${result.buyAllowed ? 'Os gates simulados aprovaram a entrada.' : 'Um ou mais gates simularam bloqueio.'}` : 'Ajuste os parâmetros ou escolha um preset; os resultados são recalculados ao vivo. Clique em “Executar cenário” para registrar que avaliou esta configuração.'}</p>
          </div>
          {result.backendReserveGap && <div className="border-t border-amber-400/25 bg-amber-400/5 p-4 text-xs leading-5 warning" role="alert"><strong>Desvio de política encontrado no backend:</strong> a política documentada exige validar reservas em todos os modos, mas o caminho atual de market cap WebSocket desativa o gate de reservas na avaliação do candidato. {input.rpcAvailable && result.reservesPass ? 'Este cenário está dentro da faixa; altere as reservas para fora dos limites para reproduzir a divergência.' : 'Neste cenário, a política esperada bloqueia, mas o gate de entrada WebSocket não consulta as reservas.'} A preparação de uma execução live ainda pode falhar depois ao exigir cotação RPC. O backend não foi alterado.</div>}
        </section>
        <section className="panel">
          <div className="border-b border-[hsl(var(--border))] px-4 py-3"><h2 className="m-0 text-sm font-semibold">Cotação estimada da compra</h2></div>
          <div className="grid grid-cols-2 gap-3 p-4">
            <div><div className="eyebrow">IMPACTO DA CURVA</div><div className={`display mt-2 text-xl ${result.impactBps <= input.maxCurveImpactBps ? 'positive' : 'negative'}`}>{Number.isFinite(result.impactBps) ? `${result.impactBps.toFixed(1)} bps` : 'Inválido'}</div></div>
            <div><div className="eyebrow">SAÍDA MÍNIMA TEÓRICA</div><div className="display mt-2 text-xl">{result.minimumTokens.toLocaleString('pt-BR', { maximumFractionDigits: 2 })}</div><div className="text-[10px] muted">tokens após slippage configurado</div></div>
            <div className="col-span-2 rounded-md border border-[hsl(var(--border))] p-3 text-[11px] leading-5 muted">Saída estimada antes do slippage: <span className="mono">{result.expectedTokens.toLocaleString('pt-BR', { maximumFractionDigits: 2 })}</span> tokens. Modelo simplificado baseado nas reservas virtuais e na fórmula de impacto usada pelo bot; não representa uma cotação do Pump.fun.</div>
          </div>
        </section>
      </div>
    </div>

    <section className="panel mb-4">
      <div className="border-b border-[hsl(var(--border))] px-4 py-3">
        <h2 className="m-0 text-sm font-semibold">Curva de impacto por tamanho de ordem</h2>
        <p className="mb-0 mt-1 text-xs muted">Recalculada com as reservas virtuais acima. Valores de saída são estimativas simplificadas.</p>
      </div>
      <div className="table-wrap"><table className="min-w-[560px]">
        <thead><tr><th>Ordem</th><th>Impacto estimado</th><th>Tokens antes de slippage</th><th>Limite configurado</th><th>Resultado</th></tr></thead>
        <tbody>{curveRows.map(row => <tr key={row.order}>
          <td className="mono text-xs">{row.order.toFixed(3)} SOL</td>
          <td className="mono text-xs">{row.impact.toFixed(1)} bps</td>
          <td className="mono text-xs">{row.tokens.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}</td>
          <td className="mono text-xs">{input.maxCurveImpactBps} bps</td>
          <td className={`text-xs font-medium ${row.impact <= input.maxCurveImpactBps ? 'positive' : 'negative'}`}>{row.impact <= input.maxCurveImpactBps ? 'Dentro do limite' : 'Bloqueada'}</td>
        </tr>)}</tbody>
      </table></div>
    </section>

    <section className="panel mb-4">
      <div className="border-b border-[hsl(var(--border))] px-4 py-3"><h2 className="m-0 text-sm font-semibold">Simulador de saída</h2><p className="mb-0 mt-1 text-xs muted">Aplica a ordem dos gatilhos do bot; saída só é considerada se a liquidez estiver verificada.</p></div>
      <div className="grid gap-4 p-4 sm:grid-cols-2 lg:grid-cols-4">
        <Field label="Custo de entrada" value={input.positionCostSol} unit="SOL" step={0.001} onChange={v => set('positionCostSol', v)} />
        <Field label="Valor atual" value={input.currentPositionSol} unit="SOL" step={0.001} onChange={v => set('currentPositionSol', v)} />
        <Field label="Pico da posição" value={input.peakPositionSol} unit="SOL" step={0.001} onChange={v => set('peakPositionSol', v)} />
        <Field label="Tempo retido" value={input.heldSeconds} unit="segundos" step={1} onChange={v => set('heldSeconds', v)} />
        <Field label="Max hold" value={input.maxHoldSeconds} unit="segundos" step={1} onChange={v => set('maxHoldSeconds', v)} />
        <Field label="Take-profit" value={input.takeProfitPct} unit="%" step={1} onChange={v => set('takeProfitPct', v)} />
        <Field label="Stop-loss" value={input.stopLossPct} unit="%" step={1} onChange={v => set('stopLossPct', v)} />
        <Field label="Trailing stop / armado em" value={input.trailingStopPct} unit="retração %" step={1} onChange={v => set('trailingStopPct', v)} />
        <Field label="Armar trailing após ganho" value={input.trailingArmPct} unit="%" step={1} onChange={v => set('trailingArmPct', v)} />
        <label className="flex items-center gap-2 text-xs"><input type="checkbox" checked={input.exitLiquidityVerified} onChange={event => set('exitLiquidityVerified', event.target.checked)} /> Liquidez de saída verificada</label>
        <label className="flex items-center gap-2 text-xs"><input type="checkbox" checked={input.exitMarkFresh} onChange={event => set('exitMarkFresh', event.target.checked)} /> Marca atual válida e fresca</label>
        <div className="sm:col-span-2 lg:col-span-3 flex items-center gap-2 rounded-md border border-[hsl(var(--border))] p-3 text-xs">
          {result.exitReason.startsWith('Sem saída') ? <AlertTriangle size={15} className="warning" /> : <ShieldCheck size={15} className="technical" />}
          <span><strong>Decisão simulada:</strong> {result.exitReason}</span>
        </div>
      </div>
    </section>

    <section className="panel mb-4">
      <div className="border-b border-[hsl(var(--border))] px-4 py-3"><h2 className="m-0 text-sm font-semibold">Envio, reconciliação e RugCheck</h2><p className="mb-0 mt-1 text-xs muted">Cenários pós-entrada sem assinar nem enviar transações.</p></div>
      <div className="grid gap-4 p-4 md:grid-cols-2">
        <label className="block text-xs"><span className="muted">Resultado simulado da transação</span>
          <select className="select mt-1" value={input.transactionOutcome} onChange={event => set('transactionOutcome', event.target.value as LabInputs['transactionOutcome'])}>
            <option value="confirmed">Confirmada</option><option value="definitive-failure">Falha definitiva</option><option value="unknown">Desconhecida / sem confirmação</option>
          </select>
          <span className={`mt-2 block leading-5 ${input.transactionOutcome === 'unknown' ? 'warning' : 'muted'}`}>{transactionResult}</span>
        </label>
        <label className="flex items-start gap-3 rounded-md border border-[hsl(var(--border))] p-3 text-xs">
          <input className="mt-0.5" type="checkbox" checked={input.rugCheckFailed} onChange={event => set('rugCheckFailed', event.target.checked)} />
          <span><strong className="block">RugCheck falhou depois da entrada</strong><span className="mt-1 block leading-5 muted">{input.rugCheckFailed ? 'Resultado esperado: alertar; não forçar encerramento da posição.' : 'O screening pós-entrada é assíncrono e não bloqueia o sniping.'}</span></span>
        </label>
      </div>
    </section>

    <section className="panel">
      <div className="border-b border-[hsl(var(--border))] px-4 py-3"><h2 className="m-0 text-sm font-semibold">Limites e gaps que esta tela ajuda a inspecionar</h2></div>
      <div className="grid gap-3 p-4 md:grid-cols-2 xl:grid-cols-3">
        {[
          ['Idade e atraso', 'A idade é checada depois da espera; no limite exato, o token é rejeitado.'],
          ['Feed WebSocket', 'Market cap WS pode aprovar o gate sem validar reservas reais nessa etapa; o painel sinaliza isso.'],
          ['Reservas não são volume', 'O limite observa reservas reais SOL da curva, uma proxy de liquidez — não volume negociado acumulado.'],
          ['Cotação e slippage', 'No live, cotação vencida e impacto bloqueiam a preparação; slippage acima do limite também é rejeitada. Paper não modela execução de swap.'],
          ['Impacto e tamanho', 'O impacto cresce com a ordem em relação à reserva virtual e pode bloquear ordens grandes.'],
          ['Saída e confirmação', 'Max hold não encerra posição positiva; uma saída live precisa de confirmação, não só de estimativa.'],
        ].map(([title, text]) => <div key={title} className="rounded-md border border-[hsl(var(--border))] p-3"><div className="text-xs font-semibold">{title}</div><p className="mb-0 mt-1 text-[11px] leading-5 muted">{text}</p></div>)}
      </div>
    </section>
  </div>;
}
