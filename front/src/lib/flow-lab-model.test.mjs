import assert from 'node:assert/strict';
import test from 'node:test';
import { evaluateEntryFlow, evaluateExitFlow } from './flow-lab-model.mjs';

const defaults = {
  executionMode: 'live',
  websocketMarketCap: true,
  rpcAvailable: true,
  rpcLatencyMs: 350,
  legacyEntryRpcTimeoutMs: 2000,
  liveSnipeTimeoutMs: 15000,
  botAcceptingEntries: true,
  reconciliationRequired: false,
  solUsdPriceAvailable: true,
  activePositionExists: false,
  previousBuyExists: false,
  databaseAvailable: true,
  creatorBlocked: false,
  queueAvailable: true,
  realSolReserves: 0.7,
  minReserves: 0,
  maxReserves: 1.5,
  websocketMarketCapSol: 35,
  rpcMarketCapSol: 33,
  marketCapDifferenceAlertPct: 5,
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
  maxTradeSol: 0.035,
  priorityFeeSol: 0.001,
  maxPriorityFeeSol: 0.001,
  openPositions: 0,
  pendingBuys: 0,
  maxOpenPositions: 20,
  openExposureSol: 0,
  pendingExposureSol: 0,
  maxExposureSol: 0.5,
  dailyRealizedLossSol: 0,
  maxDailyLossSol: 0.15,
  availableBalanceSol: 1,
  feeReserveSol: 0.02,
  pendingReservedSol: 0,
  simulationBaseFeeSol: 0.000005,
};

test('fast WebSocket admission is independent of delayed RPC and live readiness', () => {
  const result = evaluateEntryFlow({
    ...defaults,
    rpcLatencyMs: 5000,
    realSolReserves: 0.2,
    minReserves: 0.3,
  });

  assert.equal(result.candidateAllowed, true);
  assert.equal(result.reserveCheck.status, 'warn');
  assert.equal(result.reserveCheck.blocksCandidate, false);
  assert.equal(result.executionReady, false);
  assert.equal(result.entryFlowReady, false);
});

test('WebSocket market cap remains the admission source when RPC disagrees', () => {
  const result = evaluateEntryFlow({
    ...defaults,
    websocketMarketCapSol: 35,
    rpcMarketCapSol: 18,
  });

  assert.equal(result.candidateAllowed, true);
  assert.match(result.admissionChecks.find(item => item.label === 'Market cap de entrada').detail, /35\.00 SOL via WebSocket/);
  assert.equal(result.sourceDifferenceCheck.status, 'warn');
  assert.equal(result.entryFlowReady, true);
});

test('out-of-range reserves are diagnostic in fast path and blocking in legacy RPC path', () => {
  const fast = evaluateEntryFlow({
    ...defaults,
    realSolReserves: 0.2,
    minReserves: 0.3,
  });
  assert.equal(fast.candidateAllowed, true);
  assert.equal(fast.reserveCheck.status, 'fail');
  assert.equal(fast.reserveCheck.blocksCandidate, false);

  const legacy = evaluateEntryFlow({
    ...defaults,
    websocketMarketCap: false,
    realSolReserves: 0.2,
    minReserves: 0.3,
  });
  assert.equal(legacy.candidateAllowed, false);
  assert.equal(legacy.reserveCheck.blocksCandidate, true);
});

test('legacy admission fails closed when its RPC snapshot is unavailable', () => {
  const result = evaluateEntryFlow({
    ...defaults,
    websocketMarketCap: false,
    rpcAvailable: false,
  });

  assert.equal(result.candidateAllowed, false);
  assert.equal(result.reserveCheck.status, 'fail');
  assert.equal(result.reserveCheck.blocksCandidate, true);
});

test('admission fails closed when persisted strategy state is unavailable', () => {
  const result = evaluateEntryFlow({
    ...defaults,
    databaseAvailable: false,
  });

  assert.equal(result.candidateAllowed, false);
  assert.match(
    result.admissionChecks.find(item => item.label === 'Duplicidade e posição anterior').detail,
    /Banco indisponível/,
  );
});

test('legacy RPC admission uses the earlier of the RPC and overall snipe timeouts', () => {
  const result = evaluateEntryFlow({
    ...defaults,
    websocketMarketCap: false,
    rpcLatencyMs: 1500,
    legacyEntryRpcTimeoutMs: 2000,
    liveSnipeTimeoutMs: 1000,
  });

  assert.equal(result.candidateAllowed, false);
  assert.match(
    result.admissionChecks.find(item => item.label === 'Market cap de entrada').detail,
    /timeout efetivo legado \(1000 ms\)/,
  );
});

test('stale quote, excess slippage, and high impact affect execution only', () => {
  for (const patch of [
    { quoteAgeMs: 2200 },
    { requestedSlippageBps: 2000 },
    { orderSol: 1.2, virtualSolReserves: 5 },
  ]) {
    const result = evaluateEntryFlow({ ...defaults, ...patch });
    assert.equal(result.candidateAllowed, true);
    assert.equal(result.executionReady, false);
    assert.equal(result.entryFlowReady, false);
  }
});

test('engine risk and resource limits block execution, not candidate admission', () => {
  const cases = [
    { label: 'Bot aceita novas entradas', patch: { botAcceptingEntries: false } },
    { label: 'Reconciliação de operações', patch: { reconciliationRequired: true } },
    { label: 'Preço SOL/USD para dimensionar a ordem', patch: { solUsdPriceAvailable: false } },
    { label: 'Limite por operação', patch: { maxTradeSol: 0.02 } },
    { label: 'Taxa prioritária', patch: { priorityFeeSol: 0.002 } },
    { label: 'Posições simultâneas', patch: { openPositions: 19, pendingBuys: 1 } },
    { label: 'Exposição total', patch: { openExposureSol: 0.48 } },
    { label: 'Perda diária realizada', patch: { dailyRealizedLossSol: 0.15 } },
    { label: 'Saldo, reserva de taxas e entradas pendentes', patch: { availableBalanceSol: 0.05 } },
    { label: 'Slippage solicitado', patch: { requestedSlippageBps: 1501 } },
  ];

  for (const { label, patch } of cases) {
    const result = evaluateEntryFlow({ ...defaults, ...patch });
    assert.equal(result.candidateAllowed, true, `${label} changed candidate admission`);
    assert.equal(result.executionReady, false, `${label} did not block live execution`);
    assert.equal(result.entryFlowReady, false, `${label} did not block the complete flow`);
    assert.equal(
      result.executionChecks.find(item => item.label === label).status,
      'fail',
      `${label} was not reported as a failed execution check`,
    );
  }
});

test('engine resource limits allow values exactly at inclusive caps', () => {
  const result = evaluateEntryFlow({
    ...defaults,
    orderSol: 0.035,
    maxTradeSol: 0.035,
    priorityFeeSol: 0.001,
    maxPriorityFeeSol: 0.001,
    openPositions: 19,
    maxOpenPositions: 20,
    openExposureSol: 0.465,
    maxExposureSol: 0.5,
    availableBalanceSol: 0.521,
  });

  assert.equal(result.entryFlowReady, true);
});

test('age boundary is strict and zero disables only the age filter', () => {
  const atBoundary = evaluateEntryFlow({
    ...defaults,
    ageAtDetection: 24.8,
    networkDelayMs: 200,
    maxTokenAge: 30,
  });
  assert.equal(atBoundary.candidateAllowed, false);

  const disabled = evaluateEntryFlow({
    ...defaults,
    ageAtDetection: 100,
    maxTokenAge: 0,
  });
  assert.equal(disabled.candidateAllowed, true);
});

test('inclusive reserve boundaries pass as a diagnostic without changing fast admission', () => {
  for (const realSolReserves of [0.3, 1.5]) {
    const result = evaluateEntryFlow({
      ...defaults,
      realSolReserves,
      minReserves: 0.3,
      maxReserves: 1.5,
    });
    assert.equal(result.reserveCheck.status, 'pass');
    assert.equal(result.candidateAllowed, true);
  }
});

test('paper mode reports live execution as not applicable', () => {
  const result = evaluateEntryFlow({
    ...defaults,
    executionMode: 'paper',
    rpcAvailable: false,
    quoteAgeMs: 5000,
  });
  assert.equal(result.candidateAllowed, true);
  assert.equal(result.executionReady, null);
  assert.equal(result.entryFlowReady, true);
});

test('paper mode still enforces common engine slippage limits', () => {
  const result = evaluateEntryFlow({
    ...defaults,
    executionMode: 'paper',
    requestedSlippageBps: 2000,
  });

  assert.equal(result.candidateAllowed, true);
  assert.equal(result.executionReady, null);
  assert.equal(result.entryFlowReady, false);
  assert.equal(result.executionChecks.find(item => item.label === 'Slippage solicitado').status, 'fail');
});

const exitDefaults = {
  positionCostSol: 1,
  currentPositionSol: 1,
  peakPositionSol: 1,
  heldSeconds: 10,
  maxHoldSeconds: 300,
  exitMarkFresh: true,
  exitLiquidityVerified: true,
  takeProfitPct: 50,
  stopLossPct: 25,
  trailingStopPct: 12,
  trailingArmPct: 10,
  exitTriggerBufferPct: 0.5,
};

test('exit simulation uses the runtime trigger buffer without changing stop order', () => {
  const stop = evaluateExitFlow({ ...exitDefaults, currentPositionSol: 0.755 });
  assert.match(stop, /^Stop-loss/);

  const trailing = evaluateExitFlow({
    ...exitDefaults,
    positionCostSol: 0.9,
    currentPositionSol: 0.884,
    peakPositionSol: 1,
  });
  assert.match(trailing, /^Trailing stop/);

  const takeProfit = evaluateExitFlow({
    ...exitDefaults,
    currentPositionSol: 1.496,
    peakPositionSol: 1.496,
  });
  assert.match(takeProfit, /^Take-profit/);
});

test('max hold zero disables the timer; positive positions remain open after it', () => {
  const disabled = evaluateExitFlow({
    ...exitDefaults,
    currentPositionSol: 0.9,
    heldSeconds: 500,
    maxHoldSeconds: 0,
  });
  assert.match(disabled, /^Manter posição/);

  const positive = evaluateExitFlow({
    ...exitDefaults,
    currentPositionSol: 1.1,
    peakPositionSol: 1.1,
    heldSeconds: 300,
  });
  assert.match(positive, /posição positiva permanece aberta/);
});

test('max hold exits flat positions only with an enabled timer and a fresh mark', () => {
  const flatAtMaxHold = evaluateExitFlow({
    ...exitDefaults,
    heldSeconds: 300,
    currentPositionSol: 1,
  });
  assert.match(flatAtMaxHold, /^Max hold/);

  const staleStopLoss = evaluateExitFlow({
    ...exitDefaults,
    currentPositionSol: 0.7,
    exitMarkFresh: false,
  });
  assert.match(staleStopLoss, /posição permanece aberta/);
});

test('stop-loss takes precedence over a trailing-stop signal', () => {
  const result = evaluateExitFlow({
    ...exitDefaults,
    currentPositionSol: 0.8,
    peakPositionSol: 1.2,
    trailingStopPct: 10,
    stopLossPct: 20,
  });

  assert.match(result, /^Stop-loss/);
});

test('invalid exit trigger buffer is reported instead of silently disabled', () => {
  const result = evaluateExitFlow({
    ...exitDefaults,
    exitTriggerBufferPct: Number.NaN,
  });

  assert.match(result, /parâmetros ou marca.*inválidos/);
});
