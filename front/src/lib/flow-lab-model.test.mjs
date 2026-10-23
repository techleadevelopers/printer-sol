import assert from 'node:assert/strict';
import test from 'node:test';
import { evaluateEntryFlow } from './flow-lab-model.mjs';

const defaults = {
  executionMode: 'live',
  websocketMarketCap: true,
  rpcAvailable: true,
  rpcLatencyMs: 350,
  legacyEntryRpcTimeoutMs: 2000,
  liveSnipeTimeoutMs: 15000,
  activePositionExists: false,
  previousBuyExists: false,
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
