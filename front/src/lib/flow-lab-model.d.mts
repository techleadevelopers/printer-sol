export type GateStatus = 'pass' | 'fail' | 'warn';
export type Check = { label: string; status: GateStatus; detail: string };

export type EntryFlowInput = {
  executionMode: 'paper' | 'live';
  websocketMarketCap: boolean;
  rpcAvailable: boolean;
  rpcLatencyMs: number;
  legacyEntryRpcTimeoutMs: number;
  liveSnipeTimeoutMs: number;
  activePositionExists: boolean;
  previousBuyExists: boolean;
  creatorBlocked: boolean;
  queueAvailable: boolean;
  realSolReserves: number;
  minReserves: number;
  maxReserves: number;
  websocketMarketCapSol: number;
  rpcMarketCapSol: number;
  marketCapDifferenceAlertPct: number;
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
};

export type EntryFlowResult = {
  admissionChecks: Check[];
  reserveCheck: Check & { blocksCandidate: boolean };
  sourceDifferenceCheck: Check;
  executionChecks: Check[];
  candidateAllowed: boolean;
  executionReady: boolean | null;
  entryFlowReady: boolean;
  impactBps: number;
  expectedTokens: number;
  minimumTokens: number;
  reservesPass: boolean;
  marketCapDifferencePct: number;
};

export function evaluateEntryFlow(input: EntryFlowInput): EntryFlowResult;
