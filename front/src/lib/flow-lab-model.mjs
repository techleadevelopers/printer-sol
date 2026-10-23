const check = (label, status, detail) => ({ label, status, detail });
const finite = (...values) => values.every(Number.isFinite);

export function evaluateEntryFlow(input) {
  const elapsedAge = input.ageAtDetection + input.entryDelay + input.networkDelayMs / 1000;
  // Runtime applies the fixed legacy RPC read timeout inside the overall
  // snipe context, so the effective deadline is whichever expires first.
  const effectiveLegacyEntryTimeoutMs = Math.min(
    input.legacyEntryRpcTimeoutMs,
    input.liveSnipeTimeoutMs,
  );
  const rpcResponsiveForEntry = input.rpcAvailable
    && input.rpcLatencyMs <= effectiveLegacyEntryTimeoutMs;
  const rpcResponsiveForLive = input.rpcAvailable
    && input.rpcLatencyMs <= input.liveSnipeTimeoutMs;
  const marketCapSource = input.websocketMarketCap ? 'WebSocket' : 'RPC';
  const effectiveMarketCap = input.websocketMarketCap
    ? input.websocketMarketCapSol
    : input.rpcMarketCapSol;
  const validTiming = finite(
    input.ageAtDetection,
    input.entryDelay,
    input.networkDelayMs,
    input.maxTokenAge,
    input.rpcLatencyMs,
    input.legacyEntryRpcTimeoutMs,
    input.liveSnipeTimeoutMs,
  ) && input.ageAtDetection >= 0 && input.entryDelay >= 0
    && input.networkDelayMs >= 0 && input.maxTokenAge >= 0
    && input.rpcLatencyMs >= 0 && input.legacyEntryRpcTimeoutMs >= 0
    && input.liveSnipeTimeoutMs >= 0 && Number.isFinite(elapsedAge);
  const validMarketCapLimits = finite(input.minMarketCap, input.maxMarketCap)
    && input.minMarketCap >= 0 && input.maxMarketCap >= 0
    && (input.maxMarketCap === 0 || input.maxMarketCap >= input.minMarketCap);
  const marketCapPass = validMarketCapLimits
    && Number.isFinite(effectiveMarketCap)
    && effectiveMarketCap > 0
    && effectiveMarketCap >= input.minMarketCap
    && (input.maxMarketCap === 0 || effectiveMarketCap <= input.maxMarketCap);
  const validReserveLimits = finite(input.minReserves, input.maxReserves)
    && input.minReserves >= 0 && input.maxReserves >= input.minReserves;
  const reserveValueAvailable = rpcResponsiveForEntry
    && Number.isFinite(input.realSolReserves) && input.realSolReserves >= 0;
  const reservesPass = reserveValueAvailable && validReserveLimits
    && input.realSolReserves >= input.minReserves
    && input.realSolReserves <= input.maxReserves;
  const reserveBlocksCandidate = !input.websocketMarketCap;

  const admissionChecks = [
    check(
      'Parâmetros de admissão',
      validTiming && validMarketCapLimits ? 'pass' : 'fail',
      !validTiming
        ? 'Idades e latências precisam ser finitas e não negativas.'
        : !validMarketCapLimits
          ? 'Os limites de market cap precisam ser finitos e não negativos.'
          : 'Parâmetros válidos; regras de execução são avaliadas em separado.',
    ),
    check(
      'Idade do token após espera',
      !validTiming || (input.maxTokenAge > 0 && elapsedAge >= input.maxTokenAge) ? 'fail' : 'pass',
      `${Number.isFinite(elapsedAge) ? `${elapsedAge.toFixed(2)} s` : 'inválida'} no gate`
        + (input.maxTokenAge > 0
          ? ` · limite estrito < ${input.maxTokenAge} s`
          : ' · filtro desativado (limite 0)'),
    ),
    check(
      'Duplicidade e posição anterior',
      input.activePositionExists || input.previousBuyExists ? 'fail' : 'pass',
      input.activePositionExists
        ? 'Já existe posição não reconciliada para este mint.'
        : input.previousBuyExists
          ? 'Já existe tentativa/compra persistida; nova compra bloqueada.'
          : 'Sem posição ativa ou compra persistida para o mint.',
    ),
    check(
      'Blacklist do criador',
      input.creatorBlocked ? 'fail' : 'pass',
      input.creatorBlocked
        ? 'Criador bloqueado após stop-loss; entrada rejeitada.'
        : 'Criador não está bloqueado neste cenário.',
    ),
    check(
      'Deduplicação e capacidade da fila',
      input.queueAvailable ? 'pass' : 'fail',
      input.queueAvailable
        ? 'Mint não está pendente e há capacidade para enfileirar.'
        : 'Duplicado dentro do TTL ou fila cheia; entrada rejeitada.',
    ),
    check(
      'Market cap de entrada',
      (!input.websocketMarketCap && !rpcResponsiveForEntry) || !marketCapPass ? 'fail' : 'pass',
      !input.websocketMarketCap && !rpcResponsiveForEntry
        ? `RPC indisponível ou acima do timeout efetivo legado (${effectiveLegacyEntryTimeoutMs} ms).`
        : `${Number.isFinite(effectiveMarketCap) ? effectiveMarketCap.toFixed(2) : 'inválido'} SOL via ${marketCapSource}`
          + ` · mínimo ${input.minMarketCap} SOL`
          + (input.maxMarketCap > 0 ? ` · máximo ${input.maxMarketCap} SOL` : ' · teto desativado'),
    ),
  ];

  const reserveStatus = !reserveValueAvailable || !validReserveLimits
    ? (input.websocketMarketCap ? 'warn' : 'fail')
    : reservesPass ? 'pass' : 'fail';
  const reserveCheck = {
    ...check(
    'Faixa-alvo de reservas reais da bonding curve',
    reserveStatus,
    !validReserveLimits
      ? 'Faixa de reserva inválida; não é possível avaliar conformidade.'
      : !reserveValueAvailable
        ? input.websocketMarketCap
          ? 'RPC atrasado/indisponível: reserva não confirmada; não bloqueia a admissão WebSocket.'
          : `RPC indisponível ou acima do timeout efetivo legado (${effectiveLegacyEntryTimeoutMs} ms); gate RPC bloqueia a admissão.`
        : reservesPass
          ? `${input.realSolReserves.toFixed(3)} SOL dentro da faixa inclusiva ${input.minReserves.toFixed(3)}–${input.maxReserves.toFixed(3)} SOL.`
          : `${input.realSolReserves.toFixed(3)} SOL fora da faixa ${input.minReserves.toFixed(3)}–${input.maxReserves.toFixed(3)} SOL.`
            + (input.websocketMarketCap ? ' Diagnóstico separado; não bloqueia o fast path atual.' : ' O gate RPC legado bloqueia a admissão.'),
    ),
    blocksCandidate: reserveBlocksCandidate,
  };
  const marketCapDifferencePct = Number.isFinite(input.websocketMarketCapSol)
    && Number.isFinite(input.rpcMarketCapSol) && input.rpcMarketCapSol > 0
    ? Math.abs(input.websocketMarketCapSol - input.rpcMarketCapSol) / input.rpcMarketCapSol * 100
    : Number.POSITIVE_INFINITY;
  const sourceDifferenceCheck = check(
    'Diferença de market cap WS/RPC · diagnóstico',
    Number.isFinite(marketCapDifferencePct)
      && Number.isFinite(input.marketCapDifferenceAlertPct)
      && marketCapDifferencePct <= input.marketCapDifferenceAlertPct ? 'pass' : 'warn',
    Number.isFinite(marketCapDifferencePct)
      ? `${input.websocketMarketCapSol.toFixed(2)} SOL WS vs ${input.rpcMarketCapSol.toFixed(2)} SOL RPC`
        + ` · diferença ${marketCapDifferencePct.toFixed(1)}%`
        + ` · alerta acima de ${input.marketCapDifferenceAlertPct}% · não altera a fonte de admissão`
      : 'Uma fonte está indisponível/inválida; diagnóstico não bloqueia o candidato.',
  );

  const validExecutionConfig = finite(
    input.orderSol,
    input.virtualSolReserves,
    input.virtualTokenReserves,
    input.quoteAgeMs,
    input.maxQuoteAgeMs,
    input.requestedSlippageBps,
    input.maxSlippageBps,
    input.maxCurveImpactBps,
  ) && input.orderSol > 0 && input.virtualSolReserves > 0
    && input.virtualTokenReserves > 0 && input.quoteAgeMs >= 0
    && input.maxQuoteAgeMs >= 1 && input.maxQuoteAgeMs <= 120_000
    && input.requestedSlippageBps >= 0
    && input.maxSlippageBps >= 1 && input.maxSlippageBps <= 10_000
    && input.maxCurveImpactBps >= 1 && input.maxCurveImpactBps <= 10_000;
  const validQuoteInputs = finite(input.orderSol, input.virtualSolReserves, input.virtualTokenReserves)
    && input.orderSol > 0 && input.virtualSolReserves > 0 && input.virtualTokenReserves > 0;
  const expectedTokens = validQuoteInputs
    ? input.virtualTokenReserves * input.orderSol / (input.virtualSolReserves + input.orderSol)
    : 0;
  const impactBps = validQuoteInputs
    ? input.orderSol / (input.virtualSolReserves + input.orderSol) * 10_000
    : Number.POSITIVE_INFINITY;
  const minimumTokens = expectedTokens * (
    1 - Math.min(input.requestedSlippageBps, input.maxSlippageBps) / 10_000
  );
  const paper = input.executionMode === 'paper';
  const executionChecks = [
    check(
      'Parâmetros da ordem e limites',
      paper ? 'warn' : validExecutionConfig ? 'pass' : 'fail',
      paper
        ? 'Informativo no paper; configuração live não é executada neste modo.'
        : validExecutionConfig
          ? 'Tamanho, reservas virtuais e limites de execução válidos.'
          : 'Ordem, reservas virtuais, quote age, slippage e impacto precisam respeitar limites válidos.',
    ),
    check(
      'Cotação RPC fresca',
      paper ? 'warn' : !rpcResponsiveForLive || input.quoteAgeMs > input.maxQuoteAgeMs ? 'fail' : 'pass',
      paper
        ? 'O fluxo paper não prepara swap live.'
        : !rpcResponsiveForLive
          ? `RPC indisponível ou acima do timeout live (${input.liveSnipeTimeoutMs} ms).`
          : !Number.isFinite(input.quoteAgeMs) || input.quoteAgeMs > input.maxQuoteAgeMs
            ? `Cotação vencida: ${input.quoteAgeMs} ms · máximo ${input.maxQuoteAgeMs} ms.`
            : `Cotação com ${input.quoteAgeMs} ms · máximo ${input.maxQuoteAgeMs} ms.`,
    ),
    check(
      'Slippage solicitado',
      paper ? 'warn' : input.requestedSlippageBps <= input.maxSlippageBps ? 'pass' : 'fail',
      paper
        ? `${input.requestedSlippageBps} bps · não há envio de swap no paper.`
        : `${input.requestedSlippageBps} bps (${(input.requestedSlippageBps / 100).toFixed(2)}%) · máximo ${input.maxSlippageBps} bps.`,
    ),
    check(
      'Impacto estimado da curva',
      paper ? 'warn' : impactBps <= input.maxCurveImpactBps ? 'pass' : 'fail',
      paper
        ? `${Number.isFinite(impactBps) ? `${impactBps.toFixed(1)} bps` : 'inválido'} · informativo no paper.`
        : `${Number.isFinite(impactBps) ? impactBps.toFixed(1) : 'inválido'} bps · máximo ${input.maxCurveImpactBps} bps.`,
    ),
  ];

  const candidateAllowed = admissionChecks.every(item => item.status !== 'fail')
    && !reserveCheck.blocksCandidate;
  const executionReady = paper
    ? null
    : executionChecks.every(item => item.status !== 'fail');

  return {
    admissionChecks,
    reserveCheck,
    sourceDifferenceCheck,
    executionChecks,
    candidateAllowed,
    executionReady,
    entryFlowReady: candidateAllowed && (paper || executionReady === true),
    impactBps,
    expectedTokens,
    minimumTokens,
    reservesPass,
    marketCapDifferencePct,
  };
}
