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
      !input.databaseAvailable || input.activePositionExists || input.previousBuyExists ? 'fail' : 'pass',
      !input.databaseAvailable
        ? 'Banco indisponível; a estratégia falha fechada ao validar posição, compra anterior e blacklist.'
        : input.activePositionExists
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
    'Diferença de market cap WS/RPC · diagnóstico local',
    Number.isFinite(marketCapDifferencePct)
      && Number.isFinite(input.marketCapDifferenceAlertPct)
      && marketCapDifferencePct <= input.marketCapDifferenceAlertPct ? 'pass' : 'warn',
    Number.isFinite(marketCapDifferencePct)
      ? `${input.websocketMarketCapSol.toFixed(2)} SOL WS vs ${input.rpcMarketCapSol.toFixed(2)} SOL RPC`
        + ` · diferença ${marketCapDifferencePct.toFixed(1)}%`
        + ` · aviso visual acima de ${input.marketCapDifferenceAlertPct}% · não altera a fonte de admissão`
      : 'Uma fonte está indisponível/inválida; aviso local não bloqueia o candidato.',
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
  const validSlippageConfig = finite(input.requestedSlippageBps, input.maxSlippageBps)
    && input.requestedSlippageBps >= 0
    && input.maxSlippageBps >= 1 && input.maxSlippageBps <= 10_000;
  const validEngineLimits = finite(
    input.maxTradeSol,
    input.priorityFeeSol,
    input.maxPriorityFeeSol,
    input.openPositions,
    input.pendingBuys,
    input.maxOpenPositions,
    input.openExposureSol,
    input.pendingExposureSol,
    input.maxExposureSol,
    input.dailyRealizedLossSol,
    input.maxDailyLossSol,
    input.availableBalanceSol,
    input.feeReserveSol,
    input.pendingReservedSol,
    input.simulationBaseFeeSol,
  ) && input.maxTradeSol >= 0
    && input.priorityFeeSol >= 0 && input.maxPriorityFeeSol >= 0
    && Number.isInteger(input.openPositions) && input.openPositions >= 0
    && Number.isInteger(input.pendingBuys) && input.pendingBuys >= 0
    && Number.isInteger(input.maxOpenPositions) && input.maxOpenPositions >= 1
    && input.openExposureSol >= 0 && input.pendingExposureSol >= 0
    && input.maxExposureSol >= 0 && input.dailyRealizedLossSol >= 0
    && input.maxDailyLossSol >= 0 && input.availableBalanceSol >= 0
    && input.feeReserveSol >= 0 && input.pendingReservedSol >= 0
    && input.simulationBaseFeeSol >= 0;
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
  const balanceRequired = input.orderSol + input.priorityFeeSol + input.feeReserveSol
    + input.pendingReservedSol + (paper ? input.simulationBaseFeeSol : input.openExposureSol);
  const commonExecutionChecks = [
    check(
      'Bot aceita novas entradas',
      input.botAcceptingEntries ? 'pass' : 'fail',
      input.botAcceptingEntries
        ? 'O estado do bot permite novas compras.'
        : 'Bot parado, pausado ou em parada de emergência; o motor recusa compras.',
    ),
    check(
      'Reconciliação de operações',
      input.reconciliationRequired ? 'fail' : 'pass',
      input.reconciliationRequired
        ? 'Há operação com resultado desconhecido; compras ficam bloqueadas até reconciliar.'
        : 'Nenhuma reconciliação pendente bloqueia compras.',
    ),
    check(
      'Preço SOL/USD para dimensionar a ordem',
      input.solUsdPriceAvailable ? 'pass' : 'fail',
      input.solUsdPriceAvailable
        ? 'Existe cotação SOL/USD utilizável para converter TRADE_USD em SOL.'
        : 'Sem cotação SOL/USD atual; o motor não consegue dimensionar a compra.',
    ),
    check(
      'Limite por operação',
      validEngineLimits && input.orderSol > 0
        && (input.maxTradeSol === 0 || input.orderSol <= input.maxTradeSol) ? 'pass' : 'fail',
      !validEngineLimits
        ? 'Parâmetros financeiros inválidos.'
        : input.maxTradeSol === 0
          ? 'Limite por operação desativado.'
          : `${input.orderSol.toFixed(4)} SOL · máximo ${input.maxTradeSol.toFixed(4)} SOL.`,
    ),
    check(
      'Taxa prioritária',
      validEngineLimits
        && (input.maxPriorityFeeSol === 0 || input.priorityFeeSol <= input.maxPriorityFeeSol) ? 'pass' : 'fail',
      !validEngineLimits
        ? 'Parâmetros de taxa inválidos.'
        : input.maxPriorityFeeSol === 0
          ? 'Limite de taxa prioritária desativado.'
          : `${input.priorityFeeSol.toFixed(6)} SOL · máximo ${input.maxPriorityFeeSol.toFixed(6)} SOL.`,
    ),
    check(
      'Posições simultâneas',
      validEngineLimits && input.openPositions + input.pendingBuys < input.maxOpenPositions ? 'pass' : 'fail',
      validEngineLimits
        ? `${input.openPositions} abertas + ${input.pendingBuys} pendentes · máximo ${input.maxOpenPositions}.`
        : 'Contagens ou limite de posições inválidos.',
    ),
    check(
      'Exposição total',
      validEngineLimits
        && (input.maxExposureSol === 0
          || input.openExposureSol + input.pendingExposureSol + input.orderSol <= input.maxExposureSol) ? 'pass' : 'fail',
      !validEngineLimits
        ? 'Valores de exposição inválidos.'
        : input.maxExposureSol === 0
          ? 'Limite de exposição desativado.'
          : `${(input.openExposureSol + input.pendingExposureSol + input.orderSol).toFixed(4)} SOL após a ordem · máximo ${input.maxExposureSol.toFixed(4)} SOL.`,
    ),
    check(
      'Perda diária realizada',
      validEngineLimits
        && (input.maxDailyLossSol === 0 || input.dailyRealizedLossSol < input.maxDailyLossSol) ? 'pass' : 'fail',
      !validEngineLimits
        ? 'Valores de perda diária inválidos.'
        : input.maxDailyLossSol === 0
          ? 'Limite de perda diária desativado.'
          : `${input.dailyRealizedLossSol.toFixed(4)} SOL · bloqueio em ${input.maxDailyLossSol.toFixed(4)} SOL.`,
    ),
    check(
      'Saldo, reserva de taxas e entradas pendentes',
      validEngineLimits && input.availableBalanceSol >= balanceRequired ? 'pass' : 'fail',
      validEngineLimits
        ? `${input.availableBalanceSol.toFixed(4)} SOL disponíveis · ${balanceRequired.toFixed(4)} SOL necessários${paper ? ' incluindo taxa-base paper' : ' incluindo exposição já aberta'}.`
        : 'Saldo, reserva ou taxa-base inválidos.',
    ),
    check(
      'Slippage solicitado',
      validSlippageConfig && input.requestedSlippageBps <= input.maxSlippageBps ? 'pass' : 'fail',
      validSlippageConfig
        ? `${input.requestedSlippageBps} bps (${(input.requestedSlippageBps / 100).toFixed(2)}%) · máximo ${input.maxSlippageBps} bps.`
        : 'O slippage solicitado ou o limite configurado é inválido.',
    ),
  ];
  const liveExecutionChecks = [
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
      'Parâmetros de cotação e impacto live',
      paper ? 'warn' : validExecutionConfig ? 'pass' : 'fail',
      paper
        ? 'Cotação, reservas virtuais e impacto do swap não são preparados no paper.'
        : validExecutionConfig
          ? 'Ordem, reservas virtuais, idade da cotação e limites live são válidos.'
          : 'Ordem, reservas virtuais, idade da cotação e limites live precisam ser válidos.',
    ),
    check(
      'Impacto estimado da curva',
      paper ? 'warn' : impactBps <= input.maxCurveImpactBps ? 'pass' : 'fail',
      paper
        ? `${Number.isFinite(impactBps) ? `${impactBps.toFixed(1)} bps` : 'inválido'} · informativo no paper.`
        : `${Number.isFinite(impactBps) ? impactBps.toFixed(1) : 'inválido'} bps · máximo ${input.maxCurveImpactBps} bps.`,
    ),
  ];

  const commonExecutionReady = commonExecutionChecks.every(item => item.status !== 'fail');
  const liveExecutionReady = liveExecutionChecks.every(item => item.status !== 'fail');
  const candidateAllowed = admissionChecks.every(item => item.status !== 'fail')
    && !reserveCheck.blocksCandidate;
  const executionReady = paper ? null : commonExecutionReady && liveExecutionReady;

  return {
    admissionChecks,
    reserveCheck,
    sourceDifferenceCheck,
    executionChecks: [...commonExecutionChecks, ...liveExecutionChecks],
    candidateAllowed,
    executionReady,
    entryFlowReady: candidateAllowed && commonExecutionReady && (paper || liveExecutionReady),
    impactBps,
    expectedTokens,
    minimumTokens,
    reservesPass,
    marketCapDifferencePct,
  };
}

export function evaluateExitFlow(input) {
  if (!input.exitMarkFresh) {
    return 'Sem saída: marca ausente ou desatualizada; posição permanece aberta.';
  }
  if (!input.exitLiquidityVerified) {
    return 'Sem saída: liquidez não verificada.';
  }
  if (!Number.isFinite(input.positionCostSol) || input.positionCostSol <= 0) {
    return 'Sem saída: custo pendente de reconciliação.';
  }
  const exitNumbers = [
    input.currentPositionSol,
    input.peakPositionSol,
    input.heldSeconds,
    input.maxHoldSeconds,
    input.takeProfitPct,
    input.stopLossPct,
    input.trailingStopPct,
    input.trailingArmPct,
    input.exitTriggerBufferPct,
  ];
  if (!finite(...exitNumbers) || exitNumbers.some(value => value < 0)
    || input.currentPositionSol === 0 || input.peakPositionSol === 0
    || input.exitTriggerBufferPct > 100) {
    return 'Sem saída: parâmetros ou marca de posição inválidos.';
  }

  const positionChange = (input.currentPositionSol - input.positionCostSol)
    / input.positionCostSol * 100;
  const peakProfitPct = (input.peakPositionSol - input.positionCostSol)
    / input.positionCostSol * 100;
  const drawdownFromPeak = input.peakPositionSol > 0
    ? (input.peakPositionSol - input.currentPositionSol) / input.peakPositionSol * 100
    : 0;
  const buffer = input.exitTriggerBufferPct;

  if (input.stopLossPct > 0 && positionChange <= -input.stopLossPct + buffer) {
    return 'Stop-loss: prioridade sobre os demais gatilhos.';
  }
  if (input.trailingStopPct > 0 && input.peakPositionSol > input.positionCostSol
    && input.currentPositionSol > 0 && peakProfitPct >= input.trailingArmPct
    && drawdownFromPeak >= input.trailingStopPct - buffer) {
    return 'Trailing stop: armado pelo pico e retração atingida.';
  }
  if (input.takeProfitPct > 0 && positionChange >= input.takeProfitPct - buffer) {
    return 'Take-profit: valorização atingiu o limite.';
  }
  if (input.maxHoldSeconds > 0 && input.heldSeconds >= input.maxHoldSeconds) {
    if (positionChange <= 0) {
      return 'Max hold: posição no zero ou negativa.';
    }
    return 'Max hold atingido, mas posição positiva permanece aberta.';
  }
  return 'Manter posição: nenhum limite de saída acionado.';
}
