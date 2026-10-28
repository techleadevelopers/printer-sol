### Impressor Solana Bot — Impressor de PnL

O Impressor Solana Bot é um backend em Go para execução de estratégias de trading de novos tokens na Solana (Pump.fun / PumpSwap), com foco em geração de PnL real através de regras explícitas de entrada, saída e gestão de risco.

A estratégia padrão é conservadora e observável: o bot só entra quando os filtros de mercado e momentum são satisfeitos, e sai por take-profit, trailing stop, stop-loss ou max-hold. O objetivo é capturar movimentos reais de preço com risco controlado — não é um "sniper cego" que compra todo mint.

```
**Aviso:** O bot não promete lucro. Preço, taxas, slippage, liquidez e execução podem causar perdas. Toda estratégia é calibrável e deve ser validada em simulação antes de qualquer capital real.
```

### Estratégia de geração de PnL

O bot foi desenhado para operar com regras de saída claras e mensuráveis, permitindo avaliar o PnL realizado por perfil, por faixa de score e por motivo de saída:

```
- Take-profit bruto: 50% (configurável).
- Stop-loss: 25% (configurável).
- Trailing stop: 12%, armado após ganho de 10% sobre o custo.
- Max-hold: `MAX_HOLD_SECONDS=300` (5 minutos) — fecha posições flat se não houver marca válida ou se o retorno ficar entre -10% e +5%.
- Saída por quarentena: apenas para saídas travadas (`STUCK_EXIT_QUARANTINE_SECONDS=120s`), nunca em posições saudáveis.
```

O **P&L realizado** só inclui vendas concluídas. O **P&L aberto** é uma estimativa baseada na variação do market cap (não é valor garantido de venda). O relatório de desempenho separa **P&L live fechado** (deltas confirmados da carteira e taxas registradas) de **P&L estimado em aberto**.

### Perfis de entrada (A e B) e geração de PnL

O bot escolhe **um perfil de entrada por candidato** e agrega os resultados separadamente (`PerformanceByProfile`), para comparar estratégias sem misturar métricas:


- Perfil A — primeiro pump entra nos primeiros segundos de vida do token. Foco em capturar o primeiro movimento. Não exige momentum nem holders.
- Perfil B — momentum validado: entra quando o token já demonstrou atividade (volume, buy ratio, unique buyers, aceleração). Foco em continuidade de movimento com confirmação.

Cada perfil tem seu próprio **score 0–100** (volume, buy ratio, unique buyers, aceleração). O score **não bloqueia compras por padrão** (`MIN_ENTRY_SCORE_*=0`), mas é gravado em `Trade.EntryScore` e `risk_assessments.entry_score`, permitindo analisar `PerformanceByScoreBucket` (0–20, 20–40, 40–60, 60–80, 80–100) e só depois calibrar limites mínimos.

### Critérios de prontidão para live (PnL comprovado em paper)

Para ativar execução real, o bot exige **prova estatística em simulação** (`LIVE_UNLOCK_REQUIRES_PAPER=true`):
```
- Pelo menos 7 dias de histórico e 100 operações fechadas.
- P&L líquido e expectativa positivos.
- Win rate ≥ 45%.
- Profit factor ≥ 1,2.
- Drawdown máximo ≤ metade da banca inicial.
- Pelo menos 10 saídas por motivo: take-profit, trailing, stop-loss e max-hold.
- Reconciliação limpa.
- Retenção média ≤ 180 segundos.
- Taxas ≤ 15% do P&L bruto**.
- Mais de 30% das saídas por trailing stop.
```

O teste com 29 operações permanece bloqueado. Ou seja: o bot só vai para live quando o PnL simulado provar que a estratégia funciona.

## Segurança e modo de execução
```
O padrão é `SIMULATION=true`. Neste modo o serviço registra a negociação simulada, mas não assina nem envia bytes de transação. O perfil de exemplo usa ordem de US$ 3, prioridade por transação de 0,001 SOL, taxa-base simulada de 0,000005 SOL, take-profit bruto de 50%, stop-loss de 25%, trailing stop de 12% armado após ganho de 10% sobre o custo e limite de retenção `MAX_HOLD_SECONDS=300` (configurável). RugCheck roda de forma assíncrona depois da entrada e não bloqueia o sniping. São parâmetros de operação, não promessa de lucro: preço, taxas, slippage, liquidez e execução podem causar perdas.

A simulação calcula saídas virtuais a partir das atualizações de preço por token do PumpPortal. Para isso, `PUMPPORTAL_API_KEY` também é obrigatória em simulação; o PumpPortal exige uma carteira vinculada com pelo menos 0,02 SOL para essa assinatura. A cotação SOL/USD validada é reutilizada por até 30 segundos; se expirar e a atualização falhar, novas entradas são bloqueadas. A banca exibida como saldo disponível desconta o capital alocado em posições abertas e incorpora resultados realizados após cada saída.

Novos mints aguardam `ENTRY_DELAY_SECONDS` antes do gate de entrada. Com `USE_WS_MARKET_CAP=true` (padrão), o market cap do PumpPortal é usado no gate de market cap e como baseline de entrada. `MIN_MARKET_CAP_SOL=0` significa sem piso adicional (o market cap ainda precisa ser positivo e válido); `MAX_MARKET_CAP_SOL=4` é um teto inclusivo, e `0` desativa somente o teto. Ambos são validados na admissão e novamente imediatamente antes da compra, usando os valores efetivos carregados do ambiente. Esse fast path não espera um snapshot RPC de reservas para admitir o candidato e não rejeita a admissão por atraso ou divergência dessa leitura. `MIN_VOLUME_BEFORE_BUY` e `MAX_VOLUME_BEFORE_BUY` continuam representando a faixa inclusiva de reservas reais de SOL da bonding curve (padrão 0–1,5 SOL), uma proxy de liquidez não volume negociado acumulado e são gate de admissão no modo legado. Com `USE_WS_MARKET_CAP=false`, market cap e reservas dependem do snapshot RPC; falha/timeout ou reserva fora da faixa bloqueiam a avaliação. A execução live permanece separada da admissão e exige cotação RPC fresca, slippage e impacto dentro dos limites configurados. 

As marcas do WebSocket são a fonte principal e processadas em segundo plano com fila coalescida por token; `PRICE_POLL_SECONDS` atualiza via RPC apenas marcas antigas no modo WebSocket e todas as marcas no modo legado. A estimativa de P&L aberto deriva da marca atual; P&L realizado só inclui vendas concluídas. As respostas da API usam `Cache-Control: no-store`, e o painel consulta os agregados a cada cinco segundos. `MAX_HOLD_SECONDS` é configurável e, com uma marca verificada, fecha posições exatamente flat, com retorno de pelo menos +5% ou de até -10%; o intervalo estrito entre -10% e +5% permanece aberto. 

Sem marca válida, a posição permanece aberta e gera alerta. A recuperação por quarentena é separada do max hold e atua somente sobre saídas travadas após `STUCK_EXIT_QUARANTINE_SECONDS` (120s por padrão), nunca em posições saudáveis. `TOKEN_MAX_AGE_SECONDS` tem padrão de 30 segundos, respeita o valor configurado sem tolerância adicional e `0` desativa apenas o corte de idade; eventos sem timestamp confiável continuam bloqueados. A configuração é passada tanto à estratégia quanto ao monitor.

`MAX_SLIPPAGE_BPS`, `MAX_QUOTE_AGE_MS` e `MAX_CURVE_IMPACT_BPS` são limites independentes e obrigatórios, calibrados com resultados reais. A confirmação live persiste o valor, a quantidade de tokens e a taxa observados no recibo on-chain. A venda usa o saldo confirmado da carteira e só é assinada se a instrução Pump.fun oferecer e aplicar o mínimo SOL calculado. Esses limites não eliminam atraso de rede, prioridade insuficiente nem competição com outras ordens; slippage só garante o mínimo quando o próprio programa de swap aplica essa proteção.
```
### Perfis de entrada (A e B)

O bot escolhe **um perfil de entrada** por candidato, com critérios independentes. O modo é configurado por `ENTRY_PROFILE_MODE`:

- `auto` (padrão): token jovem → Perfil A (primeiro pump); token velho → Perfil B (momentum).
- `first_pump_only`: força Perfil A.
- `momentum_only`: força Perfil B.
- `both`: tenta A primeiro; se A falhar, tenta B.

**Perfil A — primeiro pump.** Entra nos primeiros segundos. Limites: `FIRST_PUMP_ENABLED`, `FIRST_PUMP_MAX_TOKEN_AGE_SECONDS`, `FIRST_PUMP_MIN/MAX_MARKET_CAP_SOL`, `FIRST_PUMP_MIN/MAX_VOLUME_SOL`. Não exige momentum nem holders (o token ainda está a formar base de compradores). É o perfil que responde ao objetivo de apanhar o primeiro movimento.

**Perfil B — momentum validado.** Entra quando o token já demonstrou atividade. Limites: `MOMENTUM_PROFILE_ENABLED`, `MOMENTUM_PROFILE_MAX_TOKEN_AGE_SECONDS` (0 = sem limite de idade), `MOMENTUM_PROFILE_MIN/MAX_MARKET_CAP_SOL`, `MOMENTUM_PROFILE_MIN/MAX_VOLUME_SOL`, `MOMENTUM_PROFILE_MIN_HOLDERS`, `MOMENTUM_PROFILE_REQUIRE_MOMENTUM`. Se `MOMENTUM_PROFILE_REQUIRE_MOMENTUM=true`, o `MomentumTracker` tem de aprovar antes de o Perfil B passar.

Cada perfil tem o seu próprio score (ver abaixo). O `PerformanceByProfile` agrega os resultados separadamente, para não misturar métricas de estratégias diferentes.

### Filtro de momentum (janelas móveis)

O `MomentumTracker` mantém janelas móveis por mint. Configuração:

- `MOMENTUM_ENABLED` — liga/desliga o tracker (padrão `false`).
- `MOMENTUM_SHORT_WINDOW_SECONDS` / `MOMENTUM_MEDIUM_WINDOW_SECONDS` / `MOMENTUM_LONG_WINDOW_SECONDS` — janelas em segundos (padrão 10/30/60).
- `MOMENTUM_MIN_VOLUME_SOL` — volume mínimo na janela longa.
- `MOMENTUM_MIN_BUY_RATIO` — proporção mínima de compras (0–1).
- `MOMENTUM_MIN_UNIQUE_BUYERS` — compradores distintos mínimos.
- `MOMENTUM_MIN_ACCELERATION` — aceleração mínima (short vs medium).
- `MOMENTUM_MAX_TRADE_AGE_SECONDS` — idade máxima dos trades guardados (deve cobrir a janela longa).

O tracker regista trades de **candidatos** e de **posições rastreadas**, por isso funciona durante o `ENTRY_DELAY_SECONDS`. Sem `MOMENTUM_ENABLED=true`, o tracker fica inactivo e o Perfil B cai para os filtros de mercado apenas.

### Score em observação (Patches 1–3)

O bot calcula um **score 0–100** por oportunidade aprovada, a partir dos indicadores que tem (volume, buy ratio, unique buyers, aceleração). O score **não bloqueia compras** — é gravado no `Trade.EntryScore` e no `risk_assessments.entry_score`, e o `PerformanceByScoreBucket` agrupa os resultados por faixa (0–20, 20–40, 40–60, 60–80, 80–100).

Pesos configuráveis:

- Perfil A: `FIRST_PUMP_SCORE_WEIGHT_VOLUME`, `FIRST_PUMP_SCORE_WEIGHT_BUY_RATIO`, `FIRST_PUMP_SCORE_WEIGHT_BUYERS`, `FIRST_PUMP_SCORE_WEIGHT_ACCEL`, `FIRST_PUMP_SCORE_VOLUME_CAP_SOL`, `FIRST_PUMP_SCORE_BUYERS_CAP`.
- Perfil B: `MOMENTUM_SCORE_WEIGHT_VOLUME`, `MOMENTUM_SCORE_WEIGHT_BUY_RATIO`, `MOMENTUM_SCORE_WEIGHT_BUYERS`, `MOMENTUM_SCORE_WEIGHT_ACCEL`, `MOMENTUM_SCORE_VOLUME_CAP_SOL`, `MOMENTUM_SCORE_BUYERS_CAP`.

Pesos a `0` significam que o indicador não conta para o score. O cálculo é feito **uma vez** em `profiles.go`; a execução lê o score do `Request`, não recalcula.

### Bloqueio por score (Patch 4)

O bloqueio por score está implementado mas **desligado** por defeito:

- `MIN_ENTRY_SCORE_FIRST_PUMP=0`
- `MIN_ENTRY_SCORE_MOMENTUM=0`

Com `0`, o `prepareLiveRequest` não bloqueia. Só quando subires o valor (por exemplo, `60`) é que os candidatos com score abaixo do mínimo são rejeitados antes da assinatura. **Não coloques um limite universal `70/100` antes de teres dados.** O fluxo recomendado é:

1. Correr em simulação com `MIN_ENTRY_SCORE_*=0`.
2. Analisar `PerformanceByScoreBucket` (faixas altas → win rate melhor?).
3. Só então subir os limites.

Para ativar execução real é necessário, simultaneamente: `SIMULATION=false`, `ENABLE_LIVE_TRADING=true`, um `TRADE_USD` escolhido explicitamente, `BOT_PRIVATE_KEY` carregada somente no ambiente de execução, `KEY_PROVIDER_REF`, `WALLET_PUBLIC_ADDRESS`, um RPC saudável e saldo suficiente além da reserva de taxas. `ADMIN_PASSWORD` é obrigatória para acessar o console. A chave privada nunca é salva no banco, devolvida por HTTP/WebSocket ou registrada em logs.

Copie os nomes de variáveis de `.env.example` para o secret manager/deployment. Não versione um `.env` real.

## Execução contínua

docker build -t solana-sniper-backend .
docker run --env-file /run/secrets/sniper.env -p 8080:8080 solana-sniper-backend

## Implantação e operação em produção

Em produção, configure `SIMULATION_DATABASE_URL` e `REAL_DATABASE_URL` como bancos PostgreSQL distintos. `DATABASE_PATH`/SQLite permanece apenas como fallback local de simulação; o container não requer volume.

Use um *secret manager* para credenciais, reinicialização supervisionada (Compose/Kubernetes/systemd), logs centralizados e monitoramento de `/api/health`.

O processo trata `SIGINT` e `SIGTERM` com desligamento gracioso. O monitor e o gerenciador de RPC tentam recuperar conexões.

Com `LIVE_UNLOCK_REQUIRES_PAPER=true` (padrão), `config.Load()` aplica as migrations e consulta o ledger de simulação antes de aceitar `SIMULATION=false`. O processo repete a validação no bootstrap e mantém o banco paper conectado para exibir o checklist no console.

O gate exige pelo menos **30 operações fechadas** e todos os critérios operacionais descritos no relatório. Na inicialização, as migrations do ledger ativo são aplicadas, os RPCs são verificados e as negociações pendentes são reconciliadas antes de o monitor iniciar.

Uma transação sem resultado conhecido bloqueia novas compras até a reconciliação. Posições já persistidas contam para os limites antes de qualquer entrada.

`MAX_SUBMISSION_ATTEMPTS` controla as retransmissões do mesmo payload assinado no RPC, mantendo a mesma assinatura. `SKIP_PREFLIGHT=true` é o padrão e pode ser desativado explicitamente.

**Parar o bot nunca significa vender todas as posições.**

Ambientes de desenvolvimento não devem ser presumidos como processos 24/7. Configure armazenamento, saúde, reinício, limites de recursos e rotação de segredos no ambiente de implantação.

O worker entra em `running` a cada boot. A senha `ADMIN_PASSWORD` autentica somente o console; ela não inicia o worker e não existe uma rota HTTP `/api/bot/start`.

Use `RPC_HEALTH_CHECK_INTERVAL_SECONDS` (45 segundos por padrão) para limitar a frequência das sondas de saúde RPC.

## API e contratos

Todas as respostas HTTP usam `{ "data": ... }` em caso de sucesso ou `{ "error": { "code": "...", "message": "..." } }` em caso de erro.

### Rotas de leitura

- `GET /api/health` — separa o estado do serviço, a conexão do monitor e a capacidade de negociação.
- `GET /api/dashboard`
- `GET /api/tokens`
- `GET /api/positions`
- `GET /api/trades`
- `GET /api/rpcs`
- `GET /api/config`
- `GET /api/wallet`
- `GET /api/performance/report?from=<iso>&to=<iso>` — relatório autenticado de simulação ou execução real, com métricas, operações fechadas, curva acumulada, resultados por hora e prontidão paper para live.
- `GET /api/simulation/report?from=<iso>&to=<iso>` — compatibilidade com o relatório paper, com o mesmo conteúdo de prontidão.

### Métricas do dashboard e das posições

`/api/dashboard` calcula o P&L aberto para o modo ativo, separa o resultado realizado da estimativa em aberto e informa `estimatedOpenPnlComplete` e `unmarkedOpenPositions`.

`/api/positions` devolve os market caps de entrada e atuais, a idade da marcação e o P&L estimado. `currentTokenPriceSol` permanece `null` porque o feed registra market cap, não o preço unitário do token.

Os valores em aberto usam a variação do market cap como estimativa, não como P&L garantido de uma venda.

O console apresenta esse relatório em **Relatório de desempenho**, nos modos paper e real. O P&L live fechado usa deltas confirmados da carteira e taxas registradas; o P&L das posições abertas continua sendo uma estimativa baseada no market cap.

### Critérios de prontidão paper para live

A prontidão paper para live exige todos os seguintes critérios:

- Pelo menos **7 dias** de histórico e **100 operações fechadas**.
- P&L líquido e expectativa positivos.
- Win rate de pelo menos **45%**.
- Profit factor de pelo menos **1,2**.
- Drawdown máximo de até metade da banca inicial.
- Pelo menos **10 saídas por motivo**: take-profit, trailing, stop-loss e max-hold.
- Reconciliação limpa.
- Retenção média de até **180 segundos**.
- Taxas de até **15% do P&L bruto**.
- Mais de **30% das saídas por trailing stop**.

O teste com 29 operações permanece bloqueado.

### Autenticação

`POST /api/auth/login` recebe:

```json
{
  "password": "..."
}
```

A rota retorna uma sessão de administrador.

As rotas administrativas usam o cabeçalho:

```http
Authorization: Bearer <session-token>
```

### Rotas administrativas

- `POST /api/bot/stop`
- `POST /api/bot/pause`
- `POST /api/bot/resume`
- `POST /api/bot/emergency-stop`
- `POST /api/config`
- `POST /api/rpcs/test`
- `POST /api/rpcs/select`

As rotas `POST /api/rpcs/test` e `POST /api/rpcs/select` recebem:

```json
{
  "url": "https://..."
}
```

A rota `POST /api/config` recebe os campos de `execution.Limits` em JSON.

O painel deve tratar as respostas HTTP **401, 409 e 422** como bloqueios operacionais, não como sucesso.

### WebSocket de eventos

A rota `WS /ws/events` exige que o cliente envie primeiro uma mensagem de autenticação dentro de cinco segundos após o upgrade:

```json
{
  "type": "auth",
  "token": "<session-token>"
}
```

Os tokens HMAC de sessão expiram após **uma hora**.

Depois da autenticação, a rota entrega envelopes no formato:

```json
{
  "type": "event_name",
  "data": {},
  "at": "RFC3339"
}
```

Eventos disponíveis:

- `connection_ready`
- `token_detected`
- `token_approved`
- `token_rejected`
- `buy_sent`
- `buy_confirmed`
- `buy_failed`
- `sell_sent`
- `sell_confirmed`
- `sell_failed`
- `position_updated`
- `position_mark_updated`
- `rpc_changed`
- `risk_alert`
- `risk.alert`
- `bot_paused`
- `operational_error`

O evento `position_mark_updated` inclui o market cap atual, a variação percentual, o P&L e o valor estimados em SOL, além do horário da marcação.

O cliente deve reconectar ao receber um evento de fechamento ou timeout. Cada nova sessão recebe `connection_ready`. O servidor usa ping/pong e remove clientes desconectados.

O dashboard informa contagens de avaliações efetivamente registradas e estatísticas de saídas confirmadas. O P&L aberto continua estimado pela razão entre os market caps e não representa o valor garantido de uma venda.



## Rotas de mercado e guarda de execução

O `execution` não envia qualquer transação sem primeiro classificar o mercado do mint. A classificação é feita por `ResolveMarketRoute` e devolve um `MarketDecision`:

- `MarketBondingCurve` (`pump_curve`): a bonding curve está activa e cotada em SOL. É a rota directa da Pump.fun.
- `MarketPumpSwap` (`pump_amm`): a curva já concluiu e o pool PumpSwap foi confirmado on-chain.
- `MarketTransition`: a curva concluiu mas o pool ainda não foi confirmado. O candidato fica bloqueado até nova leitura.
- `MarketUnsupported`: a cotação não é SOL, ou as reservas da curva são inválidas. O candidato é rejeitado.

O `prepareLiveRequest` chama `validateMarketDecision` e só continua quando a rota é utilizável. Em `MarketPumpSwap`, a preparação verifica o `quoteMint` do pool; se não for SOL, bloqueia. Em `MarketTransition` e `MarketUnsupported`, não há transacção.

### `validateSOLQuoteMint` e o erro 6063

A bonding curve da Pump.fun pode usar uma moeda de cotação diferente de SOL. O programa devolve `UnsupportedQuoteMint` (6063) quando a instrução é construída para uma cotação que a rota directa não suporta.

O bot bloqueia essa rota **antes** de contactar a PumpPortal:

- `validateSOLQuoteMint(state.QuoteMint)` é chamada logo após `GetBondingCurveStateWithContext`.
- Se `QuoteMint != zero`, a execução devolve `unsupported non-SOL quote mint <mint>; live execution blocked`.
- O candidato não chega a preparar transacção, não paga taxa e não gasta prioridade.

Esta validação é a primeira linha de defesa contra o 6063. A segunda é a validação da transacção preparada (ver abaixo).

### PumpSwap: descoberta de pool e cotação

O `rpcmanager` expõe:

- `FindPumpSwapPool(ctx, baseMint)` — procura o pool canónico por `getProgramAccounts`, com filtro `memcmp` no offset 43 (`base_mint`) e offset 75 (`quote_mint`). Valida o owner do programa PumpSwap e o discriminador da conta `Pool`. Devolve `nil, nil` quando o pool ainda não existe (transição).
- `GetPumpSwapReserves(ctx, pool)` — lê `pool_base_token_account` e `pool_quote_token_account` via `getTokenAccountBalance`.
- `QuotePumpSwapSell(ctx, pool, baseAmountIn)` — calcula o output esperado com a fórmula de produto constante e taxas deduzidas do output.

O `preparePumpSwapRequest` usa estes métodos para preparar uma **venda** em PumpSwap. A **compra** em PumpSwap ainda não está implementada; o `preparePumpSwapRequest` bloqueia-a explicitamente com `PumpSwap buy route is not implemented; live execution blocked`. O objectivo é não enviar uma compra com cotação AMM construída à pressa.

## Validação da transacção preparada

Depois de a PumpPortal devolver a transacção serializada, o `execution` não a assina cegamente. A ordem é:

1. `resolveAddressLookupTables(tx)` — resolve as ALT referenciadas na mensagem V0 e injecta os endereços antes de qualquer validação.
2. `validatePreparedTransaction(tx, request, prepared)` — despacha para o validador da rota.
3. `tx.Sanitize()` — normaliza a mensagem.
4. Verificação de `quote_age` (ver abaixo).
5. `wallet.Sign(tx)` — assinatura local.
6. `SendWithMaxRetries` — envio pelo RPC.

### `validateDirectPumpRoute`

Para a rota directa Pump.fun, a lista de programas permitidos é fechada:

- `ComputeBudget111111111111111111111111111111`
- `11111111111111111111111111111111` (System)
- `ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL` (ATA)
- `TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA` (SPL Token)
- `TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb` (Token-2022)
- `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` (Pump.fun)

Qualquer outro programa na mensagem faz a validação falhar com `unapproved program <id> in direct Pump.fun route`. Isto impede que uma transacção PumpPortal inclua programas inesperados com autoridade de gasto da carteira.

### `validateProtectedPumpBuy`

Confirma que a instrução `buy` da Pump.fun existe, que o `mint` pedido está nas contas, e que a bonding curve derivada (`["bonding-curve", mint]`) também está. Se a instrução não referenciar ambos, a transacção é rejeitada.

### `validateProtectedPumpSell`

Confirma que a instrução `sell` da Pump.fun existe e que:

- a quantidade de tokens corresponde ao saldo confirmado da carteira;
- o `min_quote_amount_out` da instrução não é inferior ao mínimo SOL calculado (`minimumOutputSOL`).

Se a transacção não garantir o mínimo calculado, é rejeitada.

### `validatePumpSwapTransaction`

Para a rota PumpSwap, a validação verifica:

- programas permitidos (PumpSwap `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA`, Pump fees `pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ`, Token, Token-2022, ATA, System, ComputeBudget);
- discriminador de `sell`;
- `base_amount_in` e `min_quote_amount_out` contra o `prepared`;
- 21 contas fixas da instrução `sell` (pool, user, global_config, base_mint, quote_mint, vaults do utilizador, vaults do pool, protocol fee, token programs, system, ATA, event authority, program, coin creator vault ATA e authority, fee config, fee program);
- consistência dos mints e vaults (não-zero, distintos).

## Address Lookup Tables (ALT)

A PumpPortal pode devolver uma transacção V0 com `AddressTableLookups`. O `resolveAddressLookupTables`:

- Lê cada ALT via `GetAccountInfo`.
- Descodifica o estado com `lookup.DecodeAddressLookupTableState`.
- Rejeita ALT inactiva.
- Injecta os endereços na mensagem (`SetAddressTables`) e resolve os índices (`ResolveLookups`).

Sem esta resolução, a mensagem não está completa e a validação falharia por índice de conta inválido. É por isso que `validatePreparedTransaction` exige `tx.Message.IsResolved() == true` quando há lookups.

## `quote_age` e `MAX_QUOTE_AGE_MS`

Antes de assinar, o `execution` verifica a idade da cotação que fundamentou a preparação:

- `quoteAge := time.Since(prepared.quotedAt)`
- Se `quoteAge > MAX_QUOTE_AGE_MS`, a transacção é rejeitada com `quote exceeded MAX_QUOTE_AGE_MS before submission`.
- Se passar, o `quote_observed_at`, `quote_age_ms`, `curve_impact_bps`, `minimum_output_sol`, `requested_value_sol` e `quantity` são persistidos em `trades` via `UpdateTradeQuote`.

Isto impede que uma cotação antiga seja usada para assinar uma transacção que já não corresponde ao mercado.

## `curveImpactBPS` e `MAX_CURVE_IMPACT_BPS`

O `prepareBondingCurveRequest` calcula o impacto estimado da ordem na curva:

- Compra: `request.Amount / (virtualSOL + request.Amount) * 10000`.
- Venda: `tokenAmount / (virtualTokens + tokenAmount) * 10000`.

O valor (`curveImpactBPS`) é comparado com `MAX_CURVE_IMPACT_BPS`. Se exceder, a preparação falha com `estimated curve impact <x> bps exceeds MAX_CURVE_IMPACT_BPS`. Isto impede ordens grandes para a liquidez disponível.

## `SkipPreflight` como decisão de risco

O `rpcmanager.SendWithMaxRetries` respeita o `skipPreflight` recebido do `execution`. **Não força `true`.** O `execution` lê `Limits.SkipPreflight`, que vem do `SKIP_PREFLIGHT` do `.env`.

- `SKIP_PREFLIGHT=true` (padrão): a transacção é enviada sem simulação prévia. É mais rápido, mas uma transacção inválida chega à blockchain e paga taxa.
- `SKIP_PREFLIGHT=false`: o RPC simula antes de submeter. Transacções que falhariam deterministicamente (ex.: 6063) são rejeitadas sem custo.

O comentário no `rpcmanager` explica a escolha: o preflight é uma decisão de risco e não deve ser silenciosamente forçado. O `.env` controla.

## Holders e cache

O `holders.go` define:

- `HolderSource` — interface com `HolderCount(ctx, mint) (int, error)`.
- `NoopHolderSource` — devolve `0, nil`. É o default quando não há fonte real configurada.
- `CachedHolderSource` — envolve outra fonte com cache TTL por mint. O TTL é `HOLDERS_CACHE_SECONDS`.

O `finishCandidate` só consulta holders quando o Perfil B está activo **e** `MOMENTUM_PROFILE_MIN_HOLDERS > 0`. Sem fonte real, o `NoopHolderSource` devolve `0` e o Perfil B rejeita se `HolderCountKnown=true` e `count < MinHolders`. Na prática, para o Perfil B exigir holders, é preciso configurar uma fonte real (RPC, API externa, etc.).

## `MomentumTracker`, `TradeEvent` e `MomentumSnapshot`

O `momentum.go` define:

- `TradeEvent` — `{Mint, TxType, SOLAmount, Trader, At}`. É o que o `monitor.go` regista quando recebe `subscribeTokenTrade` para candidatos e posições rastreadas.
- `MomentumConfig` — janelas (`ShortWindow`, `MediumWindow`, `LongWindow`), limites (`MinVolumeSOL`, `MinBuyRatio`, `MinUniqueBuyers`, `MinAcceleration`), `MaxTradeAge` e `Enabled`.
- `MomentumSnapshot` — resultado da avaliação, com:
  - `VolumeShort`, `VolumeMedium`, `VolumeLong` — volume em SOL por janela;
  - `BuyVolume`, `SellVolume`, `BuyRatio`;
  - `UniqueBuyers`;
  - `Acceleration` — rácio normalizado entre o volume curto e o volume médio. Valores altos indicam aceleração; valores baixos indicam abrandamento;
  - `LastTradeAt`, `TradeCount`;
  - `Sufficient`, `RejectReason`.

O `Evaluate` percorre os trades dentro das janelas e devolve `Sufficient=false` com motivo quando:

- o volume na janela longa é inferior a `MOMENTUM_MIN_VOLUME_SOL`;
- o `BuyRatio` é inferior a `MOMENTUM_MIN_BUY_RATIO`;
- os `UniqueBuyers` são inferiores a `MOMENTUM_MIN_UNIQUE_BUYERS`;
- a `Acceleration` é inferior a `MOMENTUM_MIN_ACCELERATION`.

O `Record` guarda o trade na fila do mint e descarta trades mais antigos que `MOMENTUM_MAX_TRADE_AGE_SECONDS`. O `Forget` limpa o estado de um mint (chamado em `rejectCandidate` e quando a compra falha). O `Reset` limpa tudo (chamado no `ResetInMemoryState` do `Monitor`).

Sem `MOMENTUM_ENABLED=true`, o `Record` é no-op e o `Evaluate` devolve `Sufficient=true`. O Perfil B cai para os filtros de mercado apenas. Com `MOMENTUM_ENABLED=true`, o `Monitor` subscreve `subscribeTokenTrade` para candidatos durante o `ENTRY_DELAY_SECONDS`, para o tracker ter dados no momento do `finishCandidate`.

## Banco de dados e migrations

PostgreSQL é o banco de produção. As migrations versionadas criam configurações, tokens, avaliações de risco, posições, negociações, transações, saúde de RPC e logs operacionais.

A criação de uma negociação e seu registro de transação, assim como as mudanças de estado, são transacionais.

SQLite é mantido somente para desenvolvimento e testes locais quando `DATABASE_URL` não estiver definido.

### Migration de entry score

As migrations novas:

- `008_entry_score.sql`
- `migrations/postgres/008_entry_score.sql`

acrescentam o campo `entry_score` a `trades` e `risk_assessments`, para armazenar o score em observação.

A migration é idempotente no PostgreSQL, usando `IF NOT EXISTS`, e executada uma vez no SQLite por meio de `schema_migrations`.

## Testes

### Testes unitários

Execute os testes unitários sem rede externa:

```sh
go test ./...
```

### Laboratório de cenários do frontend

O modelo de cenários do laboratório no frontend pode ser testado sem iniciar a aplicação:

```sh
cd front && npm run test:flow-lab
```

### Testes de integração

Os testes de integração ficam separados em `tests/integration` e devem usar SQLite, HTTP/RPC e WebSocket simulados ou locais.

**Não são autorizadas compras reais na suíte de testes.**

### Smoke test cloud

O smoke test cloud não envia transações nem altera o estado do bot. Ele usa `ADMIN_PASSWORD` somente para testar o login do console:

```powershell
$env:ADMIN_PASSWORD = 'sua-senha'
.\tests\e2e-cloud.ps1
```

## Score e métricas por perfil de entrada

### Performance por perfil

`PerformanceByProfile(simulation, profile)` devolve as mesmas métricas de `Performance`, filtradas por `entry_profile` (`first_pump` ou `momentum`).

Isso permite comparar os dois perfis sem misturar seus resultados.

### Performance por faixa de score

`PerformanceByScoreBucket(simulation)` agrupa os trades fechados por faixa de `entry_score`:

- `0-20`
- `20-40`
- `40-60`
- `60-80`
- `80-100`
- `unknown`

Para cada faixa, o relatório devolve:

- Número de trades.
- Winners.
- Losers.
- Win rate.
- P&L líquido.
- Taxas.

Só depois de essa análise mostrar que faixas de score mais altas apresentam melhor resultado líquido é que faz sentido aumentar os valores de `MIN_ENTRY_SCORE_*`.

### Cálculo e persistência do score

O score é calculado em `profiles.go`, por meio de `computeProfileScore`, e propagado em `reason` com o prefixo:

```text
profile:<kind>:score=<float>:
```

O `execution.go` extrai o perfil e o score e grava esses valores em `database.Trade`. O score não é recalculado em `prepareLiveRequest`.

Quando o bloqueio por score está ativo, ele compara o score que já vem no `Request` com o mínimo configurado.


---

