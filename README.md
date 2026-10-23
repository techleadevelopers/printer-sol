# Solana Sniper Bot — backend

Backend Go conservador para observação de novos tokens, controles de risco e execução via PumpPortal Local API. Ele **não promete lucro** e a estratégia padrão apenas observa: um evento de criação de token não comprova liquidez, preço ou possibilidade de saída.

## Segurança e modo de execução

O padrão é `SIMULATION=true`. Neste modo o serviço registra a negociação simulada, mas não assina nem envia bytes de transação. O perfil de exemplo usa ordem de US$ 3, prioridade por transação de 0,001 SOL, taxa-base simulada de 0,000005 SOL, take-profit bruto de 50%, stop-loss de 25%, trailing stop de 12% armado após ganho de 10% sobre o custo e limite de retenção `MAX_HOLD_SECONDS=300` (configurável). RugCheck roda de forma assíncrona depois da entrada e não bloqueia o sniping. São parâmetros de operação, não promessa de lucro: preço, taxas, slippage, liquidez e execução podem causar perdas.

A simulação calcula saídas virtuais a partir das atualizações de preço por token do PumpPortal. Para isso, `PUMPPORTAL_API_KEY` também é obrigatória em simulação; o PumpPortal exige uma carteira vinculada com pelo menos 0,02 SOL para essa assinatura. A cotação SOL/USD validada é reutilizada por até 30 segundos; se expirar e a atualização falhar, novas entradas são bloqueadas. A banca exibida como saldo disponível desconta o capital alocado em posições abertas e incorpora resultados realizados após cada saída.

Novos mints aguardam `ENTRY_DELAY_SECONDS` antes do gate de entrada. Com `USE_WS_MARKET_CAP=true` (padrão), o market cap do PumpPortal é usado no gate de market cap e como baseline de entrada; `MIN_MARKET_CAP_SOL` e `MAX_MARKET_CAP_SOL` são limites estritos, sem tolerância extra (`MAX_MARKET_CAP_SOL=0` desativa apenas o teto). Em qualquer modo, `MIN_VOLUME_BEFORE_BUY` e `MAX_VOLUME_BEFORE_BUY` são verificados antes da compra contra as reservas reais de SOL da bonding curve via RPC (faixa inclusiva; padrão 0–1,5 SOL). Essas reservas são uma proxy de liquidez, não volume negociado acumulado. A entrada é rejeitada se o RPC não confirmar a curva ou se as reservas estiverem fora dos limites. Defina `USE_WS_MARKET_CAP=false` para usar também o market cap da bonding curve via RPC no gate e no baseline. As marcas do WebSocket são a fonte principal e processadas em segundo plano com fila coalescida por token; `PRICE_POLL_SECONDS` atualiza via RPC apenas marcas antigas no modo WebSocket e todas as marcas no modo legado. A estimativa de P&L aberto deriva da marca atual; P&L realizado só inclui vendas concluídas. As respostas da API usam `Cache-Control: no-store`, e o painel consulta os agregados a cada cinco segundos. `MAX_HOLD_SECONDS` é configurável (420 = sete minutos) e só fecha por max hold quando o retorno é zero ou negativo; retornos positivos permanecem abertos para take-profit ou trailing stop. A recuperação por quarentena é separada do max hold e atua somente sobre saídas travadas após `STUCK_EXIT_QUARANTINE_SECONDS` (120s por padrão), nunca em posições saudáveis. Sem marca positiva, a posição permanece aberta e gera alerta. `TOKEN_MAX_AGE_SECONDS` tem padrão de 30 segundos, respeita o valor configurado sem tolerância adicional e `0` desativa o filtro de idade em qualquer modo; a configuração é passada tanto à estratégia quanto ao monitor.

`MAX_SLIPPAGE_BPS`, `MAX_QUOTE_AGE_MS` e `MAX_CURVE_IMPACT_BPS` são limites independentes e obrigatórios, calibrados com resultados reais. A confirmação live persiste o valor, a quantidade de tokens e a taxa observados no recibo on-chain. A venda usa o saldo confirmado da carteira e só é assinada se a instrução Pump.fun oferecer e aplicar o mínimo SOL calculado. Esses limites não eliminam atraso de rede, prioridade insuficiente nem competição com outras ordens; slippage só garante o mínimo quando o próprio programa de swap aplica essa proteção.

Para ativar execução real é necessário, simultaneamente: `SIMULATION=false`, `ENABLE_LIVE_TRADING=true`, um `TRADE_USD` escolhido explicitamente, `BOT_PRIVATE_KEY` carregada somente no ambiente de execução, `KEY_PROVIDER_REF`, `WALLET_PUBLIC_ADDRESS`, um RPC saudável e saldo suficiente além da reserva de taxas. `ADMIN_PASSWORD` é obrigatória para acessar o console. A chave privada nunca é salva no banco, devolvida por HTTP/WebSocket ou registrada em logs.

Copie os nomes de variáveis de `.env.example` para o secret manager/deployment. Não versione um `.env` real.

## Execução contínua

Requer Go 1.24+ para execução local, ou Docker para build/deploy:

```sh
docker build -t solana-sniper-backend .
docker run --env-file /run/secrets/sniper.env -p 8080:8080 solana-sniper-backend
```

Em produção, configure `SIMULATION_DATABASE_URL` e `REAL_DATABASE_URL` como bancos PostgreSQL distintos. `DATABASE_PATH`/SQLite permanece apenas como fallback local de simulação; o container não requer volume. Use secret manager para credenciais, reinicialização supervisionada (Compose/Kubernetes/systemd), logs centralizados e monitoramento de `/api/health`. O processo trata `SIGINT` e `SIGTERM` com desligamento gracioso; o monitor e o gerenciador de RPC tentam recuperar conexões.

Com `LIVE_UNLOCK_REQUIRES_PAPER=true` (padrão), `config.Load()` aplica as migrations e consulta o ledger de simulação antes de aceitar `SIMULATION=false`; o processo repete a validação no bootstrap e mantém o banco paper conectado para exibir o checklist no console. O gate exige pelo menos 30 operações fechadas e todos os critérios operacionais descritos no relatório. Na inicialização, as migrations do ledger ativo são aplicadas, RPCs são verificados e negociações pendentes são reconciliadas antes de o monitor iniciar. Uma transação sem resultado conhecido bloqueia novas compras até reconciliação. Posições já persistidas contam para os limites antes de qualquer entrada. `MAX_SUBMISSION_ATTEMPTS` controla retransmissões do mesmo payload assinado no RPC (mesma assinatura); `SKIP_PREFLIGHT=true` é o padrão e pode ser desativado explicitamente. Parar o bot nunca significa vender todas as posições.

Ambientes de desenvolvimento não devem ser presumidos como processos 24/7: configure armazenamento, saúde, reinício, limites de recursos e rotação de segredos no ambiente de implantação.

O worker entra em `running` a cada boot. A senha `ADMIN_PASSWORD` autentica somente o console; ela nao inicia o worker e nao ha rota HTTP `/api/bot/start`. Use `RPC_HEALTH_CHECK_INTERVAL_SECONDS` (45 por padrao) para limitar a frequencia das sondas de saude RPC.

## API e contratos

Todas as respostas HTTP usam `{ "data": ... }` em sucesso ou `{ "error": { "code": "...", "message": "..." } }` em erro.

Rotas de leitura:

- `GET /api/health` — separa serviço ativo, monitor conectado e capacidade de negociação.
- `GET /api/dashboard`, `/api/tokens`, `/api/positions`, `/api/trades`, `/api/rpcs`, `/api/config`, `/api/wallet`.
- `GET /api/performance/report?from=<iso>&to=<iso>` — relatório autenticado de simulação ou execução real, com métricas, operações fechadas, curva acumulada, resultados por hora e prontidão paper para live.
- `GET /api/simulation/report?from=<iso>&to=<iso>` — compatibilidade com o relatório paper e mesmo conteúdo de prontidão.

`/api/dashboard` calcula P&L aberto para o modo ativo, separa resultado realizado de estimativa aberta e informa `estimatedOpenPnlComplete`/`unmarkedOpenPositions`. `/api/positions` devolve os market caps de entrada e atuais, idade da marca e P&L estimado; `currentTokenPriceSol` permanece `null` porque o feed registra market cap, não preço unitário do token. Valores abertos usam a variação de market cap como estimativa, não como P&L garantido de uma venda.

O console apresenta esse relatório em **Relatório de desempenho** nos modos paper e real. P&L live fechado usa deltas confirmados da carteira e taxas registradas; P&L de posições abertas continua sendo uma estimativa por market-cap. A prontidão paper para live exige 7 dias e 100 operações fechadas, P&L líquido e expectativa positivos, win rate de pelo menos 45%, profit factor de pelo menos 1,2, drawdown máximo de até metade da banca inicial, pelo menos 10 saídas por cada motivo (take-profit, trailing, stop-loss e max-hold), reconciliação limpa, retenção média de até 180 segundos, taxas de até 15% do P&L bruto e mais de 30% das saídas por trailing stop. O teste com 29 operações permanece bloqueado.

Autenticação: `POST /api/auth/login` recebe `{ "password": "..." }` e retorna uma sessão de administrador. Rotas administrativas usam `Authorization: Bearer <session-token>`:

- `POST /api/bot/stop`, `/pause`, `/resume`, `/emergency-stop`
- `POST /api/config`
- `POST /api/rpcs/test` e `/api/rpcs/select`

`POST /api/rpcs/test` e `/api/rpcs/select` recebem `{ "url": "https://..." }`. Configuração recebe os campos de `execution.Limits` em JSON. O painel deve tratar `401`, `409` e `422` como bloqueios operacionais, não como sucesso.

`WS /ws/events` exige que o cliente envie primeiro `{ "type": "auth", "token": "<session-token>" }` dentro de cinco segundos do upgrade. Tokens HMAC de sessão expiram após uma hora. Depois da autenticação, a rota entrega envelopes `{ "type": string, "data": object, "at": RFC3339 }`. Eventos incluem `token_detected`, `token_approved`, `token_rejected`, `buy_sent`, `buy_confirmed`, `buy_failed`, `sell_sent`, `sell_confirmed`, `sell_failed`, `position_updated`, `position_mark_updated`, `rpc_changed`, `risk_alert`, `risk.alert`, `bot_paused` e `operational_error`. `position_mark_updated` inclui market cap atual, variação percentual, P&L/valor estimados em SOL e horário da marca. O cliente deve reconectar ao receber close/timeout; cada nova sessão recebe `connection_ready`. O servidor usa ping/pong e remove clientes desconectados.

O dashboard informa contagens de avaliações efetivamente registradas e estatísticas de saídas confirmadas. O P&L aberto continua sendo estimado pela razão entre market caps; não representa o valor garantido de uma venda.

## Banco e testes

PostgreSQL é o banco de produção. As migrações versionadas criam configurações, tokens, avaliações de risco, posições, negociações, transações, saúde de RPC e logs operacionais. A criação de uma negociação e seu registro de transação, assim como a mudança de estado, são transacionais. SQLite é mantido somente para desenvolvimento e testes locais quando `DATABASE_URL` não estiver definido.

Execute testes unitários sem rede externa:

```sh
go test ./...
```

Testes de integração ficam separados em `tests/integration` e devem usar SQLite, HTTP/RPC e WebSocket simulados/localmente. Não são autorizadas compras reais na suíte.

O smoke test cloud nao envia transacoes nem muda o estado do bot; ele usa `ADMIN_PASSWORD` somente para testar o login do console:

```powershell
$env:ADMIN_PASSWORD = 'sua-senha'
.\tests\e2e-cloud.ps1
```
