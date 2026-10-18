# Solana Sniper Bot — backend

Backend Go conservador para observação de novos tokens, controles de risco e execução via PumpPortal Local API. Ele **não promete lucro** e a estratégia padrão apenas observa: um evento de criação de token não comprova liquidez, preço ou possibilidade de saída.

## Segurança e modo de execução

O padrão é `SIMULATION=true`. Neste modo o serviço registra a negociação simulada, mas não assina nem envia bytes de transação.

A simulação calcula saídas virtuais a partir das atualizações de preço por token do PumpPortal. Por isso, `PUMPPORTAL_API_KEY` também é obrigatória em simulação; o PumpPortal exige uma carteira vinculada com pelo menos 0,02 SOL para essa assinatura. O padrão de stop-loss da simulação é 30% e o take-profit é 70%; ambos podem ser ajustados por configuração. A banca exibida como saldo disponível desconta o capital alocado em posições abertas e incorpora resultados realizados após cada saída.

Para ativar execução real é necessário, simultaneamente: `SIMULATION=false`, `ENABLE_LIVE_TRADING=true`, `BOT_PRIVATE_KEY` carregada somente no ambiente de execução, `KEY_PROVIDER_REF`, `WALLET_PUBLIC_ADDRESS`, um RPC saudável e saldo suficiente além da reserva de taxas. `ADMIN_PASSWORD` é obrigatória para acessar o console. A chave privada nunca é salva no banco, devolvida por HTTP/WebSocket ou registrada em logs.

Copie os nomes de variáveis de `.env.example` para o secret manager/deployment. Não versione um `.env` real.

## Execução contínua

Requer Go 1.22+ para execução local, ou Docker para build/deploy:

```sh
docker build -t solana-sniper-backend .
docker run --env-file /run/secrets/sniper.env -p 8080:8080 solana-sniper-backend
```

Em produção, configure `DATABASE_URL` com a URL PostgreSQL fornecida pelo Railway. `DATABASE_PATH`/SQLite permanece apenas como fallback local; o container não requer volume. Use secret manager para credenciais, reinicialização supervisionada (Compose/Kubernetes/systemd), logs centralizados e monitoramento de `/api/health`. O processo trata `SIGINT` e `SIGTERM` com desligamento gracioso; o monitor e o gerenciador de RPC tentam recuperar conexões.

Na inicialização, migrações são aplicadas, RPCs são verificados e negociações pendentes são reconciliadas antes de o monitor iniciar. Uma transação sem resultado conhecido bloqueia novas compras até reconciliação. Posições já persistidas contam para os limites antes de qualquer entrada. Parar o bot nunca significa vender todas as posições.

Ambientes de desenvolvimento não devem ser presumidos como processos 24/7: configure armazenamento, saúde, reinício, limites de recursos e rotação de segredos no ambiente de implantação.

O worker entra em `running` a cada boot. A senha `ADMIN_PASSWORD` autentica somente o console; ela nao inicia o worker e nao ha rota HTTP `/api/bot/start`. Use `RPC_HEALTH_CHECK_INTERVAL_SECONDS` (45 por padrao) para limitar a frequencia das sondas de saude RPC.

## API e contratos

Todas as respostas HTTP usam `{ "data": ... }` em sucesso ou `{ "error": { "code": "...", "message": "..." } }` em erro.

Rotas de leitura:

- `GET /api/health` — separa serviço ativo, monitor conectado e capacidade de negociação.
- `GET /api/dashboard`, `/api/tokens`, `/api/positions`, `/api/trades`, `/api/rpcs`, `/api/config`, `/api/wallet`.

Autenticação: `POST /api/auth/login` recebe `{ "password": "..." }` e retorna uma sessão de administrador. Rotas administrativas usam `Authorization: Bearer <session-token>`:

- `POST /api/bot/stop`, `/pause`, `/resume`, `/emergency-stop`
- `POST /api/config`
- `POST /api/rpcs/test` e `/api/rpcs/select`

`POST /api/rpcs/test` e `/api/rpcs/select` recebem `{ "url": "https://..." }`. Configuração recebe os campos de `execution.Limits` em JSON. O painel deve tratar `401`, `409` e `422` como bloqueios operacionais, não como sucesso.

`WS /ws/events` entrega envelopes `{ "type": string, "data": object, "at": RFC3339 }`. Eventos incluem `token_detected`, `token_approved`, `token_rejected`, `buy_sent`, `buy_confirmed`, `buy_failed`, `sell_sent`, `sell_confirmed`, `sell_failed`, `position_updated`, `rpc_changed`, `risk_alert`, `bot_paused` e `operational_error`. O cliente deve reconectar ao receber close/timeout; cada nova sessão recebe `connection_ready`. O servidor usa ping/pong e remove clientes desconectados.

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
