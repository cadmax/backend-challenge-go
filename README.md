# Jungle Gaming — processamento de apostas em Go

API HTTP e consumidor SQS para movimentar carteiras com dinheiro exato, ledger
imutável e idempotência persistente. As duas entradas usam o mesmo caso de uso;
PostgreSQL coordena as operações entre processos, e uma outbox publica os eventos
após o commit financeiro.

O enunciado original está em [docs/CHALLENGE.md](docs/CHALLENGE.md). As decisões,
garantias e limitações estão em [ARCHITECTURE.md](ARCHITECTURE.md); os detalhes do
provisionamento local estão em [infra/README.md](infra/README.md).

O [registro de verificação](docs/VERIFICATION.md) reúne os comandos executados,
os resultados observados e os limites dessa validação.

## Executar a partir de um checkout limpo

Pré-requisitos: Docker Engine/Desktop com Docker Compose v2. Para executar Go
fora do contêiner ou os testes, Go **1.26.7** e um compilador C compatível com
`go test -race`. Os exemplos usam `curl`, `jq` e Python 3; o exemplo de SQS usa
AWS CLI v2. Não é necessário ter conta AWS para o ambiente local.

```sh
git clone https://github.com/cadmax/backend-challenge-go.git
cd backend-challenge-go
cp .env.example .env
docker compose up --build
```

O Compose inicia PostgreSQL, importa o realm do Keycloak, cria as filas e as
identidades IAM no LocalStack, executa a migration e inicia a API. O primeiro
início pode levar alguns minutos para baixar as imagens. Em outro terminal:

```sh
docker compose ps
curl --fail http://localhost:8080/health/ready
./scripts/smoke.sh
```

Para iniciar em segundo plano e aguardar readiness: `make up`.

| Serviço | Endereço no host |
| --- | --- |
| API | `http://localhost:8080` |
| Keycloak | `http://localhost:8081` |
| PostgreSQL | `localhost:55432` |
| LocalStack | `http://localhost:4567` |

As portas são publicadas somente em `127.0.0.1`. PostgreSQL e LocalStack usam
portas alternativas para coexistir com instalações locais comuns. Ajuste
`HTTP_PORT`, `POSTGRES_PORT`, `KEYCLOAK_PORT` e `LOCALSTACK_PORT` no `.env` se
necessário. Dentro da rede Docker os serviços usam seus próprios nomes e portas.

```sh
make status
make logs
make down
```

`make down` preserva os dados do PostgreSQL. O LocalStack local não oferece a
persistência de mensagens da AWS; as simulações de falha reiniciam os processos
da aplicação, mantendo o broker em execução.

## Autenticação

O realm `jungle` já contém clientes confidenciais com `client_credentials`.
Os segredos abaixo são exemplos exclusivos de desenvolvimento.

| Client ID | Segredo | Permissão |
| --- | --- | --- |
| `internal-service` | `local-internal-secret` | Abrir, consultar e reconciliar carteiras |
| `provider-a` | `local-provider-a-secret` | Enviar e consultar operações de `provider-a` |
| `provider-b` | `local-provider-b-secret` | Enviar e consultar operações de `provider-b` |
| `provider-expired` | `local-provider-expired-secret` | Identidade `provider-a`, token de 2 segundos para testes |

Os tokens normais duram 300 segundos. Renove as variáveis quando expirarem:

```sh
INTERNAL_TOKEN=$(./scripts/token.sh internal-service)
PROVIDER_TOKEN=$(./scripts/token.sh provider-a)
```

O script chama o endpoint OAuth abaixo; também é possível obter o token
manualmente:

```sh
curl --fail --silent --show-error \
  http://localhost:8081/realms/jungle/protocol/openid-connect/token \
  --data-urlencode grant_type=client_credentials \
  --data-urlencode client_id=provider-a \
  --data-urlencode client_secret=local-provider-a-secret
```

O papel `wallet:internal` protege todas as rotas de carteira. O papel
`wager:provider` e o claim assinado `provider_id` protegem as operações do
provedor, inclusive consultas por ID interno e replays. O corpo não pode
escolher uma identidade diferente da autenticada. O console local do Keycloak
usa `admin` / `local-admin-secret`.

## Exemplo completo pela API

Os comandos a seguir podem ser executados na mesma sessão de shell. Identidades
novas evitam conflitos quando o exemplo é repetido.

### Abrir uma carteira

```sh
API=http://localhost:8080
PLAYER_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
RUN_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')

WALLET=$(curl --fail-with-body --silent --show-error "$API/wallets" \
  -H "Authorization: Bearer $INTERNAL_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER_ID\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")
printf '%s\n' "$WALLET" | jq .
WALLET_ID=$(printf '%s' "$WALLET" | jq -r .id)
```

A resposta é `201 Created`, saldo `100.00` e versão `1`. A abertura positiva
inclui `OPENING`, crédito no ledger e dois eventos no mesmo commit. Abertura com
`0.00` não cria operação financeira, ledger ou eventos. O par jogador/moeda é
único; repetir a abertura resulta em `409`.

### Apostar e guardar o resultado

```sh
BET_ID="bet-$RUN_ID"
BET_KEY="provider-a:$BET_ID"
BET_PAYLOAD=$(jq -n \
  --arg player "$PLAYER_ID" --arg wallet "$WALLET_ID" --arg external "$BET_ID" \
  '{providerId:"provider-a", externalTransactionId:$external,
    playerId:$player, walletId:$wallet, roundId:"round-example",
    gameId:"fortune-chimp", kind:"BET", money:{amount:"25.00",currency:"BRL"}}')

BET_RESULT=$(curl --fail-with-body --silent --show-error "$API/wagering/transactions" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $BET_KEY" \
  -H "X-Correlation-ID: example-$RUN_ID" \
  -d "$BET_PAYLOAD")
printf '%s\n' "$BET_RESULT" | jq .
TRANSACTION_ID=$(printf '%s' "$BET_RESULT" | jq -r .transactionId)
```

O resultado esperado é `PROCESSED`, saldo `75.00` e
`idempotentReplay: false`. `Idempotency-Key` é obrigatório e aparece somente
no header HTTP. Valores monetários são strings: `25.00`, nunca um número JSON.

### Devolver integralmente a aposta

```sh
REFUND_PAYLOAD=$(printf '%s' "$BET_PAYLOAD" | jq \
  --arg external "refund-$RUN_ID" --arg reference "$BET_ID" \
  '.externalTransactionId=$external | .kind="REFUND" |
   .referenceExternalTransactionId=$reference')

curl --fail-with-body --silent --show-error "$API/wagering/transactions" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: provider-a:refund-$RUN_ID" \
  -d "$REFUND_PAYLOAD" | jq .
```

O saldo retorna a `100.00`. `REFUND` exige uma `BET` processada, valor integral
e mesma carteira, jogador, provedor, moeda e rodada. Uma referência ausente
produz `202 PENDING_REFERENCE` e é retomada pelo worker.

### Reenviar e consultar

```sh
curl --fail-with-body --silent --show-error "$API/wagering/transactions" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $BET_KEY" \
  -d "$BET_PAYLOAD" | jq .

curl --fail --silent "$API/wagering/transactions/$TRANSACTION_ID" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" | jq .
curl --fail --silent "$API/providers/provider-a/wagering/transactions/$BET_ID" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" | jq .
curl --fail --silent "$API/wallets/$WALLET_ID" \
  -H "Authorization: Bearer $INTERNAL_TOKEN" | jq .
```

O replay da aposta retorna `idempotentReplay: true` e o saldo histórico `75.00`,
mesmo depois do reembolso. A consulta da carteira mostra o saldo atual `100.00`.
Usar a mesma chave com outro conteúdo resulta em conflito; trocar apenas a
chave para o mesmo ID externo também não reaplica a movimentação.

### Ledger e reconciliação

```sh
PAGE=$(curl --fail --silent "$API/wallets/$WALLET_ID/ledger?limit=2" \
  -H "Authorization: Bearer $INTERNAL_TOKEN")
printf '%s\n' "$PAGE" | jq .
CURSOR=$(printf '%s' "$PAGE" | jq -r '.nextCursor // empty')
if [ -n "$CURSOR" ]; then
  curl --fail --silent --get "$API/wallets/$WALLET_ID/ledger" \
    -H "Authorization: Bearer $INTERNAL_TOKEN" \
    --data-urlencode "cursor=$CURSOR" --data-urlencode limit=2 | jq .
fi

curl --fail --silent -X POST "$API/wallets/$WALLET_ID/reconciliation" \
  -H "Authorization: Bearer $INTERNAL_TOKEN" | jq .
```

O ledger contém abertura, aposta e reembolso. A reconciliação reconstrói
`100.00`, retorna diferença `0.00`, `consistent: true` e `checkedEntries: 3`.
Ela não altera o saldo. A paginação tem ordenação estável, cursor opaco e limite
entre 1 e 100; o padrão é 50.

## Contrato de operações e erros

| Tipo | Valor | Efeito |
| --- | --- | --- |
| `BET` | Positivo | Débito sujeito a saldo suficiente |
| `WIN` | Positivo | Crédito; referência opcional a `BET` processada da mesma rodada |
| `LOSS` | `0.00` | Sem ledger ou alteração de versão; emite evento de processamento |
| `REFUND` | Igual à `BET` referenciada | Devolução integral |
| `ROLLBACK` | Igual à operação referenciada | Movimento contrário de `BET`, `WIN` ou `REFUND` |

`OPENING` é exclusivo da abertura interna. Não é aceito em HTTP ou SQS.
`REFUND` e `ROLLBACK` competem por uma única reversão bem-sucedida da mesma
referência. O rollback de um reembolso não libera a aposta original para nova
devolução; a justificativa está no documento de arquitetura.

| HTTP | Situação | Corpo |
| --- | --- | --- |
| `201` | Carteira criada | Carteira, saldo e versão |
| `200` | Operação processada/replay ou leitura | Resultado persistido ou recurso |
| `202` | Referência pendente | `transactionId`, `status`, `idempotentReplay` |
| `400` | Entrada inválida/corrigível | `code: INVALID_INPUT`, `message` |
| `401` | Token ausente, inválido ou expirado | `code: UNAUTHORIZED`, `message` |
| `403` | Papel ou provedor incompatível | `code: FORBIDDEN`, `message` |
| `404` | Recurso inexistente ou transação fora do provedor | `code: NOT_FOUND`, `message` |
| `409` | Chave/conteúdo ou carteira em conflito | `code: CONFLICT`, `message` |
| `422` | Rejeição financeira definitiva | Resultado `REJECTED`, `failureCode`, saldo observado |
| `503` | Dependência indisponível/timeout | `code: UNAVAILABLE`, `message`; repetir com a mesma chave |

Consultas `GET` de uma operação existente retornam `200` inclusive quando seu
estado é pendente ou rejeitado. O status HTTP da tabela para operações se refere
a `POST /wagering/transactions`. Campos desconhecidos, campos JSON duplicados,
valores monetários inválidos e corpos acima de 32 KiB são recusados na entrada HTTP.

Exemplo de rejeição definitiva:

```json
{
  "transactionId": "a991bcfb-4f58-4e49-81c0-c144a6f7a6a9",
  "status": "REJECTED",
  "balance": { "amount": "20.00", "currency": "BRL" },
  "failureCode": "INSUFFICIENT_FUNDS",
  "idempotentReplay": false
}
```

| `failureCode` | Motivo |
| --- | --- |
| `INSUFFICIENT_FUNDS` | Aposta acima do saldo disponível |
| `REVERSAL_INSUFFICIENT_FUNDS` | Rollback de um crédito sem saldo para o débito |
| `WALLET_MISMATCH` | Jogador/carteira incompatíveis |
| `CURRENCY_MISMATCH` | Moeda diferente da carteira |
| `REFERENCE_NOT_FOUND` | Referência ainda indisponível após tentativas ou TTL |
| `REFERENCE_NOT_PROCESSED` | Referência terminou rejeitada ou falhou |
| `REFERENCE_MISMATCH` | Identidade, rodada, moeda ou valor de reversão incompatíveis |
| `INVALID_REFERENCE_KIND` | Tipo referenciado não permitido |
| `ALREADY_REVERSED` | A referência já recebeu uma reversão bem-sucedida |
| `BALANCE_OVERFLOW` | Crédito ultrapassaria o limite monetário |
| `BALANCE_LIMIT_EXCEEDED` | Falha ao aplicar a transição financeira do agregado |

Rejeições persistidas não mudam quando o saldo ou as referências mudam depois.
Corrigir uma operação definitiva exige outro ID externo e outra chave; erros de
formato rejeitados antes do aceite não consomem a chave.

## Enviar a mesma operação por SQS

Este fluxo representa um serviço interno confiável que já autenticou o provedor.
Não entregue a identidade de ingresso aos provedores externos. O consumidor não
aceita tokens OAuth no envelope; a fronteira de autorização é a identidade IAM
que envia à fila. Veja a limitação de enforcement do LocalStack em
[infra/README.md](infra/README.md).

Com as variáveis do exemplo HTTP ainda disponíveis:

```sh
eval "$(./scripts/broker-env.sh ingress)"
SQS_ENDPOINT=http://localhost:4567
QUEUE_URL=$(aws --endpoint-url "$SQS_ENDPOINT" --region us-east-1 --output text \
  sqs get-queue-url --queue-name wager-transactions.fifo --query QueueUrl)
MESSAGE_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
OCCURRED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
ENVELOPE=$(jq -n --arg id "$MESSAGE_ID" --arg at "$OCCURRED_AT" \
  --arg key "$BET_KEY" --argjson data "$BET_PAYLOAD" \
  '{messageId:$id,type:"WagerTransactionRequested",occurredAt:$at,
    data:($data + {idempotencyKey:$key})}')

aws --endpoint-url "$SQS_ENDPOINT" --region us-east-1 --output json sqs send-message \
  --queue-url "$QUEUE_URL" --message-body "$ENVELOPE" \
  --message-group-id "$WALLET_ID" --message-deduplication-id "$MESSAGE_ID"
```

A aposta já processada via HTTP ganha registro na inbox, sem novo débito. Para
exercitar uma nova entrega real do mesmo envelope, preserve `ENVELOPE` e troque
apenas `--message-deduplication-id` por outro UUID. A identidade durável da inbox
é o `messageId` dentro do envelope. Alterar o envelope mantendo esse ID gera
conflito e não movimenta a carteira.

Entrada: `wager-transactions.fifo`; DLQ: `wager-transactions-dlq.fifo`; eventos:
`wager-events.fifo`. O bootstrap configura redrive após cinco recebimentos,
visibility de 30 segundos e long polling de 10 segundos. Mensagens inválidas
seguem para a DLQ após tentativas; rejeições financeiras confirmadas são removidas
normalmente. O consumidor remove a mensagem somente depois do commit.

Para receber eventos, use `eval "$(./scripts/broker-env.sh events)"`, resolva a
fila `wager-events.fifo` com `get-queue-url` e use `receive-message`.
O envelope de saída tem `eventId`, `eventType`, `aggregateId`, `correlationId`,
`causationId` opcional, `occurredAt`, `version: 1` e `data` tipado. Consumidores
externos precisam deduplicar pelo `eventId` persistente.

## Migrations e execução no host

A migration é executada automaticamente antes da API. Aplicação manual:

```sh
make migrate-up
```

A reversão remove as tabelas financeiras e seus dados. Pare todas as instâncias
antes de executá-la; use somente no ambiente que pretende apagar:

```sh
docker compose --profile multi stop app app2 app3
make migrate-down
make migrate-up
```

Para apagar também os dados do Keycloak e os volumes locais, o comando é
`docker compose --profile multi down --volumes`. Isso apaga carteiras, ledger,
realm e credenciais provisionadas; não é um procedimento de recuperação.

Para executar a API no host, mantendo somente as dependências em Docker:

```sh
make deps
set -a
. ./.env
set +a
eval "$(./scripts/broker-env.sh app)"
DATABASE_URL="$MIGRATION_DATABASE_URL" go run ./cmd/migrate up
go run ./cmd/wager-api
```

Se a API em Docker já estiver usando `8080`, pare-a ou defina outro `HTTP_ADDR`.
O binário não carrega `.env` automaticamente. O Compose lê `.env` para sua
interpolação e usa explicitamente os endereços internos da rede.

| Configuração | Padrão local / finalidade |
| --- | --- |
| `HTTP_ADDR` | `:8080` |
| `DATABASE_URL` | Runtime `wager` no host `localhost:55432`, banco `wager` |
| `MIGRATION_DATABASE_URL` | Owner local `wager_owner`; usado nos comandos de migration |
| `OIDC_ISSUER_URL` | `http://localhost:8081/realms/jungle` |
| `OIDC_JWKS_URL` | Mesmo issuer + `/protocol/openid-connect/certs`; interno no Compose |
| `OIDC_AUDIENCE` | `wagering-api` |
| `AWS_REGION` | `us-east-1` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Geradas no emulador; carregadas pelo helper/entrypoint |
| `SQS_ENDPOINT` | `http://localhost:4567`; vazio usa resolução normal AWS |
| `SQS_QUEUE_NAME`, `SQS_EVENT_QUEUE_NAME`, `SQS_DLQ_NAME` | Filas descritas acima |
| `WORKERS_ENABLED` | `true`; chave geral dos workers |
| `CONSUMER_ENABLED`, `PUBLISHER_ENABLED`, `REFERENCE_WORKER_ENABLED` | `true`; controles independentes do binário |
| `REFERENCE_MAX_ATTEMPTS`, `REFERENCE_TTL` | `8` e `24h`; vence o primeiro limite atingido |
| `REFERENCE_POLL_INTERVAL`, `OUTBOX_POLL_INTERVAL` | `1s` e `500ms` |
| `SQS_WAIT_SECONDS`, `SQS_VISIBILITY_SECONDS` | `10` e `30` |
| `PROCESS_TIMEOUT`, `SHUTDOWN_TIMEOUT` | `10s` e `20s` |

O visibility timeout deve superar `PROCESS_TIMEOUT` por **mais de cinco
segundos**; shutdown deve superar o timeout de processamento. A configuração é
validada no início. Não use credenciais ou HTTP de desenvolvimento em produção.

## Testes e simulações de falha

Sem containers, execute os testes unitários, o detector de corrida e a análise
estática:

```sh
go test ./...
go test -race ./...
go vet ./...
```

A integração usa infraestrutura real e fica atrás da build tag `integration`:

```sh
make deps
export TEST_DATABASE_URL=postgres://wager_owner:wager-owner-local@localhost:55432/wager?sslmode=disable
go test -race -tags=integration -count=1 -timeout=10m -v ./...
# Equivalente para a preparação e execução padrão:
make integration
```

A suíte distribuída cria um banco e filas isolados, compila o executável com
`-race` e inicia **três processos de sistema operacional**, cada um com seu pool
e memória. Ela remove os recursos de teste ao final. O owner local tem
`CREATEDB` somente para essa preparação. Para portas/serviços personalizados,
use `TEST_DATABASE_URL` nos testes de storage e `INTEGRATION_DATABASE_URL`,
`INTEGRATION_OIDC_ISSUER_URL` e `INTEGRATION_SQS_ENDPOINT` na suíte distribuída.
Os testes de storage criam schemas isolados e cobrem migrations, constraints,
imutabilidade e rollback financeiro quando a inserção da outbox falha. Sem
`TEST_DATABASE_URL`, esses testes de storage são explicitamente ignorados;
`make integration` configura a conexão local para executá-los.

Os cenários incluem autenticação real e expiração, isolamento de provedores,
50 reenvios da mesma aposta, duas apostas de `80.00` sobre `100.00`, avanço de
carteira independente de outra bloqueada, referências fora de ordem,
reversões, paginação, replay cruzando HTTP/SQS, retomada após reinício e DLQ.
Os testes consultam o ledger e reconciliam o resultado financeiro.

`TestRuntimeOutages` verifica indisponibilidade temporária em recursos isolados:
encerra as conexões do usuário da aplicação no banco de teste e injeta respostas
503 por um proxy em frente ao LocalStack real. Confere ausência de escrita
parcial, retry com a mesma chave, tentativas persistidas na outbox e recuperação
de mensagens/eventos quando as dependências voltam.

Para concentrar a execução nas janelas de interrupção:

```sh
go test -race -tags=integration -count=1 -timeout=10m -v \
  -run 'TestDistributedSystem/(committed_inbox|outbox_recovers)' ./tests/integration
```

`after_inbox_commit` encerra o consumidor entre commit e `DeleteMessage`.
`after_outbox_send` encerra o publisher entre envio e marcação da outbox.
Esses pontos só funcionam com `ENABLE_TEST_FAILPOINTS=true` e
`TEST_FAILPOINT` correspondente; encerram o processo com código `86`.
A suíte também interrompe a API depois do commit e antes de iniciar publishers.
Os processos substitutos devem recuperar o trabalho e preservar identidades.

Para explorar manualmente três instâncias usando o mesmo banco e broker:

```sh
make up-multi
curl --fail http://localhost:8080/health/ready
curl --fail http://localhost:8082/health/ready
curl --fail http://localhost:8083/health/ready
docker compose restart app
```

Os testes de integração são a evidência reproduzível das garantias; subir três
contêineres por si só não demonstra a ausência de duplicidade.

## Diagnóstico

`GET /health/live` informa vida do processo. `GET /health/ready` verifica
PostgreSQL e as três filas SQS. O JWKS é verificado no início; indisponibilidade
posterior do IdP pode impedir renovação de chaves sem derrubar liveness.

`GET /metrics` expõe métricas no formato Prometheus: resultados, replays,
retries, conflitos, falhas de armazenamento, mensagens na DLQ, atraso da outbox,
latência de processamento e divergências de reconciliação. Os contadores são
por processo e reiniciam com ele. Health e métricas são públicos; proteja a
exposição de métricas na infraestrutura de produção.

Os logs JSON incluem os identificadores disponíveis (`correlationId`,
`messageId`, `transactionId`, `walletId`, `providerId`) sem tokens ou corpos
financeiros completos. Inspecione `docker compose logs app localstack keycloak`
quando um serviço não ficar pronto. O README apresenta comandos e resultados
esperados dos exemplos; não substitui a execução dos testes no ambiente de entrega.
