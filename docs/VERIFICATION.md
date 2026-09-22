# Verificação da entrega

Execução local em **22/09/2026**, com Go **1.26.7**, host macOS/ARM64 e containers
Linux: PostgreSQL 17.6, Keycloak 26.3.3 e LocalStack 4.7.0. Os resultados abaixo
foram obtidos depois da revisão de legibilidade; não representam benchmark.

## Comandos executados

| Comando | Resultado |
| --- | --- |
| `go test ./...` | Passou |
| `go test -race ./...` | Passou |
| `go vet ./...` | Passou |
| `./scripts/integration.sh` | Passou, incluindo PostgreSQL e suíte distribuída |
| `go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...` | Nenhuma vulnerabilidade encontrada |
| `docker compose build migrate` | Binários API/migration compilados na imagem Go 1.26.7 |
| `docker compose run --rm migrate /app/migrate down` e `up` | Reversão e reaplicação concluídas |
| `docker compose --profile multi up --build -d --wait` | Três APIs e todas as dependências saudáveis |
| `git diff --check` | Sem problemas de whitespace |

`./scripts/integration.sh` exporta a conexão de teste e executa
`go test -race -tags=integration -count=1 -timeout=10m ./...`. Assim, os testes
PostgreSQL também executam; não são omitidos por ausência de configuração.
A suíte de processos e indisponibilidade terminou em **57.864 segundos** nessa
execução. O tempo depende da máquina, do estado das imagens e do broker.

## Revalidação após adoção das bibliotecas

UUID, configuração, Prometheus, migrations, JWKs e instrumentação HTTP passaram
a usar bibliotecas específicas. Depois dessas trocas, a integração completa com
`-race` passou novamente; a suíte distribuída terminou em **61.914 segundos**.
`go test ./...`, `go vet ./...` e `govulncheck` também passaram.

A imagem foi reconstruída e atualizada sobre o banco existente, sem `down` de
migration nem reset de volume. Com as APIs temporariamente paradas, snapshots
ordenados das tabelas financeiras e de mensageria foram comparados antes e depois
do `migrate up`: conteúdo idêntico, incluindo **34 carteiras, 112 transações,
87 lançamentos, 3 registros de inbox e 201 eventos de outbox**. O engine adotou
`version=1, dirty=false` e as três APIs voltaram saudáveis.

A [coleção versionada](../postman/backend-challenge-go.postman_collection.json)
foi executada com o [ambiente local](../postman/backend-challenge-go.local.postman_environment.json)
contra essa versão atualizada usando Newman 6.2.1. Foram **464 assertions, sem
falhas**, em **43.574 segundos**. Houve 448 chamadas HTTP, incluindo disparos
concorrentes, descoberta automática das filas e polling. Esses números variam
com a quantidade de eventos pendentes e o tempo de processamento; a coleção
contém 127 requests organizadas em 13 pastas.

## Evidência por garantia

| Garantia | Verificação |
| --- | --- |
| Precisão monetária | Parsing, escala, limites `int64`, operações comparadas com `big.Int`, moeda e JSON inválidos |
| Idempotência | 50 chamadas concorrentes em três processos resultam em um débito; replay mantém o saldo original |
| Saldo não negativo | Duas apostas de 80.00 contra 100.00: uma processada, uma rejeitada, saldo 20.00 |
| Paralelismo por carteira | Uma linha bloqueada tem escritor realmente aguardando; outra carteira conclui sua operação |
| Ledger e atomicidade | Constraints, imutabilidade, reconciliação e falha na inserção da outbox que desfaz todo o commit |
| HTTP/SQS equivalentes | Reentregas com deduplication IDs distintos chegam à inbox e não debitam novamente |
| Referências duráveis | Pendências sobrevivem ao reinício de todos os processos; resolução posterior e expiração |
| Reversões | Exclusividade entre refund/rollback, referência incompatível e código próprio para rollback sem saldo |
| OAuth real | Keycloak emite tokens; ausência, token inválido/expirado, isolamento e permissões são exercitados |
| Inbox e crash | Saída forçada com código 86 depois do commit e antes do ack; substituto consome sem novo débito |
| Outbox e crash | Retomada depois do commit e depois do envio sem marcação, com o mesmo `eventId` |
| Retry e DLQ | Mensagem inválida é recebida repetidamente e chega à DLQ real |
| PostgreSQL indisponível | Sessões somente do banco isolado são encerradas; 503 sem escrita parcial, recuperação com a mesma chave |
| SQS indisponível | Proxy injeta 503 diante do LocalStack real; outbox mantém tentativas e publica após recuperação |
| Fx e shutdown | Grafo válido, processos iniciam/encerram, logs confirmam workers terminados e conexões runtime liberadas |

Os testes estão em [domain](../internal/domain),
[auth](../internal/auth), [httpapi](../internal/httpapi),
[store_integration_test.go](../internal/postgres/store_integration_test.go) e
[tests/integration](../tests/integration).

## Legibilidade e limites

A revisão separou os loops de mensageria por responsabilidade, deu nomes
explícitos às etapas do caso de uso, alinhou SQL com seus parâmetros e removeu
logging duplicado. Outra revisão conferiu a preservação dos locks, commits,
contextos e garantias antes da execução completa dos testes.

Não houve teste de carga, avaliação de IAM em AWS real ou auditoria de produção
dos containers. O LocalStack Community não prova enforcement IAM e não mantém
mensagens quando recriado. A varredura financeira das constraints tem custo
proporcional ao histórico da carteira. Essas limitações e a política de `FAILED`
estão descritas em [ARCHITECTURE.md](../ARCHITECTURE.md).
