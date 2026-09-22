# Testar o fluxo completo no Postman

Esta coleção reúne **127 requests em 13 pastas**. Cada contexto cria seus dados,
salva os IDs necessários e confere as respostas. Os arquivos são templates:
tokens e identificadores de execução começam vazios; as credenciais incluídas
são apenas os exemplos públicos do ambiente Docker local.

## Importar e executar

1. Prepare o `.env` conforme o [README principal](../README.md) e execute
   `make up-multi` na raiz do projeto. Aguarde as três APIs ficarem saudáveis.
2. No Postman Desktop, use **Import** para carregar a
   [coleção](backend-challenge-go.postman_collection.json) e o
   [ambiente](backend-challenge-go.local.postman_environment.json).
3. Selecione **Backend Challenge Go — Local**. Ajuste `base_url`, `replica_2_url`,
   `replica_3_url`, `oidc_url` e `sqs_endpoint` se modificou as portas locais.
4. Abra **Run** na coleção. Execute **uma iteração**, mantendo a ordem de **00** a
   **12**, sem desmarcar requests. Se sua configuração limita o tempo dos scripts,
   permita pelo menos 60 segundos para os disparos concorrentes.
5. Confira os resultados dos testes e as respostas no Runner. Referências, outbox
   e DLQ repetem consultas até concluir ou atingir um limite; por isso, a quantidade
   final de verificações varia entre execuções.

O Postman documenta a [importação de arquivos](https://learning.postman.com/docs/getting-started/importing-and-exporting/importing-data/)
e o [Collection Runner](https://learning.postman.com/docs/tests-and-scripts/running-collections/intro-to-collection-runs/).

## Contextos e ordem

| Pasta | Requests | O que verificar |
| --- | ---: | --- |
| 00 — Saúde e preparação | 4 | Nova identificação de execução e readiness das três APIs |
| 01 — OAuth e identidades | 5 | Descoberta OIDC e tokens das identidades locais |
| 02 — Carteiras e abertura | 7 | Saldo zero, crédito inicial e abertura duplicada |
| 03 — Apostas, ganhos e extrato | 11 | BET, WIN, LOSS, consultas e paginação do ledger |
| 04 — Idempotência e saldo histórico | 11 | Replays, conflitos e resultado financeiro original |
| 05 — Reembolsos e rollbacks | 13 | Reversões integrais e rejeições sem movimento |
| 06 — Referências fora de ordem | 13 | Retomada pelo worker e referências inválidas |
| 07 — Autenticação e autorização | 13 | Tokens, papéis e isolamento entre provedores |
| 08 — Validação e erros de negócio | 21 | Dinheiro, payloads, headers, moedas e recursos inexistentes |
| 09 — Concorrência entre três instâncias | 10 | Duplicatas paralelas, disputa de saldo e carteiras independentes |
| 10 — SQS, inbox e outbox | 13 | Consumo, reentrega, replay HTTP e eventos publicados |
| 11 — Retry e dead-letter queue | 4 | Mensagem inválida, tentativas e chegada à DLQ |
| 12 — Observabilidade e fechamento | 2 | Métricas e readiness final |

Respostas `400`, `401`, `403`, `404`, `409` e `422` são esperadas nos cenários
negativos. O sucesso do cenário depende de suas assertions, não apenas de receber
um HTTP `2xx`.

## Executar uma pasta ou usar Send

Execute as pastas **00** e **01** antes do contexto desejado. Em seguida, execute
a pasta inteira desde sua primeira request: consultas e operações posteriores
usam a carteira, as referências e os IDs criados nessa sequência. Uma nova
execução cria novos IDs e preserva o histórico anterior.

As pastas **10** e **11** resolvem as URLs reais com `GetQueueUrl` antes das
requests SQS. Isso também funciona com **Send**, sem depender das requests
"Resolver fila". A resolução da fila não substitui os pré-requisitos financeiros:
para encontrar os eventos da outbox, primeiro crie e processe a aposta da pasta
**10**, que registra `sqs_transaction_id`.

No uso manual, repita as consultas enquanto o processamento estiver pendente.
`setNextRequest` controla a sequência no Runner; não repete uma chamada feita
com **Send**. [Documentação do fluxo de execução](https://learning.postman.com/docs/tests-and-scripts/running-collections/building-workflows/)

Os tokens normais expiram em **300 segundos**. Se uma request positiva retornar
`401` após uma pausa, execute novamente as requests de token da pasta **01**.
O cliente `provider-expired` tem token de **2 segundos** e existe para o cenário
que verifica a rejeição de token expirado.

## Auditoria e preservação dos dados

Use as requests de ledger e reconciliação ao final dos contextos para conferir
lançamentos, saldo calculado, saldo armazenado e diferença zero. Na pasta **10**,
compare os eventos `WagerTransactionProcessed` e `WalletBalanceChanged` com a
transação e a versão da carteira. As métricas da pasta **12** são agregadas por
processo e ajudam a observar retries e processamento; a trilha financeira está
no ledger.

A coleção não executa `PurgeQueue`. Os eventos inspecionados são liberados pela
request seguinte; no uso manual, execute "Liberar eventos inspecionados" até
liberar todos os recibos. Se interromper a leitura, a visibilidade retorna após
120 segundos. Na DLQ, somente a mensagem inválida identificada como pertencente
à execução atual é removida; outras mensagens lidas têm sua visibilidade devolvida.

O destino SQS é o **LocalStack**, com as credenciais locais `test/test`. A coleção
mantém as carteiras e o ledger para auditoria. Testes de interrupção de processos,
indisponibilidade e constraints SQL ficam em `make integration`; o Runner não
substitui essa suíte. Para trabalhar pelo terminal, use os
[exemplos com curl e scripts](../README.md#exemplo-completo-pela-api).
