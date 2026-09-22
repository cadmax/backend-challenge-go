# Infraestrutura local

`docker compose up --build -d --wait` cria PostgreSQL, Keycloak, LocalStack, aplica
migrations e inicia a API. `docker compose --profile multi up --build -d --wait`
inicia três processos independentes em `8080`, `8082` e `8083`. As portas são
publicadas somente em `127.0.0.1`; PostgreSQL usa `55432`, Keycloak `8081` e
LocalStack `4567` para evitar colisões comuns com ambientes já existentes.
Os nomes e portas internos da rede Docker continuam `postgres:5432`,
`keycloak:8080` e `localstack:4566`.

As imagens têm versões fixas: Go 1.26.1, Alpine 3.22.1, PostgreSQL 17.6,
Keycloak 26.3.3 e LocalStack 4.7.0. O contêiner da aplicação executa como UID
10001, sem capabilities, com filesystem somente leitura. Migrations usam
`wager_owner`; a aplicação usa `wager`, sem superuser ou permissão de DDL.
As permissões de tabela/coluna estão nas migrations. O banco do Keycloak tem
usuário próprio. `docker compose down` preserva o volume PostgreSQL.
O owner local tem `CREATEDB` para os bancos isolados dos testes de integração;
essa permissão não é necessária para o usuário de migrations em produção.

## OAuth e identidades

O arquivo `keycloak/jungle-realm.json` é importado automaticamente no primeiro
início. O realm é `jungle`, a audiência `wagering-api`, o issuer
`http://localhost:8081/realms/jungle`. Dentro da rede Docker a API busca as chaves
em `http://keycloak:8080/realms/jungle/protocol/openid-connect/certs`, mas mantém
a validação do issuer externo exato. Alterar `KEYCLOAK_PORT` no `.env` atualiza
o hostname do IdP e o issuer usado pelos contêineres.

| Client ID | Segredo de desenvolvimento | Papel | `provider_id` |
| --- | --- | --- | --- |
| `provider-a` | `local-provider-a-secret` | `wager:provider` | `provider-a` |
| `provider-b` | `local-provider-b-secret` | `wager:provider` | `provider-b` |
| `internal-service` | `local-internal-secret` | `wallet:internal` | ausente |
| `provider-expired` | `local-provider-expired-secret` | `wager:provider` | `provider-a` |

Todos os clientes usam somente `client_credentials`. `provider_id` é um claim
fixo configurado no IdP, não um atributo fornecido pelo chamador. Os tokens
normais duram 300 segundos; `provider-expired` dura dois segundos para os testes
de expiração. `./scripts/token.sh provider-a` imprime um access token.
O console administrativo local usa `admin` / `local-admin-secret`.

O Keycloak mantém o realm no PostgreSQL e não sobrescreve um realm existente
durante nova importação. Para aplicar mudanças em um ambiente local já criado,
altere o realm pela administração ou recrie conscientemente os volumes de
desenvolvimento. A remoção de volumes também apaga carteiras e ledger.

## SQS e políticas

O hook `localstack/init.sh` executa `localstack/provision.py` após o broker ficar
disponível. Provisiona três filas FIFO e três identidades IAM com políticas
versionadas em `localstack/policies/`:

| Identidade | Permissões |
| --- | --- |
| `wager-ingress` | Enviar somente em `wager-transactions.fifo` e consultar seus atributos |
| `wager-app` | Receber/remover/liberar visibilidade na entrada; publicar em `wager-events.fifo`; consultar atributos das três filas |
| `wager-events` | Receber/remover/liberar visibilidade somente na fila de saída |

As credenciais são criadas pela API IAM e gravadas no volume
`broker-credentials`, sem impressão nos logs nem arquivos versionados. O
entrypoint carrega `/run/wager-aws/app.env` antes de iniciar a API.
`eval "$(./scripts/broker-env.sh ingress)"` carrega no host a identidade de envio;
substitua `ingress` por `app` ou `events` para os demais papéis.

A fila de entrada tem visibility timeout de 30 segundos, long polling de
10 segundos e redrive para `wager-transactions-dlq.fifo` após cinco recebimentos
sem remoção. A DLQ retém mensagens por 14 dias e aceita redrive somente da fila
de entrada; entrada e saída retêm mensagens por quatro dias. A deduplicação por
conteúdo está desabilitada: o produtor define explicitamente o deduplication ID.
O `MessageGroupId` de entrada é o ID da carteira; o `MessageDeduplicationId` é
um identificador da entrega. Na saída, são respectivamente o ID do agregado e
o ID estável do evento. Não use a chave financeira como identidade de entrega
nos testes de duplicidade, pois isso esconderia a deduplicação da aplicação.

O endpoint strategy `dynamic` devolve URLs adaptadas ao hostname da chamada,
permitindo acesso tanto de `localhost:4567` quanto de `localstack:4566` sem
reescrita manual de URLs. Exemplo de inspeção sem AWS CLI no host:

```sh
docker compose exec localstack awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --attribute-names All
```

### Fronteira de confiança e limites

A fila de entrada pertence a um serviço interno de ingresso confiável. Provedores
externos utilizam HTTP autenticado; não recebem credenciais `wager-ingress`.
Esse serviço interno deve validar o token do provedor, derivar `providerId` da
identidade e só então produzir a mensagem. O consumidor valida o domínio,
mas uma identidade IAM de ingresso tem autoridade para representar os provedores.
Distribuir essa credencial para provedores permitiria falsificar `providerId`.

O LocalStack Community 4.7 reproduz SQS e provisiona IAM, mas não oferece a
mesma imposição de políticas IAM da AWS. As políticas e credenciais locais
demonstram o provisionamento; um teste de acesso negado no emulador não comprova
isolamento em produção. Para homologar a autorização do broker, use SQS real
com roles IAM equivalentes ou uma edição do emulador com enforcement habilitado.
Não exponha o endpoint local à rede. As credenciais `test` são usadas somente
pelo bootstrap/inspeção administrativa do emulador.

O LocalStack deste ambiente não persiste mensagens após recriação do contêiner.
Reinícios usados nos testes de recuperação reiniciam a aplicação e preservam
o broker; a durabilidade da outbox reside no PostgreSQL. Uma fila de saída já
consumida não pode ser reconstruída automaticamente depois de apagar o broker.
Credenciais IAM são rotacionadas ao iniciar o LocalStack; reinicie os processos
da aplicação em seguida para recarregá-las. HTTPS, backups, rotação de segredos,
IdP em modo de produção e infraestrutura AWS ficam fora deste Compose local.

Referências oficiais: [containers Keycloak](https://www.keycloak.org/server/containers),
[IAM LocalStack](https://docs.localstack.cloud/aws/services/iam/),
[enforcement IAM LocalStack](https://docs.localstack.cloud/aws/capabilities/security-testing/iam-policy-enforcement/).
