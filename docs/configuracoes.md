# Configurações do Apache Kafka

Referência prática dos principais parâmetros de configuração de **brokers**, **topics**, **producers**, **consumers**, **retries**, **retenção**, **compressão**, **batches**, **ISR**, **DLQ** e **Protobuf/Schema Registry**.

A ideia deste documento não é decorar todas as propriedades do Kafka, mas entender **quais botões existem**, **qual problema cada um resolve** e **quais trade-offs aparecem em produção**.

---

## Visão geral das camadas de configuração

Kafka tem configurações em várias camadas. Algumas vivem no broker, outras no topic, outras no producer e outras no consumer.

```text
  ┌──────────────────────────────────────────────────────────────┐
  │ Kafka Cluster                                                │
  │                                                              │
  │  Broker config                                               │
  │  ├── listeners                                               │
  │  ├── log dirs                                                │
  │  ├── defaults de retenção                                    │
  │  ├── defaults de replication factor                          │
  │  └── configs de segurança                                    │
  │                                                              │
  │  Topic config                                                │
  │  ├── partitions                                              │
  │  ├── replication.factor                                      │
  │  ├── retention.ms / retention.bytes                          │
  │  ├── cleanup.policy                                          │
  │  └── min.insync.replicas                                     │
  │                                                              │
  └──────────────────────────────────────────────────────────────┘
             ▲                                      ▲
             │                                      │
             │                                      │
  ┌─────────────────────┐              ┌─────────────────────┐
  │ Producer config     │              │ Consumer config     │
  │ ├── acks            │              │ ├── group.id         │
  │ ├── retries         │              │ ├── auto.offset.reset│
  │ ├── compression     │              │ ├── enable.auto.commit│
  │ ├── batch.size      │              │ ├── max.poll.records │
  │ ├── linger.ms       │              │ └── session.timeout  │
  │ └── idempotence     │              └─────────────────────┘
  └─────────────────────┘
```

### Regra mental

| Camada | Pergunta que ela responde |
|---|---|
| **Broker** | Como o cluster opera e se comunica? |
| **Topic** | Como os dados daquele fluxo serão armazenados e replicados? |
| **Producer** | Como publicar com segurança, latência e throughput adequados? |
| **Consumer** | Como ler, commitar offsets, lidar com lentidão e rebalanceamento? |
| **Schema Registry/Protobuf** | Como garantir contrato entre quem publica e quem consome? |

---

## Configurações de Broker

O **broker** é o servidor Kafka. Suas configurações definem identidade, rede, armazenamento, segurança e valores padrão usados pelos topics.

```properties
broker.id=1                         # ZooKeeper mode
node.id=1                           # KRaft mode
process.roles=broker,controller     # KRaft single node/lab
log.dirs=/var/lib/kafka/data
num.partitions=3
advertised.listeners=PLAINTEXT://broker1:9092
```

### `broker.id` / `node.id`

Identifica unicamente o broker dentro do cluster.

```text
  Cluster Kafka

  Broker 1  →  node.id=1
  Broker 2  →  node.id=2
  Broker 3  →  node.id=3
```

Em versões modernas com **KRaft**, `node.id` substitui o papel clássico de `broker.id` em muitos cenários.

---

## Listeners e advertised listeners

Uma das configurações que mais causa problema em Kafka é `advertised.listeners`.

### Diferença simples

| Config | Função |
|---|---|
| `listeners` | Onde o broker realmente escuta conexões |
| `advertised.listeners` | Endereço que o broker anuncia para os clients usarem |

```properties
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://broker1.company.local:9092
```

```text
  Producer
     │
     │ 1. conecta no bootstrap
     ▼
  Broker
     │
     │ 2. retorna metadata:
     │    "para falar comigo, use broker1.company.local:9092"
     ▼
  Producer passa a conectar em broker1.company.local:9092
```

Se o `advertised.listeners` estiver errado, o client pode até conectar no bootstrap, mas depois falhar ao tentar falar com o broker correto.

### Exemplo em Docker/local

```properties
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://localhost:9092
```

### Exemplo em rede interna/Kubernetes

```properties
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://kafka-0.kafka-headless.default.svc.cluster.local:9092
```

> Em produção, o endereço anunciado precisa ser alcançável pelo producer/consumer real. Às vezes funciona no `nc host porta`, mas quebra no client Kafka porque o broker devolve outro host na metadata.

---

## Configurações padrão de Topic no Broker

O broker pode definir defaults para topics criados sem configuração explícita.

```properties
num.partitions=3
default.replication.factor=3
log.retention.hours=168
log.segment.bytes=1073741824
```

| Config | Significado |
|---|---|
| `num.partitions` | Número padrão de partições para novos topics |
| `default.replication.factor` | Número padrão de réplicas |
| `log.retention.hours` | Retenção padrão por tempo |
| `log.retention.bytes` | Retenção padrão por tamanho |
| `log.segment.bytes` | Tamanho máximo de cada segmento de log |

### Importante

Configuração no broker é **default**. Configuração no topic é **específica** e geralmente prevalece.

```text
  Broker default:
    log.retention.hours=168

  Topic payments:
    retention.ms=259200000  # 3 dias

  Resultado:
    payments retém por 3 dias, não 7
```

---

## Configurações de Topic

O topic é onde você decide a maior parte do comportamento de armazenamento: partições, replicação, retenção, compactação e disponibilidade.

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic payments \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2
```

---

## Partições: `partitions`

Partições definem paralelismo, ordenação e distribuição.

```text
  Topic: fraud-events

  partitions=1       → baixa concorrência, ordem total no topic
  partitions=6       → maior paralelismo, ordem só por partição/chave
  partitions=24      → muito paralelismo, mais custo operacional
```

### Trade-offs

| Mais partições | Menos partições |
|---|---|
| Mais paralelismo | Menos overhead |
| Mais consumers ativos por group | Mais simples de operar |
| Mais arquivos/segmentos no cluster | Menos rebalanceamento complexo |
| Ordem apenas por chave/partição | Pode manter ordem mais previsível |

### Regra prática

- Se precisa de **ordem por entidade**, use uma chave estável: `user_id`, `account_id`, `cnpj`, `event_id`, etc.
- Se precisa de **throughput**, aumente partições com cuidado.
- Não aumente partições sem pensar no particionamento, porque isso pode mudar o roteamento de chaves.

```text
  Antes: 3 partições
  hash(user-1) % 3 = Partition 0

  Depois: 6 partições
  hash(user-1) % 6 = Partition 4

  A mesma chave pode passar a cair em outra partição após aumento.
```

---

## Replication factor

`replication.factor` define quantas cópias existem de cada partição.

```text
  replication.factor=3

  Partition 0
  ├── Leader   → Broker 1
  ├── Follower → Broker 2
  └── Follower → Broker 3
```

### Valores comuns

| Ambiente | Valor comum |
|---|---|
| Local/lab | `1` |
| Dev compartilhado | `2` ou `3` |
| Produção | `3` |
| Fluxo crítico | `3` ou mais, dependendo do cluster |

> Para produção, `replication.factor=3` costuma ser o ponto de partida: tolera a perda de um broker mantendo disponibilidade, desde que as demais configs estejam coerentes.

---

## ISR e `min.insync.replicas`

`min.insync.replicas` define o mínimo de réplicas sincronizadas exigidas para aceitar escrita quando o producer usa `acks=all`.

```properties
min.insync.replicas=2
```

```text
  replication.factor=3
  min.insync.replicas=2
  producer acks=all

  Estado normal:
    ISR = [Broker 1, Broker 2, Broker 3]
    Escrita aceita ✅

  1 follower caiu:
    ISR = [Broker 1, Broker 2]
    Escrita aceita ✅

  2 followers caíram:
    ISR = [Broker 1]
    Escrita rejeitada ❌
```

### Relação entre `acks` e `min.insync.replicas`

| Producer `acks` | Topic `min.insync.replicas` | Resultado |
|---|---:|---|
| `acks=1` | `2` | Producer só espera o leader; menor garantia |
| `acks=all` | `2` | Escrita só confirma com pelo menos 2 réplicas em sincronia |
| `acks=all` | `1` | Mais disponível, porém menos seguro |

### Config recomendada para fluxo crítico

```properties
# topic
replication.factor=3
min.insync.replicas=2

# producer
acks=all
enable.idempotence=true
```

---

## Retenção por tempo e tamanho

Kafka não guarda mensagens para sempre por padrão. Ele remove dados conforme política de retenção.

```properties
retention.ms=604800000       # 7 dias
retention.bytes=-1           # sem limite por tamanho
segment.ms=604800000         # rotação de segmento por tempo
segment.bytes=1073741824     # 1 GB por segmento
```

### `retention.ms`

Define por quanto tempo os dados ficam disponíveis.

| Valor | Significado |
|---|---|
| `86400000` | 1 dia |
| `259200000` | 3 dias |
| `604800000` | 7 dias |
| `-1` | Retenção infinita por tempo |

```text
  Topic: fraud-events

  offset 0 ─ msg antiga  ─┐
  offset 1 ─ msg antiga   │ passou retention.ms
  offset 2 ─ msg recente ─┘

  Kafka remove segmentos antigos quando elegíveis.
```

### `retention.bytes`

Define limite de tamanho por partição.

```properties
retention.bytes=10737418240  # 10 GB por partição
```

Se o topic tem 6 partições e `retention.bytes=10GB`, o topic pode ocupar aproximadamente até 60GB, sem contar réplicas.

```text
  6 partições x 10GB x replication.factor 3
  ≈ 180GB físicos no cluster
```

---

## Cleanup policy: delete vs compact

`cleanup.policy` define como o Kafka limpa dados antigos.

```properties
cleanup.policy=delete
```

ou:

```properties
cleanup.policy=compact
```

ou combinado:

```properties
cleanup.policy=compact,delete
```

### `delete`

Remove mensagens antigas por tempo/tamanho.

```text
  offset 0: event A  ← expira
  offset 1: event B  ← expira
  offset 2: event C  ← mantém
```

Bom para eventos históricos: pagamentos, login, fraude, auditoria, tracking.

### `compact`

Mantém a última mensagem por chave.

```text
  Antes:
    user-1 → ACTIVE
    user-2 → ACTIVE
    user-1 → BLOCKED

  Depois da compactação:
    user-2 → ACTIVE
    user-1 → BLOCKED
```

Bom para estado atual: configuração, perfil, cadastro, cache distribuído.

---

## Configurações de Producer

Producer é quem publica mensagens no Kafka. Suas configs equilibram **durabilidade**, **latência**, **throughput** e **risco de duplicidade**.

```properties
bootstrap.servers=broker1:9092,broker2:9092
acks=all
enable.idempotence=true
retries=2147483647
delivery.timeout.ms=120000
request.timeout.ms=30000
linger.ms=10
batch.size=32768
compression.type=zstd
```

---

## `acks`

`acks` define quando o broker confirma a escrita para o producer.

| `acks` | Confirmação | Uso típico |
|---|---|---|
| `0` | Não espera confirmação | Métrica descartável, altíssimo throughput |
| `1` | Espera apenas o leader | Baixa latência, risco moderado |
| `all` / `-1` | Espera ISR suficiente | Fluxo crítico, maior durabilidade |

```text
  Producer envia mensagem
       │
       ▼
  Leader recebe
       │
       ├── acks=1   → confirma aqui
       │
       ▼
  Followers replicam
       │
       └── acks=all → confirma após réplicas necessárias
```

Para eventos importantes, como fraude, pagamento, auditoria ou timeline crítica, prefira:

```properties
acks=all
enable.idempotence=true
```

---

## Idempotência do producer

`enable.idempotence=true` evita duplicidades causadas por retry do producer.

```properties
enable.idempotence=true
```

### Problema sem idempotência

```text
  1. Producer envia msg-123
  2. Broker grava msg-123
  3. Ack se perde na rede
  4. Producer acha que falhou
  5. Producer reenvia msg-123
  6. Kafka grava duplicado
```

### Com idempotência

O producer envia identificadores internos de sequência. O broker consegue perceber que aquilo é retry da mesma mensagem.

```text
  Producer ID: P1
  Sequence: 10

  Primeira escrita: aceita
  Retry da sequência 10: ignorado como duplicado
```

> Idempotência do producer reduz duplicidade no Kafka, mas não elimina a necessidade de idempotência no consumer/banco. Para isso, use `event_id` único e constraint/índice no destino.

---

## Retries do producer

Retries fazem o producer tentar novamente quando ocorre erro transitório.

```properties
retries=2147483647
retry.backoff.ms=100
delivery.timeout.ms=120000
request.timeout.ms=30000
```

### Configs importantes

| Config | Função |
|---|---|
| `retries` | Quantidade máxima de tentativas |
| `retry.backoff.ms` | Espera entre tentativas |
| `delivery.timeout.ms` | Tempo máximo total para entregar uma mensagem |
| `request.timeout.ms` | Tempo máximo aguardando resposta de uma request |

### Fluxo

```text
  Producer envia batch
       │
       ▼
  Falha transitória? timeout? leader mudou?
       │
       ├── Sim → retry
       │
       └── Não → sucesso

  Se passar delivery.timeout.ms:
       producer falha definitivamente aquela entrega
```

### Atenção

`retries` alto não significa retry infinito se `delivery.timeout.ms` for baixo.

```text
  retries=999999
  delivery.timeout.ms=30000

  Resultado:
    o producer só tenta durante aproximadamente 30s.
```

---

## Compressão

Compressão reduz tráfego de rede e uso de disco, mas consome CPU.

```properties
compression.type=snappy
```

Opções comuns:

| Tipo | Característica |
|---|---|
| `none` | Sem compressão |
| `gzip` | Alta compressão, mais CPU |
| `snappy` | Boa velocidade, compressão moderada |
| `lz4` | Muito rápido, bom equilíbrio |
| `zstd` | Compressão excelente, moderno, bom para alto volume |

### Como pensar

```text
  Sem compressão:
    mais bytes na rede/disco
    menos CPU

  Com compressão:
    menos bytes na rede/disco
    mais CPU no producer/broker/consumer
```

### Regra prática

| Cenário | Sugestão |
|---|---|
| Baixo volume/local | `none` ou `snappy` |
| Alto volume de eventos JSON | `lz4` ou `zstd` |
| Custo de rede/disco alto | `zstd` |
| CPU muito limitada | `snappy` ou `lz4` |

---

## Batches: `batch.size` e `linger.ms`

Producer não precisa enviar uma mensagem por request. Ele agrupa mensagens em batches por partição.

```properties
batch.size=32768
linger.ms=10
```

### `batch.size`

Tamanho máximo do batch por partição.

```text
  Producer buffer

  Partition 0 batch: [msg1, msg2, msg3]
  Partition 1 batch: [msg4, msg5]
  Partition 2 batch: [msg6]
```

### `linger.ms`

Tempo que o producer espera para tentar formar batches maiores.

```text
  linger.ms=0
    envia quase imediatamente
    menor latência
    batches menores

  linger.ms=10
    espera até 10ms
    maior chance de batch maior
    melhor throughput
```

### Trade-off

| Config | Efeito |
|---|---|
| `batch.size` maior | Melhor throughput, mais memória |
| `linger.ms` maior | Melhor compressão/batch, maior latência |
| `linger.ms=0` | Menor latência, menos eficiência |

---

## Buffer do producer

O producer mantém mensagens em memória antes de enviar.

```properties
buffer.memory=33554432      # 32 MB
max.block.ms=60000
```

| Config | Função |
|---|---|
| `buffer.memory` | Memória total para batches pendentes |
| `max.block.ms` | Quanto tempo `send()` pode bloquear esperando espaço/metadados |

```text
  App produz mais rápido do que Kafka aceita
       │
       ▼
  Buffer começa a encher
       │
       ├── ainda tem espaço → continua
       └── cheio → send() bloqueia até max.block.ms
```

Se o buffer enche com frequência, pode indicar:

- Kafka lento
- rede lenta
- broker indisponível
- producer gerando eventos demais
- batches grandes demais
- compressão pesada demais

---

## Tamanho máximo de mensagem

Kafka tem limites de tamanho em várias camadas.

### Producer

```properties
max.request.size=1048576    # 1 MB
```

### Broker

```properties
message.max.bytes=1048576
```

### Topic

```properties
max.message.bytes=1048576
```

### Consumer

```properties
fetch.max.bytes=52428800
max.partition.fetch.bytes=1048576
```

```text
  Producer aceita 5 MB
  Topic aceita 1 MB

  Resultado:
    mensagem de 5 MB falha no envio
```

> Kafka não é ideal para payloads gigantes. Para arquivos grandes, normalmente é melhor gravar em storage externo e publicar no Kafka apenas uma referência.

---

## Configurações de Consumer

Consumer é quem lê mensagens e controla offsets.

```properties
bootstrap.servers=broker1:9092,broker2:9092
group.id=fraud-timeline-consumer
enable.auto.commit=false
auto.offset.reset=earliest
max.poll.records=500
max.poll.interval.ms=300000
session.timeout.ms=45000
heartbeat.interval.ms=15000
```

---

## `group.id`

`group.id` identifica o grupo de consumo.

```text
  Mesmo topic: fraud-events

  group.id=timeline-service
    lê eventos para popular timeline

  group.id=audit-service
    lê os mesmos eventos para auditoria
```

Grupos diferentes têm offsets independentes.

---

## Offset reset: `auto.offset.reset`

Define o que fazer quando não existe offset commitado para o grupo.

```properties
auto.offset.reset=earliest
```

| Valor | Comportamento |
|---|---|
| `earliest` | Começa do início disponível |
| `latest` | Começa apenas das novas mensagens |
| `none` | Falha se não houver offset |

```text
  Topic já tem mensagens:

  offset 0  1  2  3  4  5

  Novo group.id com earliest → começa no 0
  Novo group.id com latest   → começa após o 5
```

---

## Auto commit vs commit manual

```properties
enable.auto.commit=true
```

Com auto commit, o consumer commita offsets periodicamente, mesmo que sua aplicação ainda não tenha terminado de processar de verdade.

```text
  Consumer lê offset 10
       │
       ├── auto commit offset 11
       │
       ▼
  App falha antes de salvar no banco
       │
       ▼
  Mensagem pode ser perdida do ponto de vista da aplicação
```

Para fluxos críticos, prefira commit manual:

```properties
enable.auto.commit=false
```

```text
  1. Consumer lê mensagem
  2. App valida
  3. App salva no banco ou manda para DLQ
  4. App commita offset
```

---

## `max.poll.records`

Define quantos registros o consumer pode receber por poll.

```properties
max.poll.records=500
```

```text
  poll()
    ├── retorna até 500 mensagens
    ├── aplicação processa
    └── depois chama poll() novamente
```

### Trade-off

| Valor alto | Valor baixo |
|---|---|
| Melhor throughput | Menor latência por lote |
| Mais memória | Menos risco de timeout |
| Mais tempo processando lote | Mais polls |

---

## `max.poll.interval.ms`

Define quanto tempo o consumer pode ficar sem chamar `poll()` antes de ser considerado travado.

```properties
max.poll.interval.ms=300000  # 5 minutos
```

```text
  Consumer faz poll
       │
       ▼
  Processa lote por muito tempo
       │
       ▼
  Se passar max.poll.interval.ms:
       Kafka remove consumer do grupo
       Rebalance acontece
```

Se seu processamento demora muito, você precisa:

- reduzir `max.poll.records`
- aumentar `max.poll.interval.ms`
- processar em paralelo com cuidado
- pausar/resumir partições
- usar batch menor no Broadway/consumer framework

---

## Heartbeat e session timeout

```properties
heartbeat.interval.ms=15000
session.timeout.ms=45000
```

| Config | Função |
|---|---|
| `heartbeat.interval.ms` | Frequência com que o consumer diz “estou vivo” |
| `session.timeout.ms` | Tempo até o grupo considerar o consumer morto |

```text
  Consumer ── heartbeat ──► Coordinator
  Consumer ── heartbeat ──► Coordinator
  Consumer X parou
       │
       ▼
  Passou session.timeout.ms
       │
       ▼
  Rebalanceamento
```

---

## Fetch configs

Controlam quanto o consumer busca por request.

```properties
fetch.min.bytes=1
fetch.max.wait.ms=500
fetch.max.bytes=52428800
max.partition.fetch.bytes=1048576
```

| Config | Função |
|---|---|
| `fetch.min.bytes` | Mínimo de bytes para o broker responder |
| `fetch.max.wait.ms` | Tempo máximo esperando acumular dados |
| `fetch.max.bytes` | Máximo total por fetch |
| `max.partition.fetch.bytes` | Máximo por partição |

```text
  fetch.min.bytes alto + fetch.max.wait alto
    → melhor throughput
    → maior latência

  fetch.min.bytes baixo
    → menor latência
    → mais requests
```

---

## Retry no Consumer

Retry no consumer é diferente de retry no producer.

Producer retry tenta **entregar no Kafka**.

Consumer retry tenta **processar algo que já está no Kafka**.

```text
  Producer retry:
    app → Kafka

  Consumer retry:
    Kafka → app → banco/API/outro serviço
```

---

## Tipos de erro no consumer

| Tipo | Exemplo | Ação comum |
|---|---|---|
| Transitório | banco fora, timeout HTTP, rede | retry e não commitar ainda |
| Permanente | JSON inválido, campo obrigatório ausente, contrato inválido | DLQ e commit |
| Negócio | CPF inválido, regra de domínio rejeitada | depende: DLQ, discard ou status de rejeição |

```text
  Consumer recebeu evento
       │
       ▼
  Decode JSON/Protobuf
       │
       ├── inválido permanente → DLQ → commit offset
       │
       ▼
  Validação de domínio
       │
       ├── inválido permanente → DLQ → commit offset
       │
       ▼
  Persistência no banco
       │
       ├── erro transitório → não commit → retry
       └── sucesso → commit offset
```

---

## DLQ — Dead Letter Queue

DLQ é um topic separado para mensagens que não puderam ser processadas corretamente.

```text
  Topic principal: fraud-events
       │
       ▼
  Consumer
       │
       ├── sucesso → banco → commit
       │
       ├── erro permanente → fraud-events-dlq → commit
       │
       └── erro transitório → sem commit → retry
```

### Payload recomendado para DLQ

```json
{
  "original_topic": "fraud-events",
  "original_partition": 2,
  "original_offset": 89123,
  "error_type": "validation_error",
  "error_message": "missing required field: event_id",
  "failed_at": "2026-07-04T18:00:00Z",
  "payload": { "...": "original message" }
}
```

### Por que commitar depois de mandar para DLQ?

Porque o erro é permanente. Se não commitar, o consumer vai ler a mesma mensagem inválida para sempre.

```text
  Mensagem inválida
       │
       ├── não manda DLQ
       ├── não commita
       └── reprocessa infinitamente ❌
```

Fluxo melhor:

```text
  Mensagem inválida
       │
       ├── publica na DLQ
       └── commita offset ✅
```

---

## Retry topics

Além de DLQ, alguns sistemas usam topics intermediários de retry.

```text
  fraud-events
       │
       ▼
  erro transitório
       │
       ▼
  fraud-events-retry-1m
       │
       ▼
  fraud-events-retry-10m
       │
       ▼
  fraud-events-dlq
```

Isso evita travar a partição principal com uma mensagem problemática.

### Trade-off

| Sem retry topic | Com retry topic |
|---|---|
| Mais simples | Mais complexo |
| Mantém ordem da partição | Pode quebrar ordem global de processamento |
| Pode travar consumo em erro persistente | Isola mensagens problemáticas |
| Menos tópicos | Mais tópicos e roteamento |

---

## Idempotência no Consumer

Mesmo com producer idempotente, o consumer pode processar a mesma mensagem mais de uma vez.

```text
  1. Consumer salva no banco
  2. App cai antes de commitar offset
  3. Kafka reentrega a mensagem
  4. Consumer tenta salvar de novo
```

Solução comum: `event_id` único.

```sql
CREATE UNIQUE INDEX user_fraud_events_event_id_idx
ON user_fraud_events(event_id);
```

Fluxo:

```text
  Recebe event_id=abc-123
       │
       ├── não existe no banco → insere
       └── já existe no banco → ignora como duplicado e commita
```

> Kafka normalmente entrega pelo menos uma vez. Quem dá segurança contra duplicidade no efeito final é a aplicação/destino.

---

## Segurança: SASL, SSL e ACL

Em produção, Kafka geralmente usa autenticação, criptografia e autorização.

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="API_KEY" \
  password="API_SECRET";
ssl.endpoint.identification.algorithm=https
```

### Protocolos comuns

| Config | Significado |
|---|---|
| `PLAINTEXT` | Sem TLS, sem autenticação |
| `SSL` | TLS/mTLS |
| `SASL_PLAINTEXT` | Autenticação SASL sem TLS |
| `SASL_SSL` | SASL com TLS |

### ACL/RBAC

Permissões típicas:

| Operação | Producer | Consumer |
|---|---:|---:|
| Write no topic | Sim | Não |
| Read no topic | Não | Sim |
| Describe topic | Sim | Sim |
| Read group | Não | Sim |
| Write DLQ | Às vezes | Sim, se consumer publica DLQ |

---

## Protobuf e Schema Registry

Protobuf define o formato binário/contrato da mensagem. Schema Registry ajuda a versionar e validar compatibilidade entre producer e consumer.

```text
  .proto
    │
    ├── gera código para producer
    ├── gera código para consumer
    └── registra schema no Schema Registry
```

Exemplo de envelope:

```proto
syntax = "proto3";

message FraudControlEvent {
  string event_id = 1;
  string operation = 2;
  string payload_content_type = 3;
  string payload_schema_version = 4;
  bytes payload = 5;
  int64 occurred_at_unix_ms = 6;
  string source = 7;
}
```

### Por que usar envelope?

Quando cada tipo de evento tem payload diferente, o envelope mantém o contrato externo estável.

```text
  Kafka message

  ┌─────────────────────────────────────────────┐
  │ event_id                                    │
  │ operation = "PIX_CREATED"                   │
  │ payload_content_type = "application/json"   │
  │ payload_schema_version = "v1"               │
  │ payload = bytes(JSON específico do PIX)      │
  │ occurred_at_unix_ms                         │
  │ source                                      │
  └─────────────────────────────────────────────┘
```

Assim, o consumer entende o envelope e decide como interpretar o payload interno.

---

## Compatibilidade de schema

Schema Registry pode bloquear alterações incompatíveis.

| Mudança | Geralmente saudável? | Observação |
|---|---|---|
| Adicionar campo novo com novo número | Sim | Em `proto3`, campos ausentes usam default |
| Remover campo sem reutilizar número | Com cuidado | Melhor reservar número/nome |
| Reutilizar número de campo | Não | Pode quebrar decode |
| Trocar tipo de campo | Perigoso | Pode quebrar compatibilidade |
| Renomear campo mantendo número | Pode ser ok no binário | Mas confunde código/documentação |

### Regra de ouro do Protobuf

> Nunca reutilize o número de um campo removido.

```proto
message Event {
  string event_id = 1;
  // string old_field = 2;  // removido

  reserved 2;
  reserved "old_field";
}
```

---

## Campos opcionais em Protobuf

Em `proto3`, campos escalares ausentes normalmente aparecem com valor default após decode.

```proto
message UserEvent {
  string name = 1;
  optional string nickname = 2;
}
```

Diferença conceitual:

```text
  Sem optional:
    nickname ausente  → ""
    nickname enviado "" → ""
    difícil saber se veio ou não

  Com optional:
    nickname ausente  → presença=false
    nickname enviado "" → presença=true, valor=""
```

Isso é útil quando a diferença entre “não enviado” e “enviado vazio” importa para o domínio.

---

## Producer + Schema Registry

Fluxo típico:

```text
  App monta struct/evento
       │
       ▼
  Serializa com Protobuf
       │
       ▼
  Serializer consulta/usa schema id
       │
       ▼
  Publica bytes no Kafka
```

```text
  Mensagem no Kafka pode carregar:

  ┌──────────────┬───────────────────────────────┐
  │ schema id    │ payload protobuf              │
  └──────────────┴───────────────────────────────┘
```

Dependendo da stack, o schema id vai no payload serializado pelo serializer Confluent, não como header manual.

---

## Consumer + Schema Registry

Fluxo típico:

```text
  Consumer lê bytes do Kafka
       │
       ▼
  Deserializer identifica schema id
       │
       ▼
  Busca schema se necessário
       │
       ▼
  Decodifica para struct/evento
       │
       ▼
  App processa
```

### Falhas possíveis

| Falha | Possível causa |
|---|---|
| Schema não encontrado | schema id inexistente ou registry indisponível |
| Decode inválido | bytes não batem com o schema esperado |
| Campo ausente | producer antigo ou campo opcional/default |
| Payload interno inválido | envelope válido, mas JSON interno inválido |

---

## Configuração sugerida por cenário

### 1. Evento crítico: fraude, pagamento, auditoria

```properties
# topic
partitions=6
replication.factor=3
min.insync.replicas=2
retention.ms=604800000
cleanup.policy=delete

# producer
acks=all
enable.idempotence=true
retries=2147483647
delivery.timeout.ms=120000
compression.type=zstd
linger.ms=10
batch.size=32768

# consumer
enable.auto.commit=false
auto.offset.reset=earliest
max.poll.records=500
max.poll.interval.ms=300000
```

Características:

- alta durabilidade
- boa compressão
- commit manual
- DLQ para erro permanente
- idempotência no banco via `event_id`

---

### 2. Métricas descartáveis / analytics de baixo risco

```properties
# topic
partitions=12
replication.factor=3
retention.ms=259200000
cleanup.policy=delete

# producer
acks=1
enable.idempotence=true
compression.type=lz4
linger.ms=50
batch.size=65536

# consumer
enable.auto.commit=true
auto.offset.reset=latest
max.poll.records=1000
```

Características:

- throughput alto
- alguma perda pode ser aceitável
- batches maiores
- menor custo operacional no consumer

---

### 3. Estado atual / cache / configuração

```properties
# topic
partitions=3
replication.factor=3
cleanup.policy=compact
min.cleanable.dirty.ratio=0.5

# producer
acks=all
enable.idempotence=true
compression.type=zstd
```

Características:

- mantém última mensagem por chave
- ideal para reconstruir estado
- chave é obrigatória para compactação fazer sentido

```text
  key=user-1, value={"status":"ACTIVE"}
  key=user-1, value={"status":"BLOCKED"}

  Após compactação:
    user-1 → BLOCKED
```

---

## Configuração para DLQ

```properties
# topic principal
fraud-events.partitions=6
fraud-events.replication.factor=3
fraud-events.retention.ms=604800000

# topic DLQ
fraud-events-dlq.partitions=6
fraud-events-dlq.replication.factor=3
fraud-events-dlq.retention.ms=2592000000  # 30 dias
```

### Por que DLQ pode ter retenção maior?

Porque ela serve para análise, correção e reprocessamento.

```text
  Topic principal:
    retenção curta/média para fluxo operacional

  DLQ:
    retenção maior para investigação
```

---

## Ordem, retry e DLQ

A ordem no Kafka é por partição. Retry e DLQ podem afetar a ordem observada pela aplicação.

```text
  Partition 0:

  offset 10: evento A
  offset 11: evento B com erro
  offset 12: evento C
```

### Se não commitar o B

```text
  A processa
  B falha
  C não avança

  Mantém ordem, mas pode travar a partição.
```

### Se mandar B para retry topic e commitar

```text
  A processa
  B vai para retry
  C processa

  Não trava a partição, mas C pode passar B.
```

### Escolha depende do domínio

| Domínio exige ordem estrita? | Estratégia |
|---|---|
| Sim | retry bloqueante ou particionamento mais específico |
| Não | retry topic/DLQ para não travar fluxo |
| Parcialmente | ordem por entidade usando key estável |

---

## Checklist de produção

### Topic

| Pergunta | Config relacionada |
|---|---|
| Quantos consumers paralelos preciso? | `partitions` |
| Posso perder broker sem perder dados? | `replication.factor` |
| Quero bloquear escrita se só sobrou 1 réplica? | `min.insync.replicas` |
| Quanto tempo preciso reprocessar? | `retention.ms` |
| É evento histórico ou estado atual? | `cleanup.policy` |

### Producer

| Pergunta | Config relacionada |
|---|---|
| Preciso de durabilidade forte? | `acks=all` |
| Quero evitar duplicidade por retry? | `enable.idempotence=true` |
| Preciso de throughput maior? | `batch.size`, `linger.ms`, `compression.type` |
| Quanto tempo posso esperar uma entrega? | `delivery.timeout.ms` |
| Mensagens são grandes? | `max.request.size` |

### Consumer

| Pergunta | Config relacionada |
|---|---|
| Posso commitar antes de processar? | `enable.auto.commit` |
| Novo grupo deve ler histórico? | `auto.offset.reset` |
| Meu batch é grande demais? | `max.poll.records` |
| Processamento demora muito? | `max.poll.interval.ms` |
| Quero evitar reprocessamento duplicado? | idempotência no destino |

### Protobuf/Schema

| Pergunta | Decisão |
|---|---|
| Payloads variam muito? | envelope + payload interno |
| Preciso evoluir contrato? | Schema Registry |
| Campo ausente é diferente de vazio? | `optional` |
| Posso quebrar consumers antigos? | compatibilidade backward/forward |

---

## Exemplo completo — fluxo robusto de fraude

```text
  Producer HTTP/API
       │
       │ monta FraudControlEvent
       │ serializa Protobuf
       │ acks=all + idempotence
       ▼
  Topic: fraud-events
       │
       │ partitions=6
       │ replication.factor=3
       │ min.insync.replicas=2
       ▼
  Consumer: timeline-service
       │
       ├── decode Protobuf falhou
       │      └── DLQ + commit
       │
       ├── payload JSON interno inválido
       │      └── DLQ + commit
       │
       ├── changeset/domínio inválido permanente
       │      └── DLQ + commit
       │
       ├── banco timeout/transitório
       │      └── não commit + retry
       │
       └── insert ok
              └── commit offset
```

### Config base

```properties
# topic principal
partitions=6
replication.factor=3
min.insync.replicas=2
retention.ms=604800000
cleanup.policy=delete

# producer
acks=all
enable.idempotence=true
retries=2147483647
delivery.timeout.ms=120000
compression.type=zstd
linger.ms=10
batch.size=32768

# consumer
group.id=timeline-service
enable.auto.commit=false
auto.offset.reset=earliest
max.poll.records=500
max.poll.interval.ms=300000
session.timeout.ms=45000
heartbeat.interval.ms=15000
```

---

## Referência rápida de configurações

| Config | Camada | Resumo |
|---|---|---|
| `listeners` | Broker | Endereço onde o broker escuta |
| `advertised.listeners` | Broker | Endereço anunciado aos clients |
| `num.partitions` | Broker | Default de partições para novos topics |
| `default.replication.factor` | Broker | Default de réplicas para novos topics |
| `partitions` | Topic | Paralelismo e unidade de ordenação |
| `replication.factor` | Topic | Quantidade de cópias da partição |
| `min.insync.replicas` | Topic | Mínimo de réplicas sincronizadas para escrita segura |
| `retention.ms` | Topic | Retenção por tempo |
| `retention.bytes` | Topic | Retenção por tamanho |
| `cleanup.policy` | Topic | `delete`, `compact` ou ambos |
| `acks` | Producer | Quando considerar escrita confirmada |
| `enable.idempotence` | Producer | Evita duplicidade causada por retry do producer |
| `retries` | Producer | Tentativas de reenvio em erro transitório |
| `delivery.timeout.ms` | Producer | Tempo máximo total de entrega |
| `compression.type` | Producer | Compressão das mensagens |
| `batch.size` | Producer | Tamanho máximo do batch por partição |
| `linger.ms` | Producer | Espera para formar batch maior |
| `buffer.memory` | Producer | Memória para mensagens pendentes |
| `group.id` | Consumer | Identidade do grupo de consumo |
| `enable.auto.commit` | Consumer | Commit automático de offsets |
| `auto.offset.reset` | Consumer | Onde começar sem offset prévio |
| `max.poll.records` | Consumer | Máximo de mensagens por poll |
| `max.poll.interval.ms` | Consumer | Tempo máximo processando sem novo poll |
| `session.timeout.ms` | Consumer | Tempo para considerar consumer morto |
| `heartbeat.interval.ms` | Consumer | Frequência de heartbeat |
| `fetch.min.bytes` | Consumer | Mínimo de dados para resposta do broker |
| `fetch.max.wait.ms` | Consumer | Tempo máximo esperando acumular fetch |
