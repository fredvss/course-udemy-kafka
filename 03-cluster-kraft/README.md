# 03 — Cluster KRaft (sem ZooKeeper)

Cluster de 3 brokers Kafka usando o modo **KRaft** — o modelo moderno onde o Kafka gerencia seu próprio consenso de metadata sem depender do ZooKeeper.

---

## Subindo o cluster

```bash
docker compose up -d
```

```bash
docker compose down        # parar e remover containers
docker compose logs -f     # acompanhar logs
```

### Serviços e portas

| Serviço | Porta externa | `KAFKA_NODE_ID` |
|---|---|---|
| kafka-1 | 9092 | 1 |
| kafka-2 | 9093 | 2 |
| kafka-3 | 9094 | 3 |

### Diagrama do cluster

```text
  ┌─────────────────────────────────────────────────────────┐
  │                    Kafka KRaft Cluster                  │
  │                                                         │
  │   ┌───────────────┐  ┌───────────────┐  ┌───────────── ┐│
  │   │   kafka-1     │  │   kafka-2     │  │   kafka-3    ││
  │   │   NODE_ID: 1  │  │   NODE_ID: 2  │  │   NODE_ID: 3 ││
  │   │   broker      │  │   broker      │  │   broker     ││
  │   │   controller  │  │   controller  │  │   controller ││
  │   │   :9092 (ext) │  │   :9093 (ext) │  │   :9094 (ext)││
  │   │   :9093 (ctrl)│  │   :9093 (ctrl)│  │   :9093 (ctrl)│
  │   └───────────────┘  └───────────────┘  └──────────────┘│
  │                                                         │
  │   Raft Quorum: 1@kafka-1:9093, 2@kafka-2:9093,          │
  │                3@kafka-3:9093                           │
  └─────────────────────────────────────────────────────────┘
                           │
          Producers / Consumers
          (localhost:9092, :9093 ou :9094)
```

---

## O que é KRaft

No KRaft, cada nó pode exercer dois papéis simultaneamente (`KAFKA_PROCESS_ROLES: broker,controller`):

- **broker** — recebe e serve mensagens
- **controller** — participa do quorum de consenso Raft para metadata

O quorum é configurado em `KAFKA_CONTROLLER_QUORUM_VOTERS`, listando todos os controllers:

```
1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
```

### Comparação ZooKeeper vs KRaft

```text
  ZooKeeper (antigo)              KRaft (atual)
  ┌──────────────────┐            ┌──────────────────┐
  │  ZooKeeper       │            │                  │
  │  (sistema sep.)  │            │  Kafka Cluster   │
  └────────┬─────────┘            │                  │
           │                      │  Raft Quorum     │
           │ coordena             │  (interno)       │
  ┌────────▼─────────┐            │                  │
  │  Kafka Cluster   │            └──────────────────┘
  └──────────────────┘
  2 sistemas distribuídos         1 sistema distribuído
  operação complexa               operação simplificada
  ~200k partições/cluster         +milhões de partições
```

---

## Configuração de listeners

Cada broker tem 3 listeners:

```text
INTERNAL://kafka-N:29092   ← replicação entre brokers (rede Docker)
EXTERNAL://0.0.0.0:909X    ← acesso externo (host / clientes)
CONTROLLER://kafka-N:9093  ← comunicação do quorum Raft
```

---

## Testando o cluster

Entre em qualquer broker:

```bash
docker exec -it kafka-1 bash
cd /opt/kafka/bin/
```

### Criar topic com replicação total

```bash
./kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --topic pedidos \
  --create \
  --partitions 3 \
  --replication-factor 3
```

### Verificar distribuição das partições

```bash
./kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --topic pedidos \
  --describe
```

```
Topic: pedidos  PartitionCount: 3  ReplicationFactor: 3
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

### Verificar ID do nó

```bash
echo $KAFKA_NODE_ID
```

### Localizar os logs de dados

```bash
grep -R "log.dirs" /opt/kafka/config
# geralmente: /tmp/kraft-combined-logs
```

---

## Tolerância a falhas

Com `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3` e `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2`, o cluster tolera a queda de **1 broker** sem interrupção.

```text
kafka-2 cai
    ↓
Quorum Raft: kafka-1 e kafka-3 mantêm consenso (maioria)
    ↓
Partições com leader no kafka-2 elegem novo leader
    ↓
Cluster continua operando
    ↓
kafka-2 volta → sincroniza e entra de volta no ISR
```
