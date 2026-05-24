# 02 — Cluster com ZooKeeper

Cluster de 3 brokers Kafka coordenado por ZooKeeper, usando a imagem `confluentinc/cp-kafka:7.6.1`.

> Esta é a arquitetura clássica do Kafka, anterior ao KRaft. O módulo [03-cluster](../03-cluster/README.md) cobre o modelo moderno sem ZooKeeper.

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

| Serviço | Porta externa | Uso |
|---|---|---|
| ZooKeeper | 2181 | Coordenação do cluster |
| kafka-1 | 9092 | Broker 1 (acesso externo) |
| kafka-2 | 9093 | Broker 2 (acesso externo) |
| kafka-3 | 9094 | Broker 3 (acesso externo) |

### Diagrama do cluster

```text
                    ┌──────────────────┐
                    │    ZooKeeper     │
                    │    :2181         │
                    └────────┬─────────┘
                             │  coordenação
          ┌──────────────────┼──────────────────┐
          │                  │                  │
   ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
   │   kafka-1   │    │   kafka-2   │    │   kafka-3   │
   │   ID: 1     │    │   ID: 2     │    │   ID: 3     │
   │   :9092     │    │   :9093     │    │   :9094     │
   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
          │                  │                  │
          └──────────────────┼──────────────────┘
                             │
              Producers / Consumers
              (via localhost:9092, 9093 ou 9094)
```

---

## Configuração de listeners

Cada broker usa dois listeners:

```text
INTERNAL://kafka-N:29092   ← comunicação entre brokers (dentro da rede Docker)
EXTERNAL://0.0.0.0:909X    ← acesso externo (host / clientes CLI)
```

O `KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL` garante que a replicação entre brokers use a rede interna do Docker, enquanto clientes externos se conectam pela porta mapeada no host.

---

## Testando o cluster

Entre em qualquer broker para usar a CLI:

```bash
docker exec -it <container_id_kafka-1> bash
cd /opt/kafka/bin/
```

### Criar topic com replicação

```bash
./kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --topic pedidos \
  --create \
  --partitions 3 \
  --replication-factor 3
```

> `--replication-factor 3` distribui cada partição pelos 3 brokers, garantindo tolerância a falhas.

### Verificar distribuição

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

Cada partição tem um leader diferente — carga distribuída entre os 3 brokers.

---

## Arquitetura ZooKeeper

### Papel do ZooKeeper

O ZooKeeper era responsável por toda a coordenação distribuída do Kafka:

- registro de brokers ativos
- eleição de controller e partition leaders
- armazenamento de metadata (topics, ACLs, configurações)
- monitoramento de ISR (In-Sync Replicas)
- detecção de falhas

```text
ZooKeeper mantém internamente:

/brokers                  ← lista de brokers ativos
/topics                   ← metadata dos topics
/controller               ← qual broker é o controller
/isr_change_notification  ← mudanças nas ISRs
```

### Controller

Um dos brokers é eleito **controller** pelo ZooKeeper. Ele é responsável por:

- criar e deletar topics
- eleger leaders de partição
- detectar e reagir a falhas de broker

```text
ZooKeeper
   ↓ elege
Controller (ex: Broker 1)
   ↓ gerencia
Brokers 2 e 3
```

### Falha de broker

```text
Broker 2 cai
    ↓
ZooKeeper detecta (sessão expira)
    ↓
Controller reage
    ↓
Novo leader eleito para partições que tinham Broker 2 como leader
    ↓
Clients atualizam metadata e continuam operando
```

### ISR — In-Sync Replicas

ISR é o conjunto de réplicas que estão sincronizadas com o leader.

```text
Estado normal:    ISR = [1, 2, 3]
Replica atrasou:  ISR = [1, 3]
```

Kafka só elege como leader uma réplica que está no ISR, evitando perda de dados.

### Replicação e durabilidade

```text
Producer
   ↓
Leader da Partition  ←── escreve no log
   ↓
Followers replicam
   ↓
ISR confirma
   ↓
ACK para o producer
   ↓
Consumer lê
```

Nível de garantia configurável via `acks`:

| `acks` | Comportamento |
|---|---|
| `0` | Producer não espera confirmação (mais rápido, menos seguro) |
| `1` | Apenas o leader confirma (followers podem perder dados) |
| `all` | Todos os ISR confirmam (mais seguro) |

---

## Por que o ZooKeeper foi substituído

A arquitetura Kafka + ZooKeeper exigia operar dois sistemas distribuídos complexos em paralelo, o que gerava:

- overhead operacional elevado
- sincronização de estado entre dois sistemas
- maior latência no processamento de metadata
- limite de ~200k partições por cluster

O KRaft resolve esses problemas internalizando o consenso de metadata dentro do próprio Kafka (ver [03-cluster](../03-cluster/README.md)).
