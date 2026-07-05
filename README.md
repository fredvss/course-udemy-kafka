# Apache Kafka — Curso Udemy

Repositório de estudos práticos sobre Apache Kafka, cobrindo desde um broker isolado até clusters com ZooKeeper e KRaft.

---

## Documentação

- [01 - Fundamentos](docs/fundamentos.md)
  - Arquitetura do cluster
  - Brokers
  - Topics e partições
  - Replicação e ISR
  - Offsets
  - Consumer Groups
  - Retenção e Log Compaction

- [02 - Configurações](docs/configuracoes.md)
  - Broker
  - Topic
  - Producer
  - Consumer
  - Compressão
  - Batching
  - Retries
  - Durabilidade (acks, ISR)
  - Retenção e segmentos
  - Serialização (Protobuf/Schema Registry)

---

## Módulos

| Módulo | Descrição | Tecnologia |
|---|---|---|
| [01-quickstart](01-quickstart/README.md) | Broker único via Docker, CLI de topics/producer/consumer/consumer-groups | `apache/kafka:4.0.2` |
| [02-cluster-zookeeper](02-cluster-zookeeper/README.md) | Cluster de 3 brokers coordenado por ZooKeeper | `confluentinc/cp-kafka:7.6.1` |
| [03-cluster](03-cluster/README.md) | Cluster de 3 brokers KRaft (sem ZooKeeper) | `apache/kafka:4.0.2` |
| [04-kafka-api](04-kafka-api/README.md) | Producer e consumer em Python com `confluent-kafka` | Python 3 |
| [05-nginx-logs](05-nginx-logs/README.md) | Nginx com tag fixa, producer lendo `access.log` e consumer do topic `nginx-logs` | Nginx + Python 3 |

---

## Conceitos abordados

- Topics, partições e replicação
- Producers: round-robin e roteamento por chave
- Consumers e consumer groups
- Offsets, lag e reset de offsets
- Arquitetura ZooKeeper vs KRaft
- Configuração de listeners internos/externos
- Ingestão de logs do Nginx em um topic Kafka
- Producer com `acks`, idempotência, compressão e batching
- Consumer group lendo eventos publicados

---

## Pré-requisitos

- Docker e Docker Compose instalados
- Nenhuma dependência local além do Docker

```bash
docker --version
docker compose version
```
