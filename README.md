# Apache Kafka — Curso Udemy

Repositório de estudos práticos sobre Apache Kafka, cobrindo desde um broker isolado até clusters com ZooKeeper e KRaft.

---

## Documentação

- [Fundamentos do Kafka](docs/fundamentos.md) — broker, bootstrap server, topics, partições, replicação, offsets, consumer groups

---

## Módulos

| Módulo | Descrição | Tecnologia |
|---|---|---|
| [01-quickstart](01-quickstart/README.md) | Broker único via Docker, CLI de topics/producer/consumer/consumer-groups | `apache/kafka:4.0.2` |
| [02-cluster-zookeeper](02-cluster-zookeeper/README.md) | Cluster de 3 brokers coordenado por ZooKeeper | `confluentinc/cp-kafka:7.6.1` |
| [03-cluster](03-cluster/README.md) | Cluster de 3 brokers KRaft (sem ZooKeeper) | `apache/kafka:4.0.2` |

---

## Conceitos abordados

- Topics, partições e replicação
- Producers: round-robin e roteamento por chave
- Consumers e consumer groups
- Offsets, lag e reset de offsets
- Arquitetura ZooKeeper vs KRaft
- Configuração de listeners internos/externos

---

## Pré-requisitos

- Docker e Docker Compose instalados
- Nenhuma dependência local além do Docker

```bash
docker --version
docker compose version
```
