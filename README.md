# Apache Kafka — Curso Udemy

Material prático de estudos sobre Apache Kafka, passando pelos fundamentos da plataforma, configuração de brokers e clients, clusters ZooKeeper e KRaft, desenvolvimento de producers/consumers e integração com aplicações reais utilizando Python.

## Pré-requisitos

| Requisito | Módulos |
|-----------|----------|
| Docker Engine | todos |
| Docker Compose v2 | todos |
| Python 3.11+ | 04, 05 |
| pip | 04, 05 |

## Estrutura do repositório

| Pasta | Tema | Documentação |
|-------|------|--------------|
| [01-quickstart](01-quickstart/) | Primeiro broker Kafka, CLI e operações básicas | [README](01-quickstart/README.md) |
| [02-cluster-zookeeper](02-cluster-zookeeper/) | Cluster Kafka utilizando ZooKeeper | [README](02-cluster-zookeeper/README.md) |
| [03-cluster-kraft](03-cluster-kraft/) | Cluster Kafka utilizando KRaft | [README](03-cluster-kraft/README.md) |
| [04-kafka-api](04-kafka-api/) | Producer e Consumer em Python | [README](04-kafka-api/README.md) |
| [05-nginx-logs](05-nginx-logs/) | Ingestão de logs do Nginx via Kafka | [README](05-nginx-logs/README.md) |

## Documentação

Fundamentos e configurações em **docs/**.

### Fundamentos

- Fundamentos do Kafka
- Configurações do Kafka

### Guias práticos

- Criando um cluster KRaft
- Criando um cluster ZooKeeper
- Producer em Python
- Consumer em Python
- Publicando logs do Nginx

### Recursos extras

- Anotações
- Diagramas

## Ordem sugerida

```text
01-quickstart
      ↓
02-cluster-zookeeper
      ↓
03-cluster-kraft
      ↓
04-kafka-api
      ↓
05-nginx-logs
```

> Os documentos em `docs/` complementam os exercícios práticos e podem ser lidos a qualquer momento.

## Conceitos estudados

- Arquitetura Kafka
- Brokers
- Controllers
- ZooKeeper
- KRaft
- Topics
- Partições
- Replicação
- ISR
- Offsets
- Consumer Groups
- Producer
- Consumer
- Leader Election
- Retenção
- Log Compaction
- Batching
- Compressão
- Retries
- Idempotência
- DLQ
- Serialização
- Protobuf
- Schema Registry

## Licença

Material de estudo pessoal — use livremente para aprendizado.