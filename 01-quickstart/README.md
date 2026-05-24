# 01 — Quickstart: Broker único com Docker

Guia prático para subir um broker Kafka usando a imagem oficial `apache/kafka` e explorar os principais comandos de topics, producers, consumers e consumer groups.

---

## Subindo o container

```bash
# Baixar a imagem
docker pull apache/kafka:4.0.2

# Executar o container
docker run -it \
  --name kafka \
  --network host \
  apache/kafka:4.0.2
```

> `--network host` faz o container compartilhar a interface de rede do host, tornando o broker acessível diretamente em `localhost:9092` sem mapeamento de portas.

```text
  Host (localhost)
  ┌─────────────────────────────────────────┐
  │                                         │
  │   Container: kafka  (--network host)    │
  │   ┌───────────────────────────────┐     │
  │   │  apache/kafka:4.0.2           │     │
  │   │                               │     │
  │   │  Broker  ──────► :9092        │     │
  │   │  KRaft (sem ZooKeeper)        │     │
  │   │                               │     │
  │   │  /opt/kafka/bin/              │     │
  │   │  /opt/kafka/config/           │     │
  │   │  /tmp/kraft-combined-logs/    │     │
  │   └───────────────────────────────┘     │
  │                                         │
  │   Clientes CLI  ──► localhost:9092      │
  └─────────────────────────────────────────┘
```

---

## Topics

Todos os comandos a seguir são executados dentro do container (ou em qualquer terminal com acesso ao broker).

```bash
cd /opt/kafka/bin/
```

### Listar

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Criar

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 \
  --topic payments \
  --create \
  --partitions 3 \
  --replication-factor 1
```

### Descrever

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 \
  --topic payments \
  --describe
```

```
Topic: payments  PartitionCount: 3  ReplicationFactor: 1
  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

### Alterar partições

> Só é possível aumentar; nunca reduzir.

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 \
  --topic payments \
  --alter \
  --partitions 4
```

### Deletar

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 \
  --topic payments \
  --delete
```

---

## Explorando os logs em disco

Com KRaft, os dados ficam em `/tmp/kraft-combined-logs/`.

```bash
grep -R "log.dirs" /opt/kafka/config  # confirma o diretório
cd /tmp/kraft-combined-logs
ls -la
# payments-0/  payments-1/  payments-2/  ...
```

```text
  /tmp/kraft-combined-logs/
  ├── payments-0/
  │   ├── 00000000000000000000.log
  │   ├── 00000000000000000000.index
  │   └── ...
  ├── payments-1/
  └── payments-2/
```

### Variáveis de ambiente do broker

```bash
echo $KAFKA_NODE_ID                   # ID do nó no KRaft
cat /opt/kafka/config/server.properties
```

---

## Producer

### Mensagens sem chave (round-robin entre partições)

```bash
./kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic payments
```

```
> teste1
> teste2
```

`Ctrl+C` para sair.

### Mensagens com chave

Garante que mensagens com a mesma chave sempre vão para a mesma partição.

```bash
./kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic payments \
  --property parse.key=true \
  --property key.separator=:
```

```
> user-1:pagamento aprovado
> user-2:pagamento pendente
> user-1:pagamento cancelado
```

### Auto-criação de topic

Se `auto.create.topics.enable=true` (padrão), o producer cria o topic automaticamente quando ele não existe:

```bash
./kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic non-existing-topic
```

---

## Consumer

### Apenas novas mensagens (padrão)

```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic payments
```

### Desde o início

```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic payments \
  --from-beginning
```

### Partição específica

```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic payments \
  --partition 0
```

### A partir de um offset específico

```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic payments \
  --partition 0 \
  --offset 2
```

### Com limite de mensagens

```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic payments \
  --partition 0 \
  --offset 2 \
  --max-messages 1
```

### Com consumer group

```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic payments \
  --group consumidores-payments
```

> Cada consumer em um mesmo group recebe mensagens de partições distintas. Subir múltiplos terminais com o mesmo `--group` distribui a carga automaticamente.

---

## Consumer Groups

### Listar grupos

```bash
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

### Descrever grupo (offsets, lag, partições)

```bash
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe \
  --group consumidores-payments
```

```
GROUP                  TOPIC    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
consumidores-payments  payments 0          4               4               0
consumidores-payments  payments 1          3               3               0
consumidores-payments  payments 2          2               2               0
```

### Resetar offsets para o início

> O consumer precisa estar parado para o reset funcionar.

```bash
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group consumidores-payments \
  --topic payments \
  --reset-offsets \
  --to-earliest \
  --execute
```

---

## Diagrama: Producer → Topic → Consumer Group

```text
                     Topic: payments  (3 partições)
                     ┌──────────────────────────────────┐
                     │  Partition 0  [msg1, msg4, ...]   │
  Producer ────────► │  Partition 1  [msg2, msg5, ...]   │
  (sem chave:        │  Partition 2  [msg3, msg6, ...]   │
   round-robin)      └──────┬─────────────┬─────────────┘
                             │             │             │
                        Consumer A    Consumer B    Consumer C
                        (part. 0)     (part. 1)     (part. 2)
                        └─────────────────────────────────┘
                              Group: consumidores-payments
```

**Com chave — ordenação garantida por chave:**

```text
  Producer (parse.key=true)
  ├── user-1:msg  ──► hash("user-1") % 3  ──► Partition 0  ──► Consumer A
  ├── user-2:msg  ──► hash("user-2") % 3  ──► Partition 1  ──► Consumer B
  └── user-1:msg  ──► hash("user-1") % 3  ──► Partition 0  ──► Consumer A  ✓
```

---

## Referência rápida

| Ação | Comando |
|---|---|
| Listar topics | `kafka-topics.sh ... --list` |
| Criar topic | `kafka-topics.sh ... --topic X --create --partitions N --replication-factor 1` |
| Descrever topic | `kafka-topics.sh ... --topic X --describe` |
| Alterar partições | `kafka-topics.sh ... --topic X --alter --partitions N` |
| Deletar topic | `kafka-topics.sh ... --topic X --delete` |
| Produzir mensagens | `kafka-console-producer.sh ... --topic X` |
| Produzir com chave | `kafka-console-producer.sh ... --topic X --property parse.key=true --property key.separator=:` |
| Consumir (grupo) | `kafka-console-consumer.sh ... --topic X --group G` |
| Consumir desde início | `kafka-console-consumer.sh ... --topic X --from-beginning` |
| Listar consumer groups | `kafka-consumer-groups.sh ... --list` |
| Descrever consumer group | `kafka-consumer-groups.sh ... --describe --group G` |
| Resetar offsets | `kafka-consumer-groups.sh ... --group G --topic X --reset-offsets --to-earliest --execute` |
