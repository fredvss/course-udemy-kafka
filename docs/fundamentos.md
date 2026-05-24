# Fundamentos do Apache Kafka

Referência conceitual dos principais componentes e mecanismos do Kafka.

---

## O que é o Kafka

Kafka é, em essência, um **log distribuído de escrita sequencial** (distributed append-only log). Tudo o mais — topics, partições, replicação, consumer groups — é construído sobre esse conceito.

```text
  Partition Log

  offset →  0        1        2        3        4
           ┌────────┬────────┬────────┬────────┬────────┐
           │  msg0  │  msg1  │  msg2  │  msg3  │  msg4  │  ...
           └────────┴────────┴────────┴────────┴────────┘
            (imutável — escrita apenas no final)
```

Mensagens nunca são alteradas ou deletadas imediatamente — elas expiram por tempo (`log.retention.hours`) ou tamanho (`log.retention.bytes`).

---

## Broker

Um **broker** é um servidor Kafka — um processo que:

- armazena partições em disco
- recebe mensagens de producers
- serve mensagens para consumers
- replica dados para outros brokers

Um cluster Kafka é formado por múltiplos brokers. Cada broker é identificado por um ID único (`KAFKA_BROKER_ID` ou `KAFKA_NODE_ID` no KRaft).

```text
  Kafka Cluster

  ┌───────────┐   ┌───────────┐   ┌───────────┐
  │  Broker 1 │   │  Broker 2 │   │  Broker 3 │
  │  ID: 1    │   │  ID: 2    │   │  ID: 3    │
  │           │   │           │   │           │
  │ partições │   │ partições │   │ partições │
  │ em disco  │   │ em disco  │   │ em disco  │
  └───────────┘   └───────────┘   └───────────┘
```

---

## Bootstrap Server

O **bootstrap server** é o endereço inicial que um client (producer ou consumer) usa para se conectar ao cluster.

O client **não precisa conhecer todos os brokers** — ele se conecta ao bootstrap, recebe os metadados do cluster (lista de brokers, líderes de partição, etc.) e passa a se comunicar diretamente com o broker correto para cada operação.

```text
  Client (producer ou consumer)
       │
       │ 1. conecta ao bootstrap
       ▼
  ┌─────────────┐
  │  Broker 1   │  ◄── bootstrap-server=localhost:9092
  │  :9092      │
  └──────┬──────┘
         │ 2. retorna metadata do cluster
         │    (brokers, líderes, partições)
         ▼
  Client descobre o cluster inteiro
  e passa a operar diretamente:

  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
  │  Broker 1   │   │  Broker 2   │   │  Broker 3   │
  └─────────────┘   └─────────────┘   └─────────────┘
        ▲                  ▲
        │ escreve/lê       │ escreve/lê
        └──────────────────┘
            conforme o líder de cada partição
```

> Basta um broker estar disponível no bootstrap para que o client descubra o restante. Em produção recomenda-se listar ao menos dois: `broker1:9092,broker2:9093`.

---

## Topic

Um **topic** é um canal lógico de mensagens — uma categoria ou feed de dados. Producers publicam em topics; consumers assinam topics.

```text
  Producers                     Consumers
     │                              │
     │  publica em →  [ payments ]  → │ assina
     │  publica em →  [ orders   ]  → │ assina
     │  publica em →  [ events   ]  → │ assina
```

Topics são apenas nomes lógicos — os dados físicos ficam nas **partições**.

---

## Partições

Cada topic é dividido em **partições** — a unidade fundamental de paralelismo e armazenamento do Kafka.

```text
  Topic: payments  (3 partições)

  Partition 0  ──  Broker 1
  ┌────┬────┬────┬────┐
  │ m0 │ m3 │ m6 │ m9 │ ...
  └────┴────┴────┴────┘

  Partition 1  ──  Broker 2
  ┌────┬────┬────┐
  │ m1 │ m4 │ m7 │ ...
  └────┴────┴────┘

  Partition 2  ──  Broker 3
  ┌────┬────┬────┐
  │ m2 │ m5 │ m8 │ ...
  └────┴────┴────┘
```

**Propriedades importantes:**

- A **ordem é garantida dentro de uma partição**, não entre partições
- Quanto mais partições, maior o paralelismo de leitura e escrita
- O número de partições só pode ser **aumentado**, nunca reduzido
- Cada partição tem um **líder** em um broker e réplicas nos demais

### Roteamento de mensagens pelo producer

| Cenário | Comportamento |
|---|---|
| Sem chave | Round-robin entre as partições |
| Com chave | `hash(key) % numPartitions` — mesma chave → sempre mesma partição |

```text
  Producer (sem chave — round-robin)        Producer (com chave)

  msg1 ──► Partition 0                      user-1:msg ──► hash("user-1") % 3 = 0 ──► Partition 0
  msg2 ──► Partition 1                      user-2:msg ──► hash("user-2") % 3 = 1 ──► Partition 1
  msg3 ──► Partition 2                      user-1:msg ──► hash("user-1") % 3 = 0 ──► Partition 0
  msg4 ──► Partition 0                      user-3:msg ──► hash("user-3") % 3 = 2 ──► Partition 2
```

---

## Replicação

Cada partição pode ter **réplicas** distribuídas entre brokers, garantindo durabilidade e tolerância a falhas.

```text
  Topic: payments  —  replication-factor: 3

  Partition 0
  ├── Leader   →  Broker 1  ◄── producers escrevem aqui
  ├── Follower →  Broker 2  (replica)
  └── Follower →  Broker 3  (replica)

  Partition 1
  ├── Leader   →  Broker 2
  ├── Follower →  Broker 3
  └── Follower →  Broker 1

  Partition 2
  ├── Leader   →  Broker 3
  ├── Follower →  Broker 1
  └── Follower →  Broker 2
```

Cada partição tem exatamente **1 líder** e `replication-factor - 1` followers. Somente o líder recebe escritas; os followers replicam de forma assíncrona.

### ISR — In-Sync Replicas

ISR é o conjunto de réplicas que estão sincronizadas com o líder (dentro de um lag tolerável).

```text
  Estado normal:         ISR = [1, 2, 3]  ← todas em dia

  Broker 3 atrasou:      ISR = [1, 2]     ← broker 3 removido temporariamente

  Broker 3 voltou:       ISR = [1, 2, 3]  ← reentrou após sincronizar
```

Kafka nunca elege como líder uma réplica fora do ISR, evitando perda de dados.

### Garantias do producer (acks)

| `acks` | Quem confirma | Risco |
|---|---|---|
| `0` | Ninguém | Perda de mensagem se o broker cair |
| `1` | Apenas o líder | Perda se o líder cair antes de replicar |
| `all` / `-1` | Todos os ISR | Sem perda (mais seguro) |

### Falha de broker

```text
  Broker 2 cai
       │
       ▼
  Controller detecta (via KRaft ou ZooKeeper)
       │
       ▼
  Novo líder eleito para as partições
  que tinham Broker 2 como líder
       │
       ▼
  Clients recebem metadata atualizada
  e continuam operando sem interrupção
```

---

## Offset

**Offset** é a posição de uma mensagem dentro de uma partição — um número inteiro sequencial, imutável e crescente.

```text
  Partition 0

  offset:  0        1        2        3        4        5
          ┌────────┬────────┬────────┬────────┬────────┬────────┐
          │ msg-A  │ msg-B  │ msg-C  │ msg-D  │ msg-E  │ msg-F  │ ...
          └────────┴────────┴────────┴────────┴────────┴────────┘
                                               ▲
                                      consumer lendo aqui (offset 4)
```

Consumers **controlam seus próprios offsets** — podem ler do início (`--from-beginning`), de um offset específico, ou apenas novas mensagens.

---

## Consumer Groups

Um **consumer group** é um conjunto de consumers que cooperam para consumir um topic. O Kafka distribui as partições entre os members do grupo — cada partição é atribuída a **no máximo um consumer por grupo**.

### Regra fundamental

> **1 partição → no máximo 1 consumer dentro do mesmo grupo**
> Um consumer pode ler múltiplas partições, mas duas instâncias do mesmo grupo nunca lerão a mesma partição ao mesmo tempo.

```text
  Topic: payments  (3 partições)
  Group: consumidores-payments

  ┌─────────────┐     ┌────────────────────────────────┐
  │ Partition 0 │────►│ Consumer A                     │
  ├─────────────┤     ├────────────────────────────────┤
  │ Partition 1 │────►│ Consumer B                     │
  ├─────────────┤     ├────────────────────────────────┤
  │ Partition 2 │────►│ Consumer C                     │
  └─────────────┘     └────────────────────────────────┘
  3 partições = 3 consumers ativos (ideal: 1 partição por consumer)
```

### Cenários com número diferente de consumers

```text
  2 consumers  (1 consumer lê 2 partições)       4 consumers  (1 consumer fica ocioso)

  Partition 0 ──► Consumer A                     Partition 0 ──► Consumer A
  Partition 1 ──► Consumer A                     Partition 1 ──► Consumer B
  Partition 2 ──► Consumer B                     Partition 2 ──► Consumer C
                                                  (nenhuma)  ──► Consumer D  ← idle
```

> Ter mais consumers do que partições não gera erro — os excedentes ficam ociosos aguardando um rebalanceamento (ex: se outro consumer cair).

### Múltiplos grupos — leitura independente

Grupos diferentes são **completamente independentes**. Cada grupo mantém seu próprio conjunto de offsets e lê todas as mensagens do topic.

```text
  Topic: payments

  Partition 0 ──┬──► Group A / Consumer A1   (offset próprio)
                └──► Group B / Consumer B1   (offset próprio)

  Partition 1 ──┬──► Group A / Consumer A2
                └──► Group B / Consumer B1

  Partition 2 ──┬──► Group A / Consumer A3
                └──► Group B / Consumer B2
```

---

## Rebalanceamento

Quando um consumer **entra ou sai** do grupo, o Kafka redistribui as partições entre os members restantes — isso se chama **rebalanceamento**.

```text
  Antes (3 consumers):          Consumer C cai:          Após rebalancear:

  P0 → Consumer A               P0 → Consumer A          P0 → Consumer A
  P1 → Consumer B               P1 → Consumer B          P1 → Consumer B
  P2 → Consumer C               P2 → ???                 P2 → Consumer A  (ou B)
```

Durante o rebalanceamento, o consumo é pausado brevemente — é normal em aplicações reais.

---

## Retenção e Log Compaction

### Retenção por tempo/tamanho

Kafka não é um banco de dados — mensagens expiram. Os dois controles principais:

```properties
log.retention.hours=168        # 7 dias (padrão)
log.retention.bytes=-1         # sem limite por tamanho (padrão)
```

### Log Compaction

Modo alternativo: em vez de expirar por tempo, Kafka mantém apenas a **última mensagem por chave**.

```text
  Antes da compactação:             Após:

  offset 0: user-1 → ACTIVE         user-1 → BLOCKED   (última)
  offset 1: user-2 → ACTIVE         user-2 → ACTIVE    (única)
  offset 2: user-1 → BLOCKED
```

Útil para topics que representam **estado atual** de entidades (ex: perfis, configurações).

---

## Visão geral: fluxo completo

```text
                        Topic: payments  (3 partições, replication-factor: 3)

                        ┌──────────────────────────────────────────────┐
                        │                                              │
  Producer A ──────────►│  Partition 0  (Leader: Broker 1)            │
  Producer B ──────────►│  Partition 1  (Leader: Broker 2)            │
  Producer C ──────────►│  Partition 2  (Leader: Broker 3)            │
                        │                                              │
                        └──────────────┬───────────────────────────────┘
                                       │
                         ┌─────────────┴──────────────┐
                         │                            │
               Group: app-pagamentos        Group: auditoria
               ┌─────────────────────┐     ┌────────────────────┐
               │ Consumer 1 ← P0     │     │ Consumer X ← P0    │
               │ Consumer 2 ← P1     │     │ Consumer X ← P1    │
               │ Consumer 3 ← P2     │     │ Consumer Y ← P2    │
               └─────────────────────┘     └────────────────────┘
               (offsets independentes)     (offsets independentes)
```

---

## Referência rápida de conceitos

| Conceito | Definição resumida |
|---|---|
| **Broker** | Servidor Kafka que armazena e serve partições |
| **Bootstrap server** | Endereço inicial para descoberta do cluster |
| **Topic** | Canal lógico de mensagens |
| **Partição** | Unidade física de armazenamento; garante ordem interna |
| **Offset** | Posição imutável de uma mensagem dentro de uma partição |
| **Replication factor** | Quantas cópias de cada partição existem no cluster |
| **Leader** | Réplica que recebe escritas para uma partição |
| **Follower** | Réplica que sincroniza do leader |
| **ISR** | Conjunto de réplicas sincronizadas com o leader |
| **Consumer group** | Consumers cooperativos; cada partição atendida por no máximo 1 consumer do grupo |
| **Rebalanceamento** | Redistribuição de partições quando o grupo muda |
| **Log compaction** | Retenção apenas da última mensagem por chave |
