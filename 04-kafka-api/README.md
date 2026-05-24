# 04 — Kafka API com Python

Producer e consumer em Python usando a biblioteca `confluent-kafka`, conectando ao cluster KRaft do módulo [03-cluster](../03-cluster/README.md).

```text
04-kafka-api/
├── producer.py
├── consumer.py
└── requirements.txt
```

---

## Setup

```bash
# Criar e ativar o ambiente virtual
python3 -m venv venv
source venv/bin/activate

# Instalar dependências
pip install -r requirements.txt
```

> Certifique-se de que o cluster do `03-cluster` está rodando antes de executar os scripts.

---

## Producer

Publica uma mensagem no topic `payments` com chave e valor.

```python
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094"
})

producer.produce(
    topic="payments",
    key="user-123",
    value="payment-created"
)

producer.flush()
```

```bash
python3 producer.py
```

> `producer.flush()` bloqueia até que todas as mensagens pendentes sejam entregues ao broker.

---

## Consumer

Consume mensagens do topic `payments` continuamente, imprimindo topic, partição, offset, chave e valor de cada mensagem.

```python
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
    "group.id": "payments-worker",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["payments"])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print(msg.error())
        continue

    print(
        msg.topic(),
        msg.partition(),
        msg.offset(),
        msg.key(),
        msg.value()
    )
```

```bash
python3 consumer.py
```

> `auto.offset.reset: earliest` faz o consumer ler desde o início quando não há offset salvo para o grupo.

---

## Configurações principais

| Parâmetro | Onde | Descrição |
|---|---|---|
| `bootstrap.servers` | producer / consumer | Lista de brokers para descoberta inicial do cluster |
| `group.id` | consumer | Identificador do consumer group |
| `auto.offset.reset` | consumer | Ponto de início quando não há offset salvo (`earliest` ou `latest`) |
