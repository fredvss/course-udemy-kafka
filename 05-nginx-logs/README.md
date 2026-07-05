# 05 — Nginx Logs com Kafka

Módulo prático para ler logs de acesso do Nginx e publicar cada nova linha em um topic Kafka.

A ideia é simular um fluxo comum de observabilidade/event streaming:

```text
Request HTTP
    │
    ▼
Nginx
    │ escreve access.log
    ▼
producer.py
    │ lê novas linhas do arquivo
    ▼
Topic Kafka: nginx-logs
    │
    ▼
consumer.py
    │ consome eventos publicados
    ▼
stdout / processamento futuro
```

```text
05-nginx-logs/
├── docker-compose.yaml
├── producer.py
├── consumer.py
├── requirements.txt
├── html/
│   └── index.html
└── logs/
```

---

## Pré-requisito

Este módulo assume que o cluster KRaft do módulo [03-cluster](../03-cluster/README.md) já está rodando.

```bash
cd ../03-cluster
docker compose up -d
```

Os scripts Python usam os brokers expostos localmente:

```text
localhost:9092,localhost:9093,localhost:9094
```

---

## Subir o Nginx

Este módulo usa uma tag fixa do Nginx, não `latest`:

```yaml
image: nginx:1.27.5-alpine3.21
```

Para subir:

```bash
cd 05-nginx-logs
docker compose up -d
```

A aplicação ficará disponível em:

```text
http://localhost:8080
```

O arquivo de log será escrito em:

```text
05-nginx-logs/logs/access.log
```

---

## Criar o topic

Crie o topic `nginx-logs` no cluster Kafka:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --create \
  --topic nginx-logs \
  --partitions 3 \
  --replication-factor 3
```

Verifique:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:29092 \
  --describe \
  --topic nginx-logs
```

---

## Setup Python

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Producer

O producer fica seguindo o arquivo `logs/access.log`. Sempre que uma nova linha aparece, ele transforma o log em JSON e publica no topic `nginx-logs`.

```bash
python3 producer.py
```

Por padrão, ele lê apenas novos logs gerados depois da inicialização.

Para publicar também as linhas que já existiam no arquivo:

```bash
python3 producer.py --from-beginning
```

Configurações usadas no producer:

```python
Producer({
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
    "client.id": "nginx-log-producer",
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "snappy",
    "linger.ms": 20,
    "batch.size": 32768,
})
```

| Configuração | Motivo |
|---|---|
| `acks=all` | Espera confirmação das réplicas em ISR |
| `enable.idempotence=true` | Reduz risco de duplicidade por retry do producer |
| `compression.type=snappy` | Comprime bem para logs com baixo custo de CPU |
| `linger.ms=20` | Aguarda um pouco para formar batches melhores |
| `batch.size=32768` | Permite lotes maiores antes do envio |

---

## Gerar logs no Nginx

Em outro terminal, faça algumas requests:

```bash
curl http://localhost:8080/
curl http://localhost:8080/teste
curl http://localhost:8080/api/users
curl -I http://localhost:8080/
```

Você também pode acompanhar o log diretamente:

```bash
tail -f logs/access.log
```

Exemplo de linha do Nginx:

```text
172.18.0.1 - - [04/Jul/2026:20:30:00 +0000] "GET / HTTP/1.1" 200 246 "-" "curl/8.5.0"
```

Evento publicado no Kafka:

```json
{
  "source": "nginx",
  "log_type": "access",
  "parse_status": "parsed",
  "remote_addr": "172.18.0.1",
  "method": "GET",
  "path": "/",
  "protocol": "HTTP/1.1",
  "status": 200,
  "body_bytes_sent": 246,
  "http_referer": null,
  "http_user_agent": "curl/8.5.0",
  "raw": "172.18.0.1 - - [...]"
}
```

---

## Consumer

O consumer lê os eventos do topic `nginx-logs` e imprime no terminal.

```bash
python3 consumer.py
```

Configurações usadas:

```python
Consumer({
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
    "group.id": "nginx-log-reader",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
})
```

| Configuração | Motivo |
|---|---|
| `group.id` | Define o consumer group que terá offsets próprios |
| `auto.offset.reset=earliest` | Lê desde o início quando o grupo ainda não tem offset salvo |
| `enable.auto.commit=true` | Commit automático para simplificar o laboratório |

Para testar com outro grupo e reler tudo desde o início:

```bash
python3 consumer.py --group-id nginx-log-reader-2 --auto-offset-reset earliest
```

---

## Fluxo recomendado de teste

Terminal 1 — cluster Kafka:

```bash
cd 03-cluster
docker compose up -d
```

Terminal 2 — Nginx:

```bash
cd 05-nginx-logs
docker compose up -d
```

Terminal 3 — producer:

```bash
cd 05-nginx-logs
source venv/bin/activate
python3 producer.py
```

Terminal 4 — consumer:

```bash
cd 05-nginx-logs
source venv/bin/activate
python3 consumer.py
```

Terminal 5 — gerar tráfego:

```bash
curl http://localhost:8080/
curl http://localhost:8080/produtos
curl http://localhost:8080/pagamentos
curl http://localhost:8080/nao-existe
```

---

## Encerrando

```bash
docker compose down
```

Para parar também o cluster Kafka:

```bash
cd ../03-cluster
docker compose down
```
