import argparse
import json
from confluent_kafka import Consumer

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
DEFAULT_TOPIC = "nginx-logs"
DEFAULT_GROUP_ID = "nginx-log-reader"


def decode(value):
    if value is None:
        return None

    text = value.decode("utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def main():
    parser = argparse.ArgumentParser(description="Consome eventos de logs do Nginx publicados no Kafka.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    parser.add_argument("--auto-offset-reset", default="earliest", choices=["earliest", "latest"])
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "auto.offset.reset": args.auto_offset_reset,
            "enable.auto.commit": True,
        }
    )

    consumer.subscribe([args.topic])

    print(f"[consumer] consumindo topic={args.topic} group.id={args.group_id}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"[consumer] erro: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value = decode(msg.value())

            print("-" * 80)
            print(f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key={key}")
            print(json.dumps(value, indent=2, ensure_ascii=False) if isinstance(value, dict) else value)
    except KeyboardInterrupt:
        print("\n[consumer] encerrando...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
