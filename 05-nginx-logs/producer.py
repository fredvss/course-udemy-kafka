import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
DEFAULT_TOPIC = "nginx-logs"
DEFAULT_LOG_FILE = "logs/access.log"

LOG_PATTERN = re.compile(
    r'(?P<remote_addr>\S+) - (?P<remote_user>\S+) '
    r'\[(?P<time_local>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) (?P<protocol>[^"]+)" '
    r'(?P<status>\d{3}) (?P<body_bytes_sent>\d+) '
    r'"(?P<http_referer>[^"]*)" '
    r'"(?P<http_user_agent>[^"]*)"'
)


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] falha ao publicar: {err}")
        return

    print(
        "[producer] publicado",
        f"topic={msg.topic()}",
        f"partition={msg.partition()}",
        f"offset={msg.offset()}",
        f"key={msg.key().decode('utf-8') if msg.key() else None}",
    )


def parse_nginx_log(line: str) -> dict:
    raw_line = line.strip()
    match = LOG_PATTERN.match(raw_line)

    event = {
        "source": "nginx",
        "log_type": "access",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "raw": raw_line,
    }

    if not match:
        event["parse_status"] = "unparsed"
        return event

    fields = match.groupdict()
    event.update(
        {
            "parse_status": "parsed",
            "remote_addr": fields["remote_addr"],
            "remote_user": None if fields["remote_user"] == "-" else fields["remote_user"],
            "time_local": fields["time_local"],
            "method": fields["method"],
            "path": fields["path"],
            "protocol": fields["protocol"],
            "status": int(fields["status"]),
            "body_bytes_sent": int(fields["body_bytes_sent"]),
            "http_referer": None if fields["http_referer"] == "-" else fields["http_referer"],
            "http_user_agent": fields["http_user_agent"],
        }
    )
    return event


def wait_for_file(path: str):
    while not os.path.exists(path):
        print(f"[producer] aguardando arquivo de log: {path}")
        time.sleep(1)


def follow_file(path: str, from_beginning: bool):
    wait_for_file(path)

    with open(path, "r", encoding="utf-8", errors="replace") as file:
        if not from_beginning:
            file.seek(0, os.SEEK_END)

        while True:
            line = file.readline()
            if line:
                yield line
                continue

            time.sleep(0.5)

            # Caso o arquivo seja truncado/rotacionado, volta para o começo.
            if file.tell() > os.path.getsize(path):
                file.seek(0)


def build_key(event: dict) -> Optional[str]:
    return event.get("remote_addr") or event.get("source")


def main():
    parser = argparse.ArgumentParser(description="Lê access.log do Nginx e publica eventos no Kafka.")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--log-file", default=DEFAULT_LOG_FILE)
    parser.add_argument("--from-beginning", action="store_true")
    args = parser.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "nginx-log-producer",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
            "linger.ms": 20,
            "batch.size": 32768,
        }
    )

    print(f"[producer] lendo {args.log_file}")
    print(f"[producer] publicando no topic {args.topic}")

    try:
        for line in follow_file(args.log_file, args.from_beginning):
            event = parse_nginx_log(line)
            key = build_key(event)
            value = json.dumps(event, ensure_ascii=False).encode("utf-8")

            producer.produce(
                topic=args.topic,
                key=key.encode("utf-8") if key else None,
                value=value,
                callback=delivery_report,
            )
            producer.poll(0)
    except KeyboardInterrupt:
        print("\n[producer] encerrando...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
