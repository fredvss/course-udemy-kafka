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