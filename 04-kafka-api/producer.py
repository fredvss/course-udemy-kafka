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