from confluent_kafka import Consumer, KafkaError

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'media-data-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000  # Commit offsets every second
}

# Create the consumer instance
consumer = Consumer(consumer_config)
consumer.subscribe(['media_titles'])

processed_keys = set()  # To track processed message keys

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for messages (timeout of 1 second)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"Reached end of partition: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
            elif msg.error():
                print(f"Error: {msg.error()}")
                break
        else:
            key = msg.key().decode('utf-8')
            if key not in processed_keys:
                # Proper message
                print(f"Received message: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}: key={key}, value={msg.value().decode('utf-8')}")
                processed_keys.add(key)
            else:
                print(f"Duplicate message detected: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
