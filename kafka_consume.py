from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'trx_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=10000,
    value_deserializer=lambda m: json.loads(m.decode())
)
print('Listening for messages on topic trx_topic...')
for msg in consumer:
    print(msg.value)
consumer.close()
