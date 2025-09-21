import os
from kafka import KafkaConsumer

KAFKA_CLIENT_PASSWORDS = os.getenv("KAFKA_CLIENT_PASSWORDS", "")
if not KAFKA_CLIENT_PASSWORDS:
    print("Environment variable KAFKA_CLIENT_PASSWORDS not set.")
    exit(1)

consumer = KafkaConsumer(
    "test",
    bootstrap_servers="kafka-mlflow-kafka.kafka.svc.cluster.local:9092",
    group_id="my_group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username="user1",
    sasl_plain_password=KAFKA_CLIENT_PASSWORDS
)

# This will run indefinitely, listening for messages
for message in consumer:
    print(message.value)
    consumer.commit()  # Manually commit the offset after processing the message
