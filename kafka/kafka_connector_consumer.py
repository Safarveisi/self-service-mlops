import os
from kafka import KafkaConsumer

required = {
    "KAFKA_CLIENT_PASSWORDS": os.getenv("KAFKA_CLIENT_PASSWORDS", ""),
    "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", ""),
    "KAFKA_SASL_USERNAME": os.getenv("KAFKA_SASL_USERNAME", ""),
    "KAFKA_BOOTSTRAP": os.getenv("KAFKA_BOOTSTRAP", ""),
}

missing = [k for k, v in required.items() if not v]

if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

consumer = KafkaConsumer(
    required["KAFKA_TOPIC"],
    bootstrap_servers=required["KAFKA_BOOTSTRAP"],
    group_id="my_group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=required["KAFKA_SASL_USERNAME"],
    sasl_plain_password=required["KAFKA_CLIENT_PASSWORDS"],
)

# This will run indefinitely, listening for messages
for message in consumer:
    print(message.value)
    consumer.commit()  # Manually commit the offset after processing the message
