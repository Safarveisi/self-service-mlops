import os, json, logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.DEBUG)  # see authentication / metadata logs

KAFKA_CLIENT_PASSWORDS = os.getenv("KAFKA_CLIENT_PASSWORDS", "")
if not KAFKA_CLIENT_PASSWORDS:
    print("Environment variable KAFKA_CLIENT_PASSWORDS not set.")
    exit(1)

producer = KafkaProducer(
    bootstrap_servers="kafka-mlflow-kafka.kafka.svc.cluster.local:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username="user1",
    sasl_plain_password=KAFKA_CLIENT_PASSWORDS,
    retries=3,
    request_timeout_ms=50000
)

# Produce a message into a Kafka topic (test)
future = producer.send("test", {"run_id": "09123hfnasjfhyw", "experiment_name": "test_experiment"})
print(future.get(timeout=10))
producer.flush(timeout=10)
producer.close()