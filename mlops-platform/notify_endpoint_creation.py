"""
This is where we notify downstream components in the
platform that the endpoint for the MLflow model
was successfully created using KServe.
"""

import json
import os

from kafka import KafkaConsumer
from utils import get_logger

log = get_logger()


required = {
    "KAFKA_CLIENT_PASSWORDS": os.getenv("KAFKA_CLIENT_PASSWORDS", ""),
    "KAFKA_TOPIC_MODEL_PRODUCTION_ENDPOINT": os.getenv("KAFKA_TOPIC_MODEL_PRODUCTION_ENDPOINT", ""),
    "KAFKA_SASL_USERNAME": os.getenv("KAFKA_SASL_USERNAME", ""),
    "KAFKA_BOOTSTRAP": os.getenv("KAFKA_BOOTSTRAP", ""),
}

missing = [k for k, v in required.items() if not v]

if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

kafka_args = {
    "bootstrap_servers": required["KAFKA_BOOTSTRAP"],
    "group_id": "my_group",
    "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
    "auto_offset_reset": "earliest",
    "enable_auto_commit": False,
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "SCRAM-SHA-256",
    "sasl_plain_username": required["KAFKA_SASL_USERNAME"],
    "sasl_plain_password": required["KAFKA_CLIENT_PASSWORDS"],
}

if __name__ == "__main__":
    consumer = KafkaConsumer(
        required["KAFKA_TOPIC_MODEL_PRODUCTION_ENDPOINT"],
        **kafka_args,
    )
    # We can extend the logic here
    for message in consumer:
        inference_service_name = message.value.get("inference_service_name", "")
        inference_service_namespace = message.value.get("inference_service_namespace", "")
        experiment_id = message.value.get("experiment_id", "")
        run_id = message.value.get("run_id", "")
        status = message.value.get("status", "")
        message = message.value.get("message", "")

        log.info(  # Log the status of the inference service (kibana logs)
            f"Status for {inference_service_name} in namespace "
            f"{inference_service_namespace} is {status}."
            f" Experiment ID: {experiment_id}, Run ID: {run_id}."
        )
        log.info(f"{message}")

        consumer.commit()  # Manually commit the offset after processing the message
