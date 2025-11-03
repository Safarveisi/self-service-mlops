"""
This is where we define an API with appropriate endpoints for
receiving webhooks from MLflow. Each webhook payload
contains the specifications of a model to be deployed
using KServe on Kubernetes. The received specification is
then published to a Kafka topic, where it is
consumed by another component in the platform.
"""

import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from kafka import KafkaProducer
from pydantic import BaseModel, Field, ValidationError
from utils import get_logger

log = get_logger()

PORT = int(os.getenv("PORT", "8080"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
APP_NAME = os.getenv("APP_NAME", "mlflow-webhook-handler")
required = {
    "KAFKA_CLIENT_PASSWORDS": os.getenv("KAFKA_CLIENT_PASSWORDS", ""),
    "KAFKA_TOPIC_MODEL_PRODUCTION_VERSION": os.getenv("KAFKA_TOPIC_MODEL_PRODUCTION_VERSION", ""),
    "KAFKA_SASL_USERNAME": os.getenv("KAFKA_SASL_USERNAME", ""),
    "KAFKA_BOOTSTRAP": os.getenv("KAFKA_BOOTSTRAP", ""),
}

producer: KafkaProducer | None = None


class SetModelVersionTagPayload(BaseModel):
    name: str = Field(..., description="Mlflow registered model's name")
    version: str = Field(..., description="Model version as a string, e.g. '1'")
    key: str = Field(..., description="Tag key")
    value: str = Field(..., description="Tag value")
    run_id: str | None = Field(None, description="Associated MLflow run ID")
    experiment_id: str | None = Field(None, description="Associated MLflow experiment ID")


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    global producer
    log.info("Starting %s", APP_NAME)

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    log.info("Creating Kafka producer")
    producer = KafkaProducer(
        bootstrap_servers=required["KAFKA_BOOTSTRAP"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=required["KAFKA_SASL_USERNAME"],
        sasl_plain_password=required["KAFKA_CLIENT_PASSWORDS"],
        retries=3,
        request_timeout_ms=40000,
    )

    try:
        yield
    finally:
        # graceful shutdown
        try:
            if producer is not None:
                producer.flush(timeout=10)
                producer.close()
        except Exception as e:
            log.warning("Kafka shutdown warning: %s", e)
        log.info("Shutting down %s", APP_NAME)


app = FastAPI(title=APP_NAME, lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "app": APP_NAME}


@app.post("/push")
async def mlflow_webhook(request: Request) -> JSONResponse:
    """
    Receives MLflow webhook POSTs and publishes the payload to Kafka.
    """
    log.info("Received request: %s", request)
    raw = await request.body()
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception as e:
        log.error("Invalid JSON received: %s", raw)
        return JSONResponse({"error": "Invalid JSON", "details": e.errors()}, status_code=400)

    log.info("Running data validation: %s", data)
    # Validate & coerce types
    try:
        payload = SetModelVersionTagPayload.model_validate(data)
    except ValidationError as e:
        log.error("Data validation error: %s", e)
        return JSONResponse({"error": "Invalid data", "details": e.errors()}, status_code=422)

    log.info(
        "Producing message to Kafka topic %s",
        required["KAFKA_TOPIC_MODEL_PRODUCTION_VERSION"],
    )
    future = producer.send(required["KAFKA_TOPIC_MODEL_PRODUCTION_VERSION"], payload.model_dump())
    # Block until a single message is sent (or timeout)
    try:
        metadata = future.get(timeout=10)
    except Exception as e:
        log.error("Failed to send message to Kafka: %s", e)
        return JSONResponse(
            {"error": "Failed to send message to Kafka", "details": e.errors()}, status_code=500
        )

    log.info(
        "Message sent to topic %s partition %d offset %d",
        metadata.topic,
        metadata.partition,
        metadata.offset,
    )
    return JSONResponse(
        {
            "status": "accepted",
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "model": payload.name,
            "version": payload.version,
            "run_id": payload.run_id,
            "experiment_id": payload.experiment_id,
        },
        status_code=202,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
