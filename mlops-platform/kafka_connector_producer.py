import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from kafka import KafkaProducer
from pydantic import BaseModel, Field
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


@app.post("/webhook/mlflow")
async def mlflow_webhook(request: Request) -> JSONResponse:
    """
    Receives MLflow webhook POSTs and publishes the payload to Kafka.
    """
    log.info("Received request: %s", request)
    raw = await request.body()
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        log.error("Invalid JSON received: %s", raw)
        raise HTTPException(status_code=400, detail="Invalid JSON")

    log.info("Running data validation: %s", data)
    # Validate & coerce types
    payload = SetModelVersionTagPayload.model_validate(data)

    log.info(
        "Producing message to Kafka topic %s",
        required["KAFKA_TOPIC_MODEL_PRODUCTION_VERSION"],
    )
    future = producer.send(required["KAFKA_TOPIC_MODEL_PRODUCTION_VERSION"], payload.model_dump())
    # If you want non-blocking, remove .get(); here we wait up to 10s like your snippet.
    metadata = future.get(timeout=10)
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
