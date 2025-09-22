import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from kafka import KafkaProducer

PORT = int(os.getenv("PORT", "8080"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
APP_NAME = os.getenv("APP_NAME", "mlflow-webhook-handler")
required = {
    "KAFKA_CLIENT_PASSWORDS": os.getenv("KAFKA_CLIENT_PASSWORDS", ""),
    "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", ""),
    "KAFKA_SASL_USERNAME": os.getenv("KAFKA_SASL_USERNAME", ""),
    "KAFKA_BOOTSTRAP": os.getenv("KAFKA_BOOTSTRAP", ""),
}

producer: Optional[KafkaProducer] = None

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(APP_NAME)


class ModelVersionPayload(BaseModel):
    name: str
    version: str = Field(..., description="Model version as a string, e.g. '1'")
    source: str
    run_id: str
    tags: Dict[str, str] = {}
    description: Optional[str] = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    global producer
    log.info("Starting %s", APP_NAME)

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

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
    raw = await request.body()
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Validate & coerce types
    payload = ModelVersionPayload.model_validate(data)

    # Produce to Kafka
    if producer is None:
        raise HTTPException(status_code=500, detail="Kafka producer not initialized")

    # send and block briefly to surface errors (keeps it simple)
    future = producer.send(required["KAFKA_TOPIC"], payload.model_dump())
    # If you want non-blocking, remove .get(); here we wait up to 10s like your snippet.
    metadata = future.get(timeout=10)

    return JSONResponse(
        {
            "status": "accepted",
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "model": payload.name,
            "version": payload.version,
        },
        status_code=202,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
