import os
from utils import get_logger, run_command
from model_endpoint import CreateModelEndpoint
import time
import json
from kafka import KafkaConsumer, KafkaProducer

log = get_logger()

RETRY_INTERVAL_SECONDS = 5  # Interval between checks for pod status
TIMEPOUT_SECONDS = 300  # Timeout for waiting for pods to be in 'Running' state
CREATE_MODEL_ENDPOINT = CreateModelEndpoint()

required = {
    "KAFKA_CLIENT_PASSWORDS": os.getenv("KAFKA_CLIENT_PASSWORDS", ""),
    "KAFKA_TOPIC_MODEL_PRODUCTION_VERSION": os.getenv(
        "KAFKA_TOPIC_MODEL_PRODUCTION_VERSION", ""
    ),
    "KAFKA_TOPIC_MODEL_PRODUCTION_ENDPOINT": os.getenv(
        "KAFKA_TOPIC_MODEL_PRODUCTION_ENDPOINT", ""
    ),
    "KAFKA_SASL_USERNAME": os.getenv("KAFKA_SASL_USERNAME", ""),
    "KAFKA_BOOTSTRAP": os.getenv("KAFKA_BOOTSTRAP", ""),
    "MLFLOW_ROOT_PREFIX": os.getenv("MLFLOW_ROOT_PREFIX", ""),
    "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME", ""),
    "S3_HOST": os.getenv("S3_HOST", ""),
    "S3_ACCESS_KEY": os.getenv("S3_ACCESS_KEY", ""),
    "S3_SECRET_KEY": os.getenv("S3_SECRET_KEY", ""),
    "S3_REGION_NAME": os.getenv("S3_REGION_NAME", ""),
}

kafka_args = {
    "bootstrap_servers": required["KAFKA_BOOTSTRAP"],
    "group_id": "my_group",
    "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
    "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
    "auto_offset_reset": "earliest",
    "enable_auto_commit": False,
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "SCRAM-SHA-256",
    "sasl_plain_username": required["KAFKA_SASL_USERNAME"],
    "sasl_plain_password": required["KAFKA_CLIENT_PASSWORDS"],
    "retries": 3,
    "request_timeout_ms": 40000,
}

missing = [k for k, v in required.items() if not v]

if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

consumer = KafkaConsumer(
    required["KAFKA_TOPIC_MODEL_PRODUCTION_VERSION"],
    **{
        k: v
        for k, v in kafka_args.items()
        if k not in ("value_serializer", "retries", "request_timeout_ms")
    },
)

producer = KafkaProducer(
    **{
        k: v
        for k, v in kafka_args.items()
        if k
        not in (
            "group_id",
            "value_deserializer",
            "auto_offset_reset",
            "enable_auto_commit",
        )
    }
)


def prepare_packed_conda_env(experiment_id: str, run_id: str) -> None:
    log.info("Packing conda env for exp=%s run=%s", experiment_id, run_id)
    s3_base = [
        "s3cmd",
        f"--access_key={required['S3_ACCESS_KEY']}",
        f"--secret_key={required['S3_SECRET_KEY']}",
        f"--host-bucket=%(bucket)s.{required['S3_HOST']}",
        f"--region={required['S3_REGION_NAME']}",
    ]

    s3url_conda = (
        f"s3://{required['S3_BUCKET_NAME']}/{required['MLFLOW_ROOT_PREFIX']}/"
        f"{experiment_id}/{run_id}/artifacts/estimator/conda.yaml"
    )

    s3url_env = (
        f"s3://{required['S3_BUCKET_NAME']}/{required['MLFLOW_ROOT_PREFIX']}/"
        f"{experiment_id}/{run_id}/artifacts/estimator/environment.tar.gz"
    )

    get_conda_yaml = s3_base + ["get", "--skip-existing", s3url_conda]
    create_conda_env = ["conda", "env", "create", "--file=conda.yaml"]
    pack_conda_env = [
        "conda-pack",
        "-n",
        "mlflow-env",
        "-o",
        "environment.tar.gz",
    ]
    put_packed_env = s3_base + ["put", "environment.tar.gz", s3url_env]

    log.info("Downloading conda.yaml from %s", s3url_conda)
    run_command(
        get_conda_yaml, timeout_seconds=10
    )  # Given that conda.yaml is very small in size, 10 seconds should be more than enough

    log.info("Creating conda env from conda.yaml")
    run_command(
        create_conda_env, timeout_seconds=120
    )  # Packing the conda env might take a bit longer, so we give it 2 minutes here

    log.info("Packing conda env into environment.tar.gz")
    run_command(
        pack_conda_env, timeout_seconds=60
    )  # Packing the conda env should be quick, so 1 minute should be enough

    log.info("Uploading packed conda env to %s", s3url_env)
    run_command(
        put_packed_env, timeout_seconds=30
    )  # Uploading the packed env should be quick, so 30 seconds should be enough

    log.info("Done packing conda env. Cleaning up local files.")

    os.remove("conda.yaml")
    os.remove("environment.tar.gz")


def process_message(
    message: any,
    create_model_endpoint: str = CREATE_MODEL_ENDPOINT,
    producer: KafkaProducer = producer,
    required_env: dict = required,
    retry_interval_seconds: int = RETRY_INTERVAL_SECONDS,
    timeout_seconds: int = TIMEPOUT_SECONDS,
) -> None:
    """Process a single Kafka message."""
    experiment_id = message.value.get("experiment_id")
    run_id = message.value.get("run_id")
    if not experiment_id or not run_id:
        log.warning(
            "Skipping message without experiment_id or run_id: %s", message.value
        )
        return

    prepare_packed_conda_env(experiment_id, run_id)
    required_env.update(
        {"MLFLOW_EXPERIMENT_ID": experiment_id, "MLFLOW_RUN_ID": run_id}
    )

    log.info("Deploying model for exp=%s run=%s", experiment_id, run_id)
    rendered_yaml = create_model_endpoint.fill_k8s_resource_template(required_env)

    log.info("Deploying to Kubernetes and creating an endpoint")
    inference_service_name, inference_service_namespace = (
        create_model_endpoint.create_resource_on_k8s(rendered_yaml)
    )

    log.info("Checking the status of the deployed endpoint")
    is_running = True
    time.sleep(retry_interval_seconds)
    start_time = time.time()

    while not create_model_endpoint.check_k8s_pods_running(
        prefix=inference_service_name, namespace=inference_service_namespace
    ):
        if time.time() - start_time > timeout_seconds:
            is_running = False
            break
        log.info("Waiting for pods to be in 'Running' state...")
        time.sleep(retry_interval_seconds)

    if not is_running:
        log.error(
            "Timeout waiting for pods to be in 'Running' state for exp=%s run=%s",
            experiment_id,
            run_id,
        )
        return

    log.info("Created model endpoint for exp=%s run=%s", experiment_id, run_id)
    log.info(
        "Notifying via Kafka topic %s",
        required_env["KAFKA_TOPIC_MODEL_PRODUCTION_ENDPOINT"],
    )
    producer.send(
        required_env["KAFKA_TOPIC_MODEL_PRODUCTION_ENDPOINT"],
        {
            "experiment_id": experiment_id,
            "run_id": run_id,
            "inference_service_name": inference_service_name,
            "inference_service_namespace": inference_service_namespace,
            "status": "Running",
            "message": "Model endpoint is up and running",
        },
    )


if __name__ == "__main__":
    # This will run indefinitely, listening for messages
    for message in consumer:
        process_message(message)
        log.info("Commit the Kafka offset")
        consumer.commit()
