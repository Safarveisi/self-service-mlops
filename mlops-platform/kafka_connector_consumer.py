"""
This is where we consume a Kafka topic that contains
the specifications of models to be deployed using KServe.
Using these specifications, we adjust the models' artifact
paths on S3 before finalizing the deployment.
"""

import json
import os
import time

import boto3
import botocore
from boto3.resources.factory import ServiceResource
from kafka import KafkaConsumer, KafkaProducer
from model_endpoint import CreateModelEndpoint
from utils import get_logger, run_command

log = get_logger()

RETRY_INTERVAL_SECONDS = 10  # Interval between checks for pod status
TIMEOUT_SECONDS = 600  # Timeout for waiting for pods to be in 'Running' state

required = {
    "KAFKA_CLIENT_PASSWORDS": os.getenv("KAFKA_CLIENT_PASSWORDS", ""),
    "KAFKA_TOPIC_MODEL_PRODUCTION_VERSION": os.getenv("KAFKA_TOPIC_MODEL_PRODUCTION_VERSION", ""),
    "KAFKA_TOPIC_MODEL_PRODUCTION_ENDPOINT": os.getenv("KAFKA_TOPIC_MODEL_PRODUCTION_ENDPOINT", ""),
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


def prepare_packed_conda_env(s3: ServiceResource, experiment_id: str, run_id: str) -> str:
    log.info("Preparing conda env for exp=%s run=%s", experiment_id, run_id)

    s3key_conda = (
        f"{required['MLFLOW_ROOT_PREFIX']}/{experiment_id}/{run_id}/artifacts/estimator/conda.yaml"
    )

    s3key_env = (
        f"{required['MLFLOW_ROOT_PREFIX']}/{experiment_id}/{run_id}/artifacts/"
        "estimator/environment.tar.gz"
    )
    create_conda_env = ["conda", "env", "create", "--file=conda.yaml"]
    pack_conda_env = [
        "conda-pack",
        "-n",
        "mlflow-env",
        "-o",
        "environment.tar.gz",
    ]

    try:
        log.info("Downloading conda.yaml from %s", s3key_conda)
        s3.Bucket(required["S3_BUCKET_NAME"]).download_file(s3key_conda, "conda.yaml")
        log.info("Downloaded conda.yaml file")

        log.info("Creating conda env from conda.yaml")
        run_command(
            create_conda_env, timeout_seconds=120
        )  # Packing the conda env might take a bit longer, so we give it 2 minutes here
        log.info("Packing conda env into environment.tar.gz")
        run_command(
            pack_conda_env, timeout_seconds=60
        )  # Packing the conda env should be quick, so 1 minute should be enough

        log.info("Uploading packed conda env to %s", s3key_env)
        try:
            s3.Bucket(required["S3_BUCKET_NAME"]).upload_file("./environment.tar.gz", s3key_env)
            log.info("Uploaded environment.tar.gz into S3")
            log.info("Preparing conda env was successful. Cleaning up local files.")
            os.remove("conda.yaml")
            os.remove("environment.tar.gz")
            return "success"
        except boto3.exceptions.S3UploadFailedError as e:
            log.error(f"Uploading environment.tar.gz failed: {e}")
            log.error("Preparing conda env failed")
            return "failed"
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            log.error("The conda.yaml does not exist")
        else:
            log.error("There was an error downloading conda.yaml")
        log.error("Preparing conda env failed")
        return "failed"


def process_message(
    message: any,
    producer: KafkaProducer,
    s3: ServiceResource,
    create_model_endpoint: CreateModelEndpoint = CreateModelEndpoint(),
    required_env: dict = required,
    retry_interval_seconds: int = RETRY_INTERVAL_SECONDS,
    timeout_seconds: int = TIMEOUT_SECONDS,
) -> str:
    """Process a single Kafka message."""
    experiment_id = message.value.get("experiment_id")
    run_id = message.value.get("run_id")
    if not experiment_id or not run_id:
        log.warning("Skipping message without experiment_id or run_id: %s", message.value)
        return "skipped"

    result = prepare_packed_conda_env(s3, experiment_id, run_id)
    if result == "failed":
        return "skipped"

    required_env.update({"MLFLOW_EXPERIMENT_ID": experiment_id, "MLFLOW_RUN_ID": run_id})

    log.info("Deploying model for exp=%s run=%s", experiment_id, run_id)
    rendered_yaml = create_model_endpoint.fill_k8s_resource_template(required_env)
    if rendered_yaml is None:
        return "skipped"

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
        log.warning(
            "Timeout waiting for pods to be in 'Running' state for exp=%s run=%s",
            experiment_id,
            run_id,
        )
        return "timeout"

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
    return "success"


if __name__ == "__main__":
    session = boto3.Session(
        region_name=required["S3_REGION_NAME"],
        aws_access_key_id=required["S3_ACCESS_KEY"],
        aws_secret_access_key=required["S3_SECRET_KEY"],
    )

    s3 = session.resource(
        service_name="s3",
        endpoint_url=(
            "http://" + required["S3_HOST"]
            if not required["S3_HOST"].startswith("http://")
            else required["S3_HOST"]
        ),
        use_ssl=False,
    )

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

    # This will run indefinitely, listening for messages
    for message in consumer:
        result = process_message(message, producer, s3)
        log.info(f"Commit the Kafka offset. Result of processing: {result}")
        consumer.commit()
        log.info("Removed message from the Kafka topic")
