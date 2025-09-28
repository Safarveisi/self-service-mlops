import os
from typing import List
from utils import get_logger, run_command
from model_endpoint import deploy_to_kubernetes, fill_template
import json
from kafka import KafkaConsumer

log = get_logger()


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


required = {
    "KAFKA_CLIENT_PASSWORDS": os.getenv("KAFKA_CLIENT_PASSWORDS", ""),
    "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", ""),
    "KAFKA_SASL_USERNAME": os.getenv("KAFKA_SASL_USERNAME", ""),
    "KAFKA_BOOTSTRAP": os.getenv("KAFKA_BOOTSTRAP", ""),
    "MLFLOW_ROOT_PREFIX": os.getenv("MLFLOW_ROOT_PREFIX", ""),
    "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME", ""),
    "S3_HOST": os.getenv("S3_HOST", ""),
    "S3_ACCESS_KEY": os.getenv("S3_ACCESS_KEY", ""),
    "S3_SECRET_KEY": os.getenv("S3_SECRET_KEY", ""),
    "S3_REGION_NAME": os.getenv("S3_REGION_NAME", ""),
}

missing = [k for k, v in required.items() if not v]

if missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

consumer = KafkaConsumer(
    required["KAFKA_TOPIC"],
    bootstrap_servers=required["KAFKA_BOOTSTRAP"],
    group_id="my_group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=required["KAFKA_SASL_USERNAME"],
    sasl_plain_password=required["KAFKA_CLIENT_PASSWORDS"],
)

# This will run indefinitely, listening for messages
for message in consumer:
    # Read the message value (assuming it's JSON)
    experiment_id = message.value.get("experiment_id")
    run_id = message.value.get("run_id")
    if not experiment_id or not run_id:
        log.warning(
            "Skipping message without experiment_id or run_id: %s", message.value
        )
        consumer.commit()  # Still commit the offset to avoid reprocessing
        continue
    prepare_packed_conda_env(experiment_id, run_id)
    required.update(
        {
            "MLFLOW_EXPERIMENT_ID": experiment_id,
            "MLFLOW_RUN_ID": run_id,
        }
    )
    log.info("Deploying model for exp=%s run=%s", experiment_id, run_id)
    log.info("Filling Kserve template")
    rendered_yaml = fill_template(required)
    log.info("Deploying to Kubernetes and creating an endpoint")
    deploy_to_kubernetes(rendered_yaml)
    log.info("Deployed model for exp=%s run=%s", experiment_id, run_id)
    consumer.commit()  # Manually commit the offset after processing the message
