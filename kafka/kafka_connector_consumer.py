import os
from typing import Optional
from threading import Timer
import shlex, subprocess
import yaml
import json
from kafka import KafkaConsumer


def run_command(cmd, timeout_seconds, env=None):
    """
    Runs the specified command. If it exits with non-zero status, `RuntimeError` is raised.
    """
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    timer = Timer(timeout_seconds, proc.kill)
    try:
        timer.start()
        stdout, stderr = proc.communicate()
        stdout = stdout.decode("utf-8")
        stderr = stderr.decode("utf-8")
        if proc.returncode != 0:
            msg = "\n".join(
                [
                    f"Encountered an unexpected error while running {cmd}",
                    f"exit status: {proc.returncode}",
                    f"stdout: {stdout}",
                    f"stderr: {stderr}",
                ]
            )
            raise RuntimeError(msg)
    finally:
        if timer.is_alive():
            timer.cancel()


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
    # Get the conda.yaml file from run artifacts in S3
    cmd_get_conda_yaml = (
        f"s3cmd --access_key={required['S3_ACCESS_KEY']} --secret_key={required['S3_SECRET_KEY']} "
        f"--host-bucket=%(bucket)s.{required['S3_HOST']} --region={required['S3_REGION_NAME']} "
        "get --skip-existing "
        f"s3://{required['S3_BUCKET_NAME']}/{required['MLFLOW_ROOT_PREFIX']}/{experiment_id}/{run_id}/artifacts/estimator/conda.yaml"
    )
    args = shlex.split(cmd_get_conda_yaml) # Safely split the command into arguments
    run_command(
        args, timeout_seconds=5
    )  # Given that conda.yaml is very small in size, 5 seconds should be more than enough

    try:
        with open("conda.yaml", "r") as f:
            conda_env = yaml.safe_load(f)
        print("Conda environment:", conda_env)
        # Remove conda.yaml after reading
        os.remove("conda.yaml")
    except Exception as e:
        print(f"Unexpected error: {e}")

    consumer.commit()  # Manually commit the offset after processing the message
