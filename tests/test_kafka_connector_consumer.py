import types
from unittest.mock import Mock

import pytest


@pytest.fixture(autouse=True)
def env(monkeypatch):
    # Provide all required env vars so module import doesn't raise
    monkeypatch.setenv("KAFKA_CLIENT_PASSWORDS", "pw")
    monkeypatch.setenv("KAFKA_TOPIC_MODEL_PRODUCTION_VERSION", "model.version")
    monkeypatch.setenv("KAFKA_TOPIC_MODEL_PRODUCTION_ENDPOINT", "model.endpoint")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "user1")
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "kafka:9092")
    monkeypatch.setenv("MLFLOW_ROOT_PREFIX", "mlflow_dev")
    monkeypatch.setenv("S3_BUCKET_NAME", "bucket")
    monkeypatch.setenv("S3_HOST", "s3.local")
    monkeypatch.setenv("S3_ACCESS_KEY", "ak")
    monkeypatch.setenv("S3_SECRET_KEY", "sk")
    monkeypatch.setenv("S3_REGION_NAME", "eu-central-1")


@pytest.fixture(autouse=True)
def mock_kubernetes(monkeypatch):
    # Import the real package names to patch their attributes
    import kubernetes.client as kclient
    import kubernetes.config as kconf

    # No-op both config loaders (so import-time calls don't explode)
    monkeypatch.setattr(kconf, "load_incluster_config", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(kconf, "load_kube_config", lambda *a, **k: None, raising=True)

    # Provide dummy API classes so any instantiation succeeds
    class DummyApi:
        def __getattr__(self, name):
            # Any method call will be a harmless lambda
            return lambda *a, **k: None

    monkeypatch.setattr(kclient, "CoreV1Api", lambda *a, **k: DummyApi(), raising=True)
    monkeypatch.setattr(kclient, "CustomObjectsApi", lambda *a, **k: DummyApi(), raising=True)


def import_module(monkeypatch):
    """
    Import the target module safely:
    - mock kubernetes classes in its namespace
    """
    # We cannot set builtins in the module; instead, patch in its import path
    # Do a first lightweight import to locate the package name if needed.
    # Here we assume the file is promote_to_production.py at repo root.
    import mlops_platform.kafka_connector_consumer as mod

    return mod


def test_prepare_packed_conda_env_happy_path(monkeypatch):
    # Import after env fixture applied
    import kafka_connector_consumer as mod

    # Mock run_command to capture calls
    calls = []

    def fake_run_command(cmd, timeout_seconds=None):
        calls.append((tuple(cmd), timeout_seconds))
        return 0

    monkeypatch.setattr(mod, "run_command", fake_run_command)

    # Mock file removal so we don't touch the FS
    removed = []
    monkeypatch.setattr(mod.os, "remove", lambda p: removed.append(p))

    # Execute
    mod.prepare_packed_conda_env("69", "abcdef")

    # Assert the expected external commands were invoked in order
    cmd_seq = [c[0] for c in calls]
    # 1) s3cmd get conda.yaml
    assert any(cmd[0] == "s3cmd" and "get" in cmd for cmd in cmd_seq)
    # 2) conda env create
    assert any(cmd[:3] == ("conda", "env", "create") for cmd in cmd_seq)
    # 3) conda-pack
    assert any(cmd[0] == "conda-pack" for cmd in cmd_seq)
    # 4) s3cmd put environment.tar.gz
    assert any(cmd[0] == "s3cmd" and "put" in cmd for cmd in cmd_seq)

    # Cleanup should remove the two local files
    assert sorted(removed) == ["conda.yaml", "environment.tar.gz"]


def test_process_message_happy_path(monkeypatch):
    import kafka_connector_consumer as mod

    # Stub out the external command runner & file removals
    monkeypatch.setattr(mod, "run_command", lambda *a, **k: 0)
    monkeypatch.setattr(mod.os, "remove", lambda p: None)

    # Optionally, short-circuit the whole env pack step:
    # monkeypatch.setattr(mod, "prepare_packed_conda_env", lambda *a, **k: None)

    # Fake endpoint creator behavior
    cme = Mock()
    cme.fill_k8s_resource_template.return_value = "yaml"
    cme.create_resource_on_k8s.return_value = ("svc-name", "ns")
    cme.check_k8s_pods_running.return_value = True  # already running

    # Fake producer
    producer = Mock()

    # Message & env copy
    msg = types.SimpleNamespace(value={"experiment_id": "69", "run_id": "abcdef"})
    env_copy = mod.required.copy()

    status = mod.process_message(
        msg,
        producer,
        create_model_endpoint=cme,
        required_env=env_copy,
    )

    assert status == "success"
    cme.fill_k8s_resource_template.assert_called_once()
    cme.create_resource_on_k8s.assert_called_once_with("yaml")
    cme.check_k8s_pods_running.assert_called()
    producer.send.assert_called_once()
    assert env_copy["MLFLOW_EXPERIMENT_ID"] == "69"
    assert env_copy["MLFLOW_RUN_ID"] == "abcdef"


def test_process_message_skips_when_missing_ids(monkeypatch):
    import kafka_connector_consumer as mod

    # Prepare a minimal message without run_id
    message = types.SimpleNamespace(value={"experiment_id": "123"})

    # Mocks to ensure nothing heavy is called
    cme = Mock()
    producer = Mock()

    status = mod.process_message(
        message,
        producer,
        create_model_endpoint=cme,
        required_env=mod.required.copy(),  # pass a copy to avoid mutating global
    )

    assert status == "skipped"
    cme.fill_k8s_resource_template.assert_not_called()
    cme.create_resource_on_k8s.assert_not_called()
    producer.send.assert_not_called()


def test_process_message_timeout_path(monkeypatch):
    import kafka_connector_consumer as mod

    # Stub out the external command runner & file removals
    monkeypatch.setattr(mod, "run_command", lambda *a, **k: 0)
    monkeypatch.setattr(mod.os, "remove", lambda p: None)

    cme = Mock()
    cme.fill_k8s_resource_template.return_value = "yaml"
    cme.create_resource_on_k8s.return_value = ("svc-name", "ns")
    # Always false -> never reaches Running
    cme.check_k8s_pods_running.return_value = False

    producer = Mock()

    msg = types.SimpleNamespace(value={"experiment_id": "69", "run_id": "abcdef"})

    status = mod.process_message(
        msg,
        producer,
        create_model_endpoint=cme,
        required_env=mod.required.copy(),
        timeout_seconds=5,  # shorten to keep test quick
        retry_interval_seconds=1,
    )

    assert status == "timeout"
    producer.send.assert_not_called()
