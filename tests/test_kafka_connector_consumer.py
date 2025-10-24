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


# Provide dummy API classes so any instantiation succeeds
class DummyApi:
    def __getattr__(self, name):
        # Any method call will be a harmless lambda
        return lambda *a, **k: None


@pytest.fixture(autouse=True)
def mock_kubernetes(monkeypatch):
    # Import the real package names to patch their attributes
    import kubernetes.client as kclient
    import kubernetes.config as kconf

    # No-op both config loaders (so import-time calls don't explode)
    monkeypatch.setattr(kconf, "load_incluster_config", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(kconf, "load_kube_config", lambda *a, **k: None, raising=True)

    monkeypatch.setattr(kclient, "CoreV1Api", lambda *a, **k: DummyApi(), raising=True)
    monkeypatch.setattr(kclient, "CustomObjectsApi", lambda *a, **k: DummyApi(), raising=True)


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

    s3 = Mock()

    # Execute
    mod.prepare_packed_conda_env(s3, "69", "abcdef")

    # Assert the expected external commands were invoked in order
    cmd_seq = [c[0] for c in calls]
    # 1) conda env create
    assert any(cmd[:3] == ("conda", "env", "create") for cmd in cmd_seq)
    # 2) conda-pack
    assert any(cmd[0] == "conda-pack" for cmd in cmd_seq)

    # Cleanup should remove the two local files
    assert sorted(removed) == ["conda.yaml", "environment.tar.gz"]


def test_process_message_happy_path(monkeypatch):
    import kafka_connector_consumer as mod

    # Stub out the external command runner & file removals
    monkeypatch.setattr(mod, "run_command", lambda *a, **k: 0)
    monkeypatch.setattr(mod.os, "remove", lambda p: None)
    monkeypatch.setattr(mod, "prepare_packed_conda_env", lambda *a, **k: "success")
    monkeypatch.setattr(
        mod,
        "get_model_endpoint_configuration",
        lambda *a, **k: {"concurrency": 4, "max_replicas": 2, "cpu": "2", "memory": "1000Mi"},
    )
    monkeypatch.setattr(mod, "validate_endpoint_config", lambda *a, **k: True)
    monkeypatch.setattr(mod, "can_deploy_replicas", lambda *a, **k: True)

    # Fake endpoint creator behavior
    cme = Mock()
    cme.fill_k8s_resource_template.return_value = "yaml"
    cme.create_resource_on_k8s.return_value = ("svc-name", "ns")
    cme.check_k8s_pods_running.return_value = True  # already running

    # Fake producer
    producer = Mock()
    s3 = Mock()

    # Message & env copy
    msg = types.SimpleNamespace(value={"experiment_id": "69", "run_id": "abcdef"})
    env_copy = mod.required.copy()

    status = mod.process_message(
        msg,
        producer,
        s3,
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
    s3 = Mock()

    status = mod.process_message(
        message,
        producer,
        s3,
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
    monkeypatch.setattr(mod, "prepare_packed_conda_env", lambda *a, **k: "success")
    monkeypatch.setattr(
        mod,
        "get_model_endpoint_configuration",
        lambda *a, **k: {"concurrency": 4, "max_replicas": 2, "cpu": "2", "memory": "1000Mi"},
    )
    monkeypatch.setattr(mod, "validate_endpoint_config", lambda *a, **k: True)
    monkeypatch.setattr(mod, "can_deploy_replicas", lambda *a, **k: True)

    cme = Mock()
    cme.fill_k8s_resource_template.return_value = "yaml"
    cme.create_resource_on_k8s.return_value = ("svc-name", "ns")
    # Always false -> never reaches Running
    cme.check_k8s_pods_running.return_value = False

    producer = Mock()
    s3 = Mock()

    msg = types.SimpleNamespace(value={"experiment_id": "69", "run_id": "abcdef"})

    status = mod.process_message(
        msg,
        producer,
        s3,
        create_model_endpoint=cme,
        required_env=mod.required.copy(),
        timeout_seconds=5,  # shorten to keep test quick
        retry_interval_seconds=1,
    )

    assert status == "timeout"
    producer.send.assert_not_called()


def test_process_message_prepare_conda_env_skipped(monkeypatch):
    import kafka_connector_consumer as mod

    monkeypatch.setattr(mod, "prepare_packed_conda_env", lambda *a, **k: "failed")
    monkeypatch.setattr(mod, "get_model_endpoint_configuration", lambda *a, **k: None)
    monkeypatch.setattr(mod, "validate_endpoint_config", lambda *a, **k: False)

    cme = Mock()
    producer = Mock()
    s3 = Mock()

    msg = types.SimpleNamespace(value={"experiment_id": "69", "run_id": "abcdef"})

    status = mod.process_message(
        msg,
        producer,
        s3,
        create_model_endpoint=cme,
        required_env=mod.required.copy(),
    )

    assert status == "skipped"
    producer.send.assert_not_called()


def test_validate_endpoint_config():
    import kafka_connector_consumer as mod

    config1 = {"concurrency": "10", "max_replicas": 3, "cpu": "2", "memory": "4Gi"}
    result1 = mod.validate_endpoint_config(config1)
    config2 = {"concurrency": "10", "cpu": "2", "memory": "4Gi"}
    result2 = mod.validate_endpoint_config(config2)
    config3 = {"concurrency": 10, "max_replicas": 3, "cpu": "2", "memory": "4Gi"}
    result3 = mod.validate_endpoint_config(config3)
    assert not result1
    assert not result2
    assert result3
