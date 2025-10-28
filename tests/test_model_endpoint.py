from types import SimpleNamespace
from unittest import mock

import pytest
from model_endpoint import CreateModelEndpoint


def _make_pod(name: str, phase: str):
    """Return a minimal pod object with the required attributes."""
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        status=SimpleNamespace(phase=phase),
    )


@pytest.fixture(autouse=True)
def patch_all(monkeypatch):
    """
    Returns a dict with the faked objects so the tests can make assertions
    about how they were used.
    """
    #  config
    dummy_cfg = mock.Mock()
    dummy_cfg.load_incluster_config = mock.Mock()
    dummy_cfg.load_kube_config = mock.Mock()
    monkeypatch.setattr("model_endpoint.config", dummy_cfg)

    #  ApiClient
    dummy_api_client = mock.Mock()
    monkeypatch.setattr("model_endpoint.ApiClient", lambda: dummy_api_client)

    #  logger
    dummy_log = mock.Mock()
    monkeypatch.setattr("model_endpoint.log", dummy_log)

    #  CoreV1Api
    core_calls = {
        "list_namespaced_pod": mock.Mock(),
        "patch_namespaced_secret": mock.Mock(),
        "create_namespaced_secret": mock.Mock(),
        "patch_namespaced_service_account": mock.Mock(),
        "create_namespaced_service_account": mock.Mock(),
    }

    class DummyCoreV1:
        def __init__(self, *_, **__):
            pass

        # All methods receive **kwargs so the mock records keyword args.
        def list_namespaced_pod(self, *args, **kwargs):
            return core_calls["list_namespaced_pod"](*args, **kwargs)

        def patch_namespaced_secret(self, **kwargs):
            return core_calls["patch_namespaced_secret"](**kwargs)

        def create_namespaced_secret(self, **kwargs):
            return core_calls["create_namespaced_secret"](**kwargs)

        def patch_namespaced_service_account(self, **kwargs):
            return core_calls["patch_namespaced_service_account"](**kwargs)

        def create_namespaced_service_account(self, **kwargs):
            return core_calls["create_namespaced_service_account"](**kwargs)

    monkeypatch.setattr("model_endpoint.client.CoreV1Api", DummyCoreV1)

    # CustomObjectsApi
    crd_calls = {
        "patch_namespaced_custom_object": mock.Mock(),
        "create_namespaced_custom_object": mock.Mock(),
    }

    class DummyCustomObjects:
        def __init__(self, *_, **__):
            pass

        def patch_namespaced_custom_object(self, **kwargs):
            return crd_calls["patch_namespaced_custom_object"](**kwargs)

        def create_namespaced_custom_object(self, *args, **kwargs):
            return crd_calls["create_namespaced_custom_object"](*args, **kwargs)

    monkeypatch.setattr("model_endpoint.client.CustomObjectsApi", DummyCustomObjects)

    # expose everything for the tests
    return {
        "config": dummy_cfg,
        "core_calls": core_calls,
        "crd_calls": crd_calls,
        "log": dummy_log,
    }


def test_check_k8s_pods_running_all_running(patch_all):
    """All pods that match the prefix are in Running state → True."""
    core_calls = patch_all["core_calls"]
    core_calls["list_namespaced_pod"].return_value = SimpleNamespace(
        items=[
            _make_pod(name="iris-classifier-1", phase="Running"),
            _make_pod(name="iris-classifier-2", phase="Running"),
            # a pod that does *not* match the prefix - should be ignored
            _make_pod(name="other-pod", phase="Pending"),
        ]
    )

    ce = CreateModelEndpoint()
    assert ce.check_k8s_pods_running(prefix="iris-classifier", namespace="default")
    # make sure we actually called the API with the correct namespace
    core_calls["list_namespaced_pod"].assert_called_once_with("default")


def test_check_k8s_pods_running_one_not_running(patch_all):
    """At least one matching pod is not Running → False."""
    core_calls = patch_all["core_calls"]
    core_calls["list_namespaced_pod"].return_value = SimpleNamespace(
        items=[
            _make_pod(name="iris-classifier-1", phase="Running"),
            _make_pod(name="iris-classifier-2", phase="Pending"),
        ]
    )

    ce = CreateModelEndpoint()
    assert not ce.check_k8s_pods_running(prefix="iris-classifier")
    core_calls["list_namespaced_pod"].assert_called_once_with("default")


def test_check_k8s_pods_running_no_matching_pods(patch_all):
    """If no pod matches the prefix the function returns False."""
    core_calls = patch_all["core_calls"]
    core_calls["list_namespaced_pod"].return_value = SimpleNamespace(items=[])
    ce = CreateModelEndpoint()
    assert not ce.check_k8s_pods_running()


def test_create_resource_inferenceservice_patch_success(patch_all):
    """Patch succeeds → name/namespace are returned."""
    crd_calls = patch_all["crd_calls"]
    # make the patch call succeed (no exception)
    crd_calls["patch_namespaced_custom_object"].return_value = {"status": "patched"}

    yaml_doc = """
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: my-svc
  namespace: demo-ns
spec: {}
"""
    ce = CreateModelEndpoint()
    name, ns = ce.create_resource_on_k8s(yaml_doc)

    assert name == "my-svc"
    assert ns == "demo-ns"

    # verify that the patch was called with the right arguments
    crd_calls["patch_namespaced_custom_object"].assert_called_once()
    args = crd_calls["patch_namespaced_custom_object"].call_args[1]  # kwargs
    assert args["group"] == "serving.kserve.io"
    assert args["version"] == "v1beta1"
    assert args["namespace"] == "demo-ns"
    assert args["plural"] == "inferenceservices"
    assert args["name"] == "my-svc"


def test_create_resource_inferenceservice_fallback_create(patch_all):
    """Patch raises 404 → fallback to create_namespaced_custom_object."""
    from kubernetes.client.exceptions import ApiException

    crd_calls = patch_all["crd_calls"]

    # First call (patch) raises a 404 ApiException
    def raise_404(**_):
        raise ApiException(status=404, reason="Not Found")

    crd_calls["patch_namespaced_custom_object"].side_effect = raise_404
    crd_calls["create_namespaced_custom_object"].return_value = {"status": "created"}

    yaml_doc = """
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: fallback-svc
spec: {}
"""
    ce = CreateModelEndpoint()
    name, ns = ce.create_resource_on_k8s(yaml_doc)

    assert name == "fallback-svc"
    assert ns == "default"  # namespace defaults to “default” when omitted
    crd_calls["create_namespaced_custom_object"].assert_called_once()
    # make sure the fallback was hit exactly once
    crd_calls["patch_namespaced_custom_object"].assert_called_once()


def test_create_resource_inferenceservice_error_logged(patch_all):
    """Patch raises a non-404 error → error is logged and no create is attempted."""
    from kubernetes.client.exceptions import ApiException

    crd_calls = patch_all["crd_calls"]

    def raise_500(**_):
        raise ApiException(status=500, reason="Internal Server Error")

    crd_calls["patch_namespaced_custom_object"].side_effect = raise_500

    yaml_doc = """
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: broken-svc
spec: {}
"""
    ce = CreateModelEndpoint()
    name, ns = ce.create_resource_on_k8s(yaml_doc)

    # Name/namespace are still extracted before the API call.
    assert name == "broken-svc"
    assert ns == "default"

    # The error branch should have called log.error exactly once.
    assert patch_all["log"].error.called
    crd_calls["create_namespaced_custom_object"].assert_not_called()


def test_secret_patch_success(patch_all):
    """A v1 Secret is patched when it already exists."""
    core_calls = patch_all["core_calls"]
    core_calls["patch_namespaced_secret"].return_value = {"status": "patched"}

    yaml_doc = """
apiVersion: v1
kind: Secret
metadata:
  name: my-secret
  namespace: secret-ns
data:
  key: dmFsdWU=
"""
    ce = CreateModelEndpoint()
    name, ns = ce.create_resource_on_k8s(yaml_doc)

    # No InferenceService in the document → returns (None, None)
    assert name is None
    assert ns is None
    core_calls["patch_namespaced_secret"].assert_called_once_with(
        name="my-secret", namespace="secret-ns", body=mock.ANY
    )
    core_calls["create_namespaced_secret"].assert_not_called()


def test_secret_fallback_create_on_404(patch_all):
    """When patch raises 404 the code falls back to create_namespaced_secret."""
    from kubernetes.client.exceptions import ApiException

    core_calls = patch_all["core_calls"]

    def raise_404(**_):
        raise ApiException(status=404, reason="Not Found")

    core_calls["patch_namespaced_secret"].side_effect = raise_404
    core_calls["create_namespaced_secret"].return_value = {"status": "created"}

    yaml_doc = """
apiVersion: v1
kind: Secret
metadata:
  name: new-secret
spec: {}
"""
    ce = CreateModelEndpoint()
    ce.create_resource_on_k8s(yaml_doc)

    core_calls["patch_namespaced_secret"].assert_called_once()
    core_calls["create_namespaced_secret"].assert_called_once_with(
        namespace="default", body=mock.ANY
    )


def test_serviceaccount_patch_success(patch_all):
    """A ServiceAccount is patched correctly."""
    core_calls = patch_all["core_calls"]
    core_calls["patch_namespaced_service_account"].return_value = {"status": "patched"}

    yaml_doc = """
apiVersion: v1
kind: ServiceAccount
metadata:
  name: my-sa
  namespace: sa-ns
"""
    ce = CreateModelEndpoint()
    ce.create_resource_on_k8s(yaml_doc)

    core_calls["patch_namespaced_service_account"].assert_called_once_with(
        name="my-sa", namespace="sa-ns", body=mock.ANY
    )
    core_calls["create_namespaced_service_account"].assert_not_called()


def test_serviceaccount_fallback_create_on_404(patch_all):
    """When patch raises 404 the ServiceAccount is created."""
    from kubernetes.client.exceptions import ApiException

    core_calls = patch_all["core_calls"]

    def raise_404(**_):
        raise ApiException(status=404, reason="Not Found")

    core_calls["patch_namespaced_service_account"].side_effect = raise_404
    core_calls["create_namespaced_service_account"].return_value = {"status": "created"}

    yaml_doc = """
apiVersion: v1
kind: ServiceAccount
metadata:
  name: new-sa
"""
    ce = CreateModelEndpoint()
    ce.create_resource_on_k8s(yaml_doc)

    core_calls["patch_namespaced_service_account"].assert_called_once()
    core_calls["create_namespaced_service_account"].assert_called_once_with(
        namespace="default", body=mock.ANY
    )


def test_fill_template_success(monkeypatch):
    """All placeholders are provided → the rendered string is returned."""
    dummy_template = "apiVersion: v1\nkind: Secret\nmetadata:\n  name: $name\n  namespace: $ns"
    mock_open = mock.mock_open(read_data=dummy_template)
    monkeypatch.setattr("builtins.open", mock_open)

    dummy_log = mock.Mock()
    monkeypatch.setattr("model_endpoint.log", dummy_log)

    rendered = CreateModelEndpoint.fill_k8s_resource_template({"name": "my-secret", "ns": "demo"})
    expected = dummy_template.replace("$name", "my-secret").replace("$ns", "demo")
    assert rendered == expected
    assert not dummy_log.error.called


def test_fill_template_missing_key(monkeypatch):
    """If a placeholder is missing the method returns None and logs an error."""
    dummy_template = "metadata:\n  name: $name\n  namespace: $ns"
    mock_open = mock.mock_open(read_data=dummy_template)
    monkeypatch.setattr("builtins.open", mock_open)

    dummy_log = mock.Mock()
    monkeypatch.setattr("model_endpoint.log", dummy_log)

    # Omit the "ns" key - substitution will raise KeyError internally
    result = CreateModelEndpoint.fill_k8s_resource_template({"name": "test"})
    assert result is None
    dummy_log.error.assert_called_once_with(
        "Some attributes were missing while trying to fill Kserve template!"
    )
