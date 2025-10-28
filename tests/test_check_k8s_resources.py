import math
from types import SimpleNamespace
from unittest import mock

import pytest
from check_k8s_resources import (
    can_deploy_replicas,
    get_node_schedulable_free,
    parse_cpu,
    parse_memory,
)


def _make_node(name: str, cpu: str = "0", mem: str = "0"):
    """Return a minimal node object."""
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        status=SimpleNamespace(allocatable={"cpu": cpu, "memory": mem}),
    )


def _make_container(name: str, cpu_req=None, mem_req=None, resources_present=True):
    """Return a minimal container object."""
    if not resources_present:
        return SimpleNamespace(name=name, resources=None)

    req = {}
    if cpu_req is not None:
        req["cpu"] = cpu_req
    if mem_req is not None:
        req["memory"] = mem_req

    resources = SimpleNamespace(requests=req)
    return SimpleNamespace(name=name, resources=resources)


def _make_state(running: bool):
    """ContainerState-like object - only `.running` matters."""
    return SimpleNamespace(running=SimpleNamespace() if running else None)


def _make_container_status(name: str, running: bool):
    """ContainerStatus-like object."""
    return SimpleNamespace(name=name, state=_make_state(running))


def _make_pod(
    name: str,
    node_name: str,
    *,
    containers=None,
    init_containers=None,
    container_statuses=None,
    init_statuses=None,
):
    """Return a minimal pod object."""
    containers = containers or []
    init_containers = init_containers or []
    container_statuses = container_statuses or []
    init_statuses = init_statuses or []

    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        spec=SimpleNamespace(
            node_name=node_name,
            containers=containers,
            init_containers=init_containers,
        ),
        status=SimpleNamespace(
            container_statuses=container_statuses,
            init_container_statuses=init_statuses,
        ),
    )


@pytest.fixture(autouse=True)
def patch_k8s_and_logger(monkeypatch):
    """
    * Stub out ``config.load_kube_config`` / ``load_incluster_config``.
    * Replace ``client.CoreV1Api`` with a fake that returns whatever the test
      puts in ``patch_k8s_and_logger.nodes`` / ``.pods``.
    * Swallow logger calls (so the test output stays clean).
    The real ``parse_cpu`` / ``parse_memory`` are **not** mocked - the
    implementation you already have is used.
    """
    import check_k8s_resources as mod

    # config loading
    load_kube_config = mock.Mock()
    load_incluster_config = mock.Mock()
    load_kube_config.return_value = None

    monkeypatch.setattr(mod.config, "load_kube_config", load_kube_config)
    monkeypatch.setattr(mod.config, "load_incluster_config", load_incluster_config)

    # CoreV1Api - returns the data we store on the fixture object
    class _FakeCoreV1Api:
        def __init__(self):
            # they will be filled by each test case
            self._nodes = patch_k8s_and_logger.nodes
            self._pods = patch_k8s_and_logger.pods

        def list_node(self):
            return SimpleNamespace(items=self._nodes)

        def list_pod_for_all_namespaces(self):
            return SimpleNamespace(items=self._pods)

    monkeypatch.setattr(mod.client, "CoreV1Api", lambda: _FakeCoreV1Api())

    # placeholders that the individual tests will replace
    patch_k8s_and_logger.nodes = []
    patch_k8s_and_logger.pods = []

    # ---- logger ---------------------------------------------------------
    dummy_log = mock.Mock()
    monkeypatch.setattr(mod, "log", dummy_log)

    # expose the mocked config functions so tests can make assertions
    return {
        "load_kube_config": load_kube_config,
        "load_incluster_config": load_incluster_config,
        "log": dummy_log,
        "nodes": patch_k8s_and_logger.nodes,
        "pods": patch_k8s_and_logger.pods,
    }


@pytest.mark.parametrize(
    ("input_s", "expected"),
    [
        ("250m", 0.25),  # standard milli format
        ("100m", 0.1),
        ("2000m", 2.0),  # large milli -> cores
        ("12.5m", 0.0125),  # nonstandard but supported: float before 'm'
        ("12m", 0.012),  # integer before 'm'
        ("0m", 0.0),  # zero milli
        ("2", 2.0),  # integer cores
        ("0.5", 0.5),  # fractional cores
        ("  250m  ", 0.25),  # whitespace trimming
        ("abc123.5def", 123.5),  # fallback: extract number with decimal
        ("abc123mxyz", 0.123),  # fallback + milli suffix -> extracted number divided by 1000
        ("12.5", 12.5),  # plain float
    ],
)
def test_parse_cpu_various(input_s, expected):
    val = parse_cpu(input_s)
    assert math.isclose(val, expected, rel_tol=1e-9), f"{input_s!r} -> {val}, want {expected}"


def test_parse_cpu_none_and_empty():
    # function casts None -> "None" then falls back to no digits -> 0.0
    assert parse_cpu(None) == 0.0
    assert parse_cpu("") == 0.0


def test_parse_cpu_no_digits_returns_zero():
    assert parse_cpu("no-digits-here") == 0.0


def test_parse_cpu_lone_m_returns_zero():
    # "m" or strings that end with 'm' but contain no numeric prefix should return 0.0
    assert parse_cpu("m") == 0.0
    assert parse_cpu("  m  ") == 0.0


def test_parse_cpu_malformed_but_extracts_first_number():
    # For strings like 'prefix12.34suffixm' the parser should use the first numeric token
    assert math.isclose(parse_cpu("prefix12.34suffixm"), 0.01234, rel_tol=1e-9)


@pytest.mark.parametrize(
    ("input_s", "expected"),
    [
        ("128Mi", 128 * 1024**2),
        ("1Gi", 1 * 1024**3),
        ("123Ki", 123 * 1024),
        ("1024", 1024.0),  # plain integer bytes
        (1024, 1024.0),  # numeric input
        ("5Ti", 5 * 1024**4),
        ("1Pi", 1 * 1024**5),
        ("  64Mi  ", 64 * 1024**2),  # whitespace trimming
    ],
)
def test_parse_memory_units(input_s, expected):
    val = parse_memory(input_s)
    assert math.isclose(val, expected, rel_tol=1e-9), f"{input_s} -> {val}, want {expected}"


def test_parse_memory_fallback_extracts_decimal_number():
    # fallback should pick up decimal numbers in arbitrary strings
    assert math.isclose(parse_memory("foo123.5bar"), 123.5, rel_tol=1e-9)


def test_parse_memory_empty_or_no_digits():
    assert parse_memory("") == 0.0
    assert parse_memory("no-digits") == 0.0


def test_parse_memory_zero():
    assert parse_memory("0") == 0.0


def test_simple_free_calculation(patch_k8s_and_logger):
    """
    One node (4 CPU, 8 GiB) and a *running* container that asks for
    1 CPU and 2 GiB.  Expected free: 3 CPU, 6 GiB.
    """
    # arrange
    node = _make_node("node-a", cpu="4", mem="8Gi")
    patch_k8s_and_logger["nodes"].extend([node])

    container = _make_container("c-1", cpu_req="1", mem_req="2Gi")
    status = _make_container_status("c-1", running=True)

    pod = _make_pod(
        name="pod-a",
        node_name="node-a",
        containers=[container],
        container_statuses=[status],
    )
    patch_k8s_and_logger["pods"].extend([pod])

    # act
    result = get_node_schedulable_free()

    # assert
    expected = {"node-a": {"cpu_free": 3.0, "mem_free": 6 * 1024**3}}
    assert result == expected


def test_non_running_containers_are_ignored(patch_k8s_and_logger):
    """
    Containers (both init and regular) that are **not** running must not affect
    the free-resource calculation.
    """
    node = _make_node("node-c", cpu="3", mem="6Gi")
    patch_k8s_and_logger["nodes"].extend([node])

    # init container (waiting) → should be ignored
    init_cont = _make_container("init-wait", cpu_req="1", mem_req="1Gi")
    init_status = _make_container_status("init-wait", running=False)

    # regular container (waiting) → should be ignored
    cont = _make_container("app-wait", cpu_req="2", mem_req="2Gi")
    cont_status = _make_container_status("app-wait", running=False)

    pod = _make_pod(
        name="pod-c",
        node_name="node-c",
        init_containers=[init_cont],
        containers=[cont],
        init_statuses=[init_status],
        container_statuses=[cont_status],
    )
    patch_k8s_and_logger["pods"].extend([pod])

    result = get_node_schedulable_free()

    expected = {"node-c": {"cpu_free": 3.0, "mem_free": 6 * 1024**3}}
    assert result == expected


def test_init_container_counts_when_running(patch_k8s_and_logger):
    """
    Verify that an **init** container contributes to the request total only when
    its status reports ``running=True``.
    """
    node = _make_node("node-b", cpu="2", mem="4Gi")
    patch_k8s_and_logger["nodes"].extend([node])

    # init container (running) → 0.5 CPU, 1 GiB
    init_cont = _make_container("init-x", cpu_req="500m", mem_req="1Gi")
    init_status = _make_container_status("init-x", running=True)

    # regular container (running) → 0.3 CPU, 512 MiB
    cont = _make_container("app-y", cpu_req="300m", mem_req="512Mi")
    cont_status = _make_container_status("app-y", running=True)

    pod = _make_pod(
        name="pod-b",
        node_name="node-b",
        init_containers=[init_cont],
        containers=[cont],
        init_statuses=[init_status],
        container_statuses=[cont_status],
    )
    patch_k8s_and_logger["pods"].extend([pod])

    result = get_node_schedulable_free()

    # Expected free resources
    expected_cpu_free = 2.0 - (0.5 + 0.3)  # 2 - 0.8 = 1.2
    expected_mem_free = 4 * (1024**3) - 1024**3 - (512 * 1024**2)  # 4Gi - 1.5Gi

    assert result == {"node-b": {"cpu_free": expected_cpu_free, "mem_free": expected_mem_free}}


def test_total_capacity_insufficient(monkeypatch):
    """
    The cluster-wide total free resources are smaller than the request,
    so the function should fail *before* trying any placement.
    """
    # patch the external fixture manually (no need for the fixture decorator)
    import check_k8s_resources as mod

    monkeypatch.setattr(
        mod,
        "get_node_schedulable_free",
        lambda: {
            "node-1": {"cpu_free": 1.0, "mem_free": 512 * 1024**2},  # 0.5 GiB
            "node-2": {"cpu_free": 0.5, "mem_free": 256 * 1024**2},  # 0.25 GiB
        },
    )

    # Request 2 replicas, each 1 CPU / 1 GiB → clearly impossible
    result1 = can_deploy_replicas(
        replicas=2,
        cpu_per_replica=1,  # integer works as well
        mem_per_replica="1Gi",
    )

    # Request 3 replicas, each 0.5 CPU / 400 MiB → also impossible
    result2 = can_deploy_replicas(
        replicas=3,
        cpu_per_replica="500m",
        mem_per_replica="400Mi",
    )

    assert result1 is False
    assert result2 is False


def test_total_capacity_sufficient(monkeypatch):
    """
    The cluster-wide total free resources are smaller than the request,
    so the function should fail *before* trying any placement.
    """
    # patch the external fixture manually (no need for the fixture decorator)
    import check_k8s_resources as mod

    monkeypatch.setattr(
        mod,
        "get_node_schedulable_free",
        lambda: {
            "node-1": {"cpu_free": 2, "mem_free": 2 * 1024**3},  # 2 GiB
            "node-2": {"cpu_free": 2, "mem_free": 2 * 1024**3},  # 2 GiB
        },
    )

    # Request 2 replicas, each 1 CPU / 500 MiB → clearly possible
    result = can_deploy_replicas(
        replicas=2,
        cpu_per_replica="1Gi",  # integer works as well
        mem_per_replica="500Mi",
    )
    assert result is True
