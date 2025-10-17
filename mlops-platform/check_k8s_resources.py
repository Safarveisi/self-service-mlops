import re
from collections import defaultdict

from kubernetes import client, config
from utils import get_logger

log = get_logger()


def parse_cpu(cpu_str: str) -> float:
    """
    Return CPU as number of cores (float).
    Accepts formats like:
      - '250m'  -> 0.25
      - '12.5m' -> 0.0125
      - '2'     -> 2.0
      - nonstandard strings like 'cpu: 0.5' or 'abc123mxyz'
    """
    s = str(cpu_str).strip()
    if not s:
        return 0.0

    # Milli-cores (e.g. "250m", "12.5m")
    if "m" in s:
        try:
            return float(s[:-1]) / 1000.0
        except ValueError:
            # Fallback: extract numeric part before dividing
            nums = re.findall(r"[\d.]+", s)
            return float(nums[0]) / 1000.0 if nums else 0.0

    # Plain numeric cores (int or float)
    try:
        return float(s)
    except ValueError:
        # Fallback: extract number from messy strings
        nums = re.findall(r"[\d.]+", s)
        return float(nums[0]) if nums else 0.0


def parse_memory(mem_str: str) -> float:
    """Return memory in bytes. Accepts '128Mi', '1Gi', integer bytes, or numeric."""
    s = str(mem_str).strip()
    # Accept formats like 123Ki, 45Mi, 2Gi, or plain integer bytes
    m = re.match(r"^([0-9]+)([KMGTP]i)?$", s)
    units = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4, "Pi": 1024**5}
    if m:
        num, unit = m.groups()
        num = int(num)
        if not unit:
            return float(num)
        return float(num * units.get(unit, 1))
    # Fallback: extract number
    nums = re.findall(r"[\d.]+", s)
    return float(nums[0]) if nums else 0.0


def get_node_schedulable_free() -> dict[str, dict[str, float]]:
    """
    Returns a dict keyed by node name with values:
      {"cpu_free": cores (float), "mem_free": bytes (float)}
    using allocatable - sum(requests_on_node).

    NOTE: only considers containers that are currently in *running* state.
    """
    # load config
    try:
        config.load_kube_config()
        log.debug("Loaded kubeconfig from default location.")
    except Exception:
        config.load_incluster_config()
        log.debug("Loaded in-cluster kubeconfig.")

    v1 = client.CoreV1Api()

    # read nodes allocatable
    nodes = v1.list_node().items
    log.debug("Fetched %d nodes from the API.", len(nodes))
    node_alloc = {}
    for n in nodes:
        name = n.metadata.name
        alloc = n.status.allocatable or {}
        node_alloc[name] = {
            "cpu_alloc": parse_cpu(alloc.get("cpu", "0")),
            "mem_alloc": parse_memory(alloc.get("memory", "0")),
        }

    # helper to check if a given container (by name) is currently running on the pod
    def _is_container_running(pod, container_name: str, init: bool = False) -> bool:
        """
        Look up the matching ContainerStatus for the given container name and
        return True only if its state indicates running.
        """
        try:
            if init:
                statuses = pod.status.init_container_statuses or []
            else:
                statuses = pod.status.container_statuses or []
        except Exception:
            return False

        for st in statuses:
            if st.name == container_name:
                # ContainerState has attributes .running, .terminated, .waiting
                state = getattr(st, "state", None)
                if not state:
                    return False
                return getattr(state, "running", None) is not None
        return False

    # sum pod requests per node (only counting containers that are running)
    pods = v1.list_pod_for_all_namespaces().items
    log.info("Fetched %d pods from all namespaces.", len(pods))
    requests_by_node = defaultdict(lambda: {"cpu": 0.0, "mem": 0.0})
    for p in pods:
        node = p.spec.node_name
        if not node:
            continue
        # include init containers + containers (but only if the specific container is running)
        spec_init_containers = p.spec.init_containers or []
        spec_containers = p.spec.containers or []

        for c in spec_init_containers:
            # only include if init container is currently running
            if not _is_container_running(p, c.name, init=True):
                log.debug(
                    "Skipping init container %s in pod %s (not running)", c.name, p.metadata.name
                )
                continue
            if not c.resources:
                continue
            req = c.resources.requests or {}
            cpu_add = parse_cpu(req.get("cpu", 0))
            mem_add = parse_memory(req.get("memory", 0))
            log.info(
                "Init container %s (pod=%s) requests cpu: %s and memory: %s",
                c.name,
                p.metadata.name,
                cpu_add,
                mem_add,
            )
            requests_by_node[node]["cpu"] += cpu_add
            requests_by_node[node]["mem"] += mem_add

        for c in spec_containers:
            # only include if container is currently running
            if not _is_container_running(p, c.name, init=False):
                log.debug("Skipping container %s in pod %s (not running)", c.name, p.metadata.name)
                continue
            if not c.resources:
                continue
            req = c.resources.requests or {}
            cpu_add = parse_cpu(req.get("cpu", 0))
            mem_add = parse_memory(req.get("memory", 0))
            log.info(
                "Container %s (pod=%s) requests cpu: %s and memory: %s",
                c.name,
                p.metadata.name,
                cpu_add,
                mem_add,
            )
            requests_by_node[node]["cpu"] += cpu_add
            requests_by_node[node]["mem"] += mem_add

    # compute schedulable free
    free_by_node = {}
    for node, alloc in node_alloc.items():
        cpu_alloc = alloc["cpu_alloc"]
        mem_alloc = alloc["mem_alloc"]
        cpu_req = requests_by_node[node]["cpu"]
        mem_req = requests_by_node[node]["mem"]
        cpu_free = cpu_alloc - cpu_req
        mem_free = mem_alloc - mem_req
        # log per-node summary at debug level
        log.debug(
            "Node=%s alloc_cpu=%.3f alloc_mem=%d cpu_req=%.3f mem_req=%d cpu_free=%.3f mem_free=%d",
            node,
            cpu_alloc,
            int(mem_alloc),
            cpu_req,
            int(mem_req),
            cpu_free,
            int(mem_free),
        )
        free_by_node[node] = {"cpu_free": cpu_free, "mem_free": mem_free}

    return free_by_node


def can_deploy_replicas(
    replicas: int, cpu_per_replica: str, mem_per_replica: str, debug: bool = False
) -> bool:
    """
    Return True if `replicas` each requiring (cpu_per_replica, mem_per_replica)
    can be scheduled on the cluster using current schedulable free resources.

    cpu_per_replica: float (cores) or string like '250m' or '0.5'
    mem_per_replica: int (bytes) or string like '128Mi'
    """
    log.info(
        "Checking deployment feasibility: replicas=%d cpu_per_replica=%s mem_per_replica=%s",
        replicas,
        str(cpu_per_replica),
        str(mem_per_replica),
    )

    # normalize inputs
    cpu_need = parse_cpu(cpu_per_replica)
    mem_need = parse_memory(mem_per_replica)

    if cpu_need <= 0 or mem_need <= 0 or replicas <= 0:
        log.warning(
            "Invalid inputs: cpu_per_replica=%s mem_per_replica=%s replicas=%s",
            str(cpu_per_replica),
            str(mem_per_replica),
            str(replicas),
        )
        if debug:
            log.debug(
                "Parsed values: cpu_need=%.3f mem_need=%d replicas=%d",
                cpu_need,
                int(mem_need),
                replicas,
            )
        return False

    free_by_node = get_node_schedulable_free()
    # Convert to a mutable list of nodes with available resources
    nodes = [
        {"name": n, "cpu_free": v["cpu_free"], "mem_free": v["mem_free"]}
        for n, v in free_by_node.items()
    ]

    # quick total-capacity check (fast fail)
    total_cpu_free = sum(max(0.0, nd["cpu_free"]) for nd in nodes)
    total_mem_free = sum(max(0.0, nd["mem_free"]) for nd in nodes)
    log.debug(
        "Cluster totals: total_cpu_free=%.3f cores total_mem_free=%d bytes",
        total_cpu_free,
        int(total_mem_free),
    )

    if total_cpu_free < replicas * cpu_need or total_mem_free < replicas * mem_need:
        log.warning(
            "Cluster total free resources insufficient."
            " cpu_free=%.3f required=%.3f mem_free=%d required=%d",
            total_cpu_free,
            replicas * cpu_need,
            int(total_mem_free),
            int(replicas * mem_need),
        )
        return False

    # greedy placement: for each replica, put it on a node that fits both cpu and mem.
    # choose node with largest remaining cpu_free (heuristic to reduce fragmentation).
    placements: list[tuple[str, float, float]] = []  # (node, cpu_after, mem_after)
    for i in range(replicas):
        # sort nodes by descending cpu_free to try largest-first
        nodes.sort(key=lambda nd: nd["cpu_free"], reverse=True)
        placed = False
        for nd in nodes:
            if nd["cpu_free"] >= cpu_need and nd["mem_free"] >= mem_need:
                nd["cpu_free"] -= cpu_need
                nd["mem_free"] -= mem_need
                placements.append((nd["name"], nd["cpu_free"], nd["mem_free"]))
                placed = True
                log.debug(
                    "Placed replica %d on node %s. remaining cpu=%.3f mem=%d",
                    i + 1,
                    nd["name"],
                    nd["cpu_free"],
                    int(nd["mem_free"]),
                )
                break
        if not placed:
            log.warning(
                "Failed to place replica %d. No node has >= cpu %.3f and mem %d",
                i + 1,
                cpu_need,
                int(mem_need),
            )
            if debug:
                log.debug("Current node states at failure: %s", nodes)
            return False

    log.info("All %d replicas placed successfully.", replicas)
    if debug:
        log.debug("Example placements (first 10): %s", placements[:10])
    return True
