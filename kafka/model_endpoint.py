from string import Template
import yaml
from kubernetes import config, client
from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException


def deploy_to_kubernetes(rendered_yaml: str) -> None:
    config.load_incluster_config()  # or config.load_kube_config() for local testing
    k8s_client = ApiClient()
    core = client.CoreV1Api(k8s_client)
    crd = client.CustomObjectsApi(k8s_client)

    for doc in yaml.safe_load_all(rendered_yaml):
        if not doc:
            continue

        api_version = str(doc.get("apiVersion", ""))
        kind = str(doc.get("kind", ""))
        meta = doc.get("metadata", {}) or {}
        namespace = meta.get("namespace", "default")
        name = meta.get("name")

        # ---- KServe CRD: InferenceService (use SSA PATCH) ----
        if (
            api_version.startswith("serving.kserve.io/")
            and kind.lower() == "inferenceservice"
        ):
            group = "serving.kserve.io"
            version = api_version.split("/", 1)[1]  # e.g., v1beta1
            plural = "inferenceservices"

            try:
                # Server-side apply creates or updates in one call
                crd.patch_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    name=name,
                    body=doc,
                )
            except ApiException as e:
                if e.status == 404:
                    # Fallback (older servers): create
                    crd.create_namespaced_custom_object(
                        group, version, namespace, plural, doc
                    )
                else:
                    raise

        # ---- Core resources: Secret / ServiceAccount (use SSA PATCH) ----
        if api_version == "v1" and kind == "Secret":
            try:
                core.patch_namespaced_secret(name=name, namespace=namespace, body=doc)
            except ApiException as e:
                if e.status == 404:
                    core.create_namespaced_secret(namespace=namespace, body=doc)
                else:
                    raise

        if api_version == "v1" and kind == "ServiceAccount":
            try:
                core.patch_namespaced_service_account(
                    name=name, namespace=namespace, body=doc
                )
            except ApiException as e:
                if e.status == 404:
                    core.create_namespaced_service_account(
                        namespace=namespace, body=doc
                    )
                else:
                    raise


def fill_template(values: dict) -> str:
    with open("kserve_template.yaml", "r", encoding="utf-8") as f:
        tpl = Template(f.read())

    rendered_yaml = tpl.substitute(values)  # raises KeyError if any placeholder missing

    return rendered_yaml
