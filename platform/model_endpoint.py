from string import Template
import yaml
from typing import Tuple
from kubernetes import config, client
from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException


class CreateModelEndpoint:
    def __init__(self):
        config.load_incluster_config()  # or config.load_kube_config() for local testing
        k8s_client = ApiClient()  # Shared client instance
        self.core = client.CoreV1Api(
            k8s_client
        )  # CoreV1Api instance for core resources
        self.crd = client.CustomObjectsApi(
            k8s_client
        )  # CustomObjectsApi instance for CRDs

    def check_k8s_pods_running(
        self,
        prefix: str = "iris-classifier",
        namespace: str = "default",
    ):
        """
        Returns True if all pods with the specified prefix are in Running state.
        """
        pods = self.core.list_namespaced_pod(namespace).items
        return all(
            p.status.phase == "Running"
            for p in pods
            if p.metadata.name.startswith(prefix)
        )

    def create_resource_on_k8s(self, rendered_yaml: str) -> Tuple[str, str]:
        # For convenience, return the name and namespace of the created InferenceService
        inference_service_name = None
        inference_service_namespace = None

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
                inference_service_name = name
                inference_service_namespace = namespace
                try:
                    # Server-side apply creates or updates in one call
                    self.crd.patch_namespaced_custom_object(
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
                        self.crd.create_namespaced_custom_object(
                            group, version, namespace, plural, doc
                        )
                    else:
                        raise

            # ---- Core resources: Secret / ServiceAccount (use SSA PATCH) ----
            if api_version == "v1" and kind == "Secret":
                try:
                    self.core.patch_namespaced_secret(
                        name=name, namespace=namespace, body=doc
                    )
                except ApiException as e:
                    if e.status == 404:
                        self.core.create_namespaced_secret(
                            namespace=namespace, body=doc
                        )
                    else:
                        raise

            if api_version == "v1" and kind == "ServiceAccount":
                try:
                    self.core.patch_namespaced_service_account(
                        name=name, namespace=namespace, body=doc
                    )
                except ApiException as e:
                    if e.status == 404:
                        self.core.create_namespaced_service_account(
                            namespace=namespace, body=doc
                        )
                    else:
                        raise

        return inference_service_name, inference_service_namespace

    @staticmethod
    def fill_k8s_resource_template(values: dict) -> str:
        with open("kserve_template.yaml", "r", encoding="utf-8") as f:
            tpl = Template(f.read())

        rendered_yaml = tpl.substitute(
            values
        )  # raises KeyError if any placeholder missing

        return rendered_yaml
