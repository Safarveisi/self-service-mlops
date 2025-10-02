# Self service MLOps platform

In this project, we will build a self-service MLOps platform that enables data scientists to seamlessly deploy their machine-learning models for inference. The design below demonstrates how this can be achieved using AWS services; in practice, we will implement the solution with open-source technologies that provide equivalent capabilities.

Platform created via AWS services

![self service mlops on aws](pngs/platform_aws.png)

Equivalent platform via open source tools and technologies

![self service mlops via open source](pngs/platform_open_source.png)

### Deploy managed kubernetes cluster

```bash
cd terraform/k8s
terraform init
terraform apply --auto-approve
```

### Deploy helm charts

Update `values.yaml` in `helm-charts/` with your custom settings.

#### Kafka

```bash
cd helm-charts/kafka
helm upgrade --install -n kafka kafka ./kafka -f ./kafka/values.yaml --create-namespace
```
#### Monitoring (EFK stack + Prometheus and Grafana)

```bash
cd helm-charts/monitoring
# EFK stack (mind the execution order)
helm upgrade --install -n monitoring elasticsearch ./elasticsearch -f ./elasticsearch/values.yaml --create-namespace
helm upgrade --install -n monitoring filebeat ./filebeat -f ./filebeat/values.yaml
# Make sure you have ingress-nginx controller installed (we access kibana UI through the specified host - see values.yaml)
helm upgrade --install -n ingress-nginx ingress-nginx ./ingress-nginx -f ./ingress-nginx/values.yaml --create-namespace
helm upgrade --install -n monitoring kibana ./kibana -f ./kibana/values.yaml
# Prometheus + Grafana
helm upgrade --install -n prometheus prometheus ./kube-prometheus-stack -f ./kube-prometheus-stack/values.yaml --create-namespace
```

### Create model inference endpoint (similar to AWS SageMaker endpoint)

#### Kserve

Install [Kserve](https://github.com/kserve/kserve) by running the commands below one by one. You may choose a different KServe release, but ensure it’s compatible with your Kubernetes version. For a step-by-step guide, see the [serverless installation docs](https://kserve.github.io/website/docs/admin-guide/serverless).

```bash
# Knative
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.17.0/serving-crds.yaml
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.17.0/serving-core.yaml

# Networking layer
kubectl apply -f https://github.com/knative/net-istio/releases/download/knative-v1.17.0/istio.yaml
kubectl apply -f https://github.com/knative/net-istio/releases/download/knative-v1.17.0/net-istio.yaml

# Cert manager
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.3/cert-manager.yaml

# Kserve
kubectl apply --server-side -f https://github.com/kserve/kserve/releases/download/v0.14.1/kserve.yaml
kubectl apply --server-side -f https://github.com/kserve/kserve/releases/download/v0.14.1/kserve-cluster-resources.yaml
```

> [!NOTE]
> KServe brings several benefits — most notably, you can deploy many models without building and pushing your own Docker image. Just point KServe to your model artifacts and use the appropriate runtime.

### Repository Guide

| Directory         | What it’s for / How to use                                                                                                                                                                                                         |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `model-endpoint/` | Sample inference client and payloads to exercise a KServe InferenceService. After deploying your model, `cd model-endpoint` and run the provided script (e.g., `prediction`) to send a test request and print the prediction.      |
| `platform/`       | Cluster/platform-level scaffolding and configuration that supports the self-service MLOps setup (base manifests, operators, helper assets). Use this to organize reusable, cluster-wide components independent of specific models. |
