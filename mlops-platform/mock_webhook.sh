#!/bin/bash

# Example curl command to test the webhook endpoint
# Prequisites: Make srue the K8s port-forwarding is running
# kubectl -n kafka port-forward svc/webhook-handler 8080:80

curl -i -X POST http://self-service-mlops.mlflow-webhook.com:80/mlflow/webhook/push \
  -H "Content-Type: application/json" \
  --data '{
    "name": "example_model",
    "version": "1",
    "key": "stage",
    "value": "Production",
    "run_id": "4528ae7bcd7947ef8495810fe22377ec",
    "experiment_id": "1"
  }'
