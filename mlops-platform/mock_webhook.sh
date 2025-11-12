#!/bin/bash

# Example curl command to test the webhook endpoint
# Prerequisites: Make sure webhook-handler service in kafka namespace is exposed externally via ingress

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
