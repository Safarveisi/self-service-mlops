#!/bin/bash

function kafka:create_interact_pod() {
    # Create a pod and use it to interact with the Kafka cluster
    kubectl run kafka-mlflow-kafka-client --restart='Never' --image docker.io/bitnami/kafka:4.0.0-debian-12-r10 --namespace kafka --command -- sleep infinity
}

function kafka:access_interact_pod_bash() {
    # You can get the password by (make sure the secret name is up-to-date)
    password=$(kubectl get secret kafka-mlflow-kafka-user-passwords --namespace kafka -o jsonpath='{.data.client-passwords}' | base64 -d | cut -d , -f 1)

    # Copy client.properties into the pod's container
    # client.properties should contain the following lines
    cat > client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="user1" password="$password";
EOF

    kubectl cp --namespace kafka ./client.properties kafka-mlflow-kafka-client:/tmp/client.properties
    # Access the container bash shell
    kubectl exec --tty -i kafka-mlflow-kafka-client --namespace kafka -- bash

    ### Examples ###

    # Make sure the bootstrap server is up-to-date before running the following commands

    # List all topics in the cluster
    # kafka-topics.sh --bootstrap-server kafka-mlflow-kafka.kafka.svc.cluster.local:9092 -command-config /tmp/client.properties --list

    # Produce a message into a topic
    # kafka-console-producer.sh \
        # --producer.config /tmp/client.properties \
        # --bootstrap-server kafka-mlflow-kafka.kafka.svc.cluster.local:9092 \
        # --topic test

    # Consume a message from a topic
    # kafka-console-consumer.sh \
        # --consumer.config /tmp/client.properties \
        # --bootstrap-server kafka-mlflow-kafka.kafka.svc.cluster.local:9092 \
        # --topic test \
        # --from-beginning
}

function kafka:delete_interact_pod() {
    # Delete the pod after you're done
    kubectl delete pod kafka-mlflow-kafka-client --namespace kafka
}

function help {
    echo "$0 <task> [args]"
    echo "Tasks:"
    compgen -A function | cat -n
}

TIMEFORMAT="Task completed in %3lR"
time ${@:-help}
