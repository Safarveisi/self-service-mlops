#!/bin/bash

STARTING_PATH=$(git rev-parse --show-toplevel)
IDENTIFIER_COMMENT="# LATEST_IMAGE_TAG"


function get_required_python_version {
    cat pyproject.toml | grep -oE 'requires-python = ">=([0-9]+\.[0-9]+)' | sed 's/.*">=//'
}

function get_project_version {
    echo "v$(cat pyproject.toml | grep '^version =' | sed -E 's/version = //' | tr -d '"=')"
}

function update_docker_image_tag {
    VERSION_TAG=$(get_project_version)
    # Update docker image tags in the manifest files
    find "$STARTING_PATH/mlops-platform" -type f \( -name "*.yaml" \) -exec grep -l "$IDENTIFIER_COMMENT" {} \; | while read -r file; do
        echo "Updating: $file"
        sed -i "s|\(\s*.*:\s*\).* \($IDENTIFIER_COMMENT\)|\1"${@:-$VERSION_TAG}" \2|" "$file";
    done
}

function help {
    echo "$0 <task> [args]"
    echo "Tasks:"
    compgen -A function | cat -n
}

TIMEFORMAT="Task completed in %3lR"
time ${@:-help}
