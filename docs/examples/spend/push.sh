#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

IMAGE="artigraph/example-spend"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"

docker build --file "${SCRIPT_DIR}/Dockerfile" --tag "${IMAGE}" "${REPO_ROOT}"
docker push "${IMAGE}"
