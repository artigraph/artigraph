#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

IMAGE="artigraph/example-spend"
VERSION="$(python3 -c 'from arti.internal import version; print(version)')"

docker build --build-arg VERSION="${VERSION}" -t "${IMAGE}" .
docker push "${IMAGE}"
