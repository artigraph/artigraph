#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

trailers=$(git interpret-trailers --parse "$1")
if ! grep -q "Signed-off-by:" <<<"$trailers"; then
    echo "ERROR: The required DCO signoff was not found in the commit message." >&2
    echo "" >&2
    echo "If this contribution adheres to the Developer Certificate of Origin (DCO),"
    echo "commit again with the '--signoff' flag. If you are unsure, review:"
    echo "    https://developercertificate.org"
    exit 1
fi
