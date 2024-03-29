#!/usr/bin/env sh
#
# Copied from https://stackoverflow.com/a/46536244

NAME="$(git config user.name)"
EMAIL="$(git config user.email)"

if [ -z "$NAME" ]; then
    echo "empty git config user.name"
    exit 1
fi

if [ -z "$EMAIL" ]; then
    echo "empty git config user.email"
    exit 1
fi

git interpret-trailers \
    --if-exists doNothing \
    --trailer "Signed-off-by: $NAME <$EMAIL>" \
    --in-place "$1"
