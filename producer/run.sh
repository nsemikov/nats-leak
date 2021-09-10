#!/usr/bin/env bash

set -e

MAX_INSTANCE_COUNT=${1}
MAX_INSTANCE_COUNT=${MAX_INSTANCE_COUNT:=2}

MESSAGES_COUNT=${2}
MESSAGES_COUNT=${MESSAGES_COUNT:=1000}

go build main.go
for (( x=0; x<${MAX_INSTANCE_COUNT}; x++ )); do
    echo "===== ./main -range=1..${MESSAGES_COUNT}"
    ./main -range=1..${MESSAGES_COUNT}
done
