#!/usr/bin/env bash

set -e

MAX_INSTANCE_COUNT=${1}
MAX_INSTANCE_COUNT=${MAX_INSTANCE_COUNT:=2}

TIMEOUT=${2}
TIMEOUT=${TIMEOUT:=5s}

DELIVER_POLICY=${3}
DELIVER_POLICY=${DELIVER_POLICY:=last}

go build main.go
for (( x=0; x<${MAX_INSTANCE_COUNT}; x++ )); do
    echo "===== ./main -close-timeout=${TIMEOUT} -deliver-policy=${DELIVER_POLICY}"
    ./main -close-timeout=${TIMEOUT} -deliver-policy=${DELIVER_POLICY}
done
