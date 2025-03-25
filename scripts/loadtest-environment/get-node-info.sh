#!/usr/bin/env bash

set -euf -o pipefail -x

STACK_NAME=$(jq -r 'keys | .[0]' cdk-outputs.json)

aws ssm get-parameters \
    --name "$(jq -r --arg stack_name "${STACK_NAME}" '.[$stack_name].KeyArn' cdk-outputs.json)" \
    --with-decryption --query "Parameters[0].Value" --output text > private-key.pem
chmod 600 private-key.pem

jq -r --arg stack_name "${STACK_NAME}" \
  '.[$stack_name] | to_entries | sort_by(.key) | .[] | select(.key | test("^Node")) | .value' \
  cdk-outputs.json > nodes.txt
