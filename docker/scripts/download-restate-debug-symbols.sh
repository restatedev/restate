#!/bin/bash

set -euo pipefail

echo "Downloading debug symbols for restate-server BUILD_ID to /usr/local/bin/.debug"
mkdir -p /usr/local/bin/.debug/
curl --compressed -sL https://4bafe29d-dd3e-4c65-ba1d-54be833b2b69.debuginfod.polarsignals.com/buildid/BUILD_ID/debuginfo -o /usr/local/bin/.debug/restate-server.debug
