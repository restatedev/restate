#!/usr/bin/env bash
#
# Copyright (c) 2024 - Restate Software, Inc., Restate GmbH
#
# This file is part of the Restate load test environment,
# which is released under the MIT license.
#
# You can find a copy of the license in file LICENSE in the
# scripts/loadtest-environment directory of this repository, or at
# https://github.com/restatedev/retate/blob/main/scripts/loadtest-environment/LICENSE
#

# The standard saturation test for moderate capacity instances. Make sure that a
# Restate server is started and the Counter service is already deployed. You'll
# want to run the test for at least several minutes in order to observe the
# effects of database compaction on performance. For cleaner results, wipe the
# restate-data directory between runs.
#
# 1. Start: restate-server --config-file .../restate-1.1.0-performance.toml --base-dir ../storage
# 2. Start: cargo run --release -p mock-service-endpoint --bin mock-service-endpoint
# 3. After clean startup, run: restate deployments register http://localhost:9080 --yes

set -e

wrk -t 8 -c 200 --latency -d5m -s get.lua http://127.0.0.1:8080
