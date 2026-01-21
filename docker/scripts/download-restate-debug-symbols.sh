#!/bin/bash

#
# Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
# All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#

set -euo pipefail

echo "Downloading debug symbols for restate-server BUILD_ID to /usr/local/bin/.debug"
mkdir -p /usr/local/bin/.debug/
curl --compressed -sL https://4bafe29d-dd3e-4c65-ba1d-54be833b2b69.debuginfod.polarsignals.com/buildid/BUILD_ID/debuginfo -o /usr/local/bin/.debug/restate-server.debug
