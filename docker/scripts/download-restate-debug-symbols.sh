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

# The docker image has a symlink from /usr/local/bin/.debug/restate-server.debug -> /tmp/.debug/restate-server.debug
# We determine the debug symbols directory based on writability (/usr/local/bin may be read only)
# First try to remove the symlink - if this fails, the filesystem is read-only
if rm -f /usr/local/bin/.debug/restate-server.debug 2>/dev/null; then
    DEBUG_DIR="/usr/local/bin/.debug"
else
    # Symlink can't be overwritten, try to write to /tmp instead
    DEBUG_DIR="/tmp/.debug"
    mkdir -p "$DEBUG_DIR"
fi

echo "Downloading debug symbols for restate-server BUILD_ID to $DEBUG_DIR"

curl --compressed -sL https://4bafe29d-dd3e-4c65-ba1d-54be833b2b69.debuginfod.polarsignals.com/buildid/BUILD_ID/debuginfo -o "$DEBUG_DIR/restate-server.debug"
