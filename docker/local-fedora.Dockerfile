# Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
# All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# We do not need the Rust toolchain to run the server binary!
FROM fedora:latest

# Copy the locally built restate-server binary
COPY restate-server /usr/local/bin

WORKDIR /
ENTRYPOINT ["/usr/local/bin/restate-server"]