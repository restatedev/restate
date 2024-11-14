# syntax=docker.io/docker/dockerfile:1.7-labs
# Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
# All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This docker file is mainly used to build a local docker image
# with restate binary to run sdk tests

FROM ghcr.io/restatedev/dev-tools:latest AS builder
WORKDIR /usr/src/app
COPY --exclude=.git . .
RUN \
    --mount=type=cache,target=/usr/src/app/target,sharing=locked \
    --mount=type=cache,target=/usr/src/app/cargo-home,sharing=locked \
    du -sh target && du -sh cargo-home && \
    CARGO_HOME=/usr/src/app/cargo-home just libc=gnu build --bin restate-server && \
    cp target/debug/restate-server restate-server && \
    du -sh target && du -sh cargo-home

# We do not need the Rust toolchain to run the server binary!
FROM debian:bookworm-slim AS runtime
COPY --from=builder /usr/src/app/restate-server /usr/local/bin
# copy OS roots
COPY --from=builder /etc/ssl /etc/ssl
WORKDIR /
ENTRYPOINT ["/usr/local/bin/restate-server"]