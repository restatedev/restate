# Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
# All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# NB this version is also pinned in dist-workspace.toml for release binary builds
FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:1.17.0 AS planner
COPY . .
RUN just chef-prepare

FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:1.17.0 AS base
COPY --from=planner /restate/recipe.json recipe.json
COPY justfile justfile

# avoid sharing sccache port between multiplatform builds - they share a network but not a filesystem, so it won't work
FROM base AS base-amd64
ARG SCCACHE_SERVER_PORT=4226

FROM base AS base-arm64
ARG SCCACHE_SERVER_PORT=4227

FROM base-$TARGETARCH AS builder
ARG SCCACHE_SERVER_PORT
ARG TARGETARCH

ENV RUSTC_WRAPPER=/usr/bin/sccache
ENV SCCACHE_DIR=/var/cache/sccache

# Adding new env vars here to get a build working? They're probably also needed in release-build-setup.yml!

# Overrides the behaviour of the release profile re including debug symbols, which in our repo is not to include them.
# Should be set to 'false' or 'true'. See https://doc.rust-lang.org/cargo/reference/environment-variables.html
ARG CARGO_PROFILE_RELEASE_DEBUG=false
# Avoids feature unification by building the three binaries individually
ARG RESTATE_FEATURES=''
RUN just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES chef-cook --release -p restate-cli -p restate-doctor -p restatectl
COPY . .

RUN --mount=type=cache,target=/var/cache/sccache \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build --release -p restate-cli -p restate-doctor -p restatectl && \
    just notice-file && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/release/restatectl target/restatectl && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/release/restate target/restate && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/release/restate-doctor target/restate-doctor;

FROM debian:trixie-slim AS tools
RUN apt-get update && apt-get install --no-install-recommends -y \
    jq curl iproute2 iputils-ping tcpdump procps htop sysstat iotop \
    less ca-certificates file neovim binutils linux-perf du-dust hwinfo \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /restate/NOTICE /NOTICE
COPY --from=builder /restate/LICENSE /LICENSE
# copy OS roots
COPY --from=builder /etc/ssl /etc/ssl
COPY --from=builder /restate/target/restatectl /usr/local/bin
COPY --from=builder /restate/target/restate /usr/local/bin
COPY --from=builder /restate/target/restate-doctor /usr/local/bin
WORKDIR /
CMD [ "/bin/bash" ]
