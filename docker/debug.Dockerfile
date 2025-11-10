# Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
# All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:1.14.7 AS planner
COPY . .
RUN just chef-prepare

FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:1.14.7 AS base
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

# todo only enable those env variables when cross compiling
# Set krb5 cross-compilation env variables (because we cannot run cross compiled tests)
ENV krb5_cv_attr_constructor_destructor=yes
ENV ac_cv_func_regcomp=yes
ENV ac_cv_printf_positional=yes

# todo only enable this env variable when cross compiling
# Set sasl2-sys cross-compilation env variables (because we cannot run cross compiled tests)
ENV ac_cv_gssapi_supports_spnego=yes

# Avoids feature unification by building the three binaries individually
ARG BUILD_INDIVIDUALLY=false
ARG RESTATE_FEATURES=''
RUN if [ "$BUILD_INDIVIDUALLY" = "true" ]; then \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES chef-cook -p restate-cli && \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES chef-cook -p restate-server && \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES chef-cook -p restatectl; \
    else \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES chef-cook -p restate-cli -p restate-server -p restatectl; \
    fi
COPY . .
# Mount the sccache directory as a cache to leverage sccache during build
# Caching layer if nothing has changed
# Use sccache during the main build
RUN --mount=type=cache,target=/var/cache/sccache \
    if [ "$BUILD_INDIVIDUALLY" = "true" ]; then \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build -p restate-cli && \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build -p restate-server && \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build -p restatectl; \
    else \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build -p restate-cli -p restate-server -p restatectl; \
    fi && \
    just notice-file && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/debug/restate-server target/restate-server && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/debug/restatectl target/restatectl && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/debug/restate target/restate

# We do not need the Rust toolchain to run the server binary!
FROM debian:trixie-slim AS runtime
COPY --from=builder /restate/target/restate-server /usr/local/bin
COPY --from=builder /restate/target/restatectl /usr/local/bin
COPY --from=builder /restate/target/restate /usr/local/bin
COPY --from=builder /restate/NOTICE /NOTICE
COPY --from=builder /restate/LICENSE /LICENSE
# copy OS roots
COPY --from=builder /etc/ssl /etc/ssl
# useful for health checks
RUN apt-get update && apt-get install -y jq curl && rm -rf /var/lib/apt/lists/*
WORKDIR /
ENTRYPOINT ["/usr/local/bin/restate-server"]
