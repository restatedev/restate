# Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
# All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

ARG UPLOAD_DEBUGINFO=false

# NB this version is also pinned in dist-workspace.toml for release binary builds
FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:1.16.1 AS planner
COPY . .
RUN just chef-prepare

FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:1.16.1 AS base
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

# todo only enable those env variables when cross compiling
# todo remove once https://github.com/MaterializeInc/rust-krb5-src/pull/30 has been merged and released
# Set krb5 cross-compilation env variables (because we cannot run cross compiled tests)
ENV krb5_cv_attr_constructor_destructor=yes
ENV ac_cv_func_regcomp=yes
ENV ac_cv_printf_positional=yes

# todo only enable this env variable when cross compiling
# todo remove once https://github.com/MaterializeInc/rust-sasl/pull/55 has been merged and released
# Set sasl2-sys cross-compilation env variables (because we cannot run cross compiled tests)
ENV ac_cv_gssapi_supports_spnego=yes

# Overrides the behaviour of the release profile re including debug symbols, which in our repo is not to include them.
# Should be set to 'false' or 'true'. See https://doc.rust-lang.org/cargo/reference/environment-variables.html
ARG CARGO_PROFILE_RELEASE_DEBUG=false
# Avoids feature unification by building the three binaries individually
ARG BUILD_INDIVIDUALLY=false
ARG RESTATE_FEATURES=''
RUN if [ "$BUILD_INDIVIDUALLY" = "true" ]; then \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES chef-cook --release -p restate-cli && \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES chef-cook --release -p restate-server && \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES chef-cook --release -p restatectl; \
    else \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES chef-cook --release -p restate-cli -p restate-server -p restatectl; \
    fi
COPY . .

FROM builder AS upload-false
RUN --mount=type=cache,target=/var/cache/sccache \
    if [ "$BUILD_INDIVIDUALLY" = "true" ]; then \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build --release -p restate-cli && \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build --release -p restate-server && \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build --release -p restatectl; \
    else \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build --release -p restate-cli -p restate-server -p restatectl; \
    fi && \
    just notice-file && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/release/restate-server target/restate-server && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/release/restatectl target/restatectl && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/release/restate target/restate
RUN cp docker/scripts/no-restate-debug-symbols.sh target/download-restate-debug-symbols.sh

FROM builder AS upload-true
RUN --mount=type=secret,id=parca --mount=type=cache,target=/var/cache/sccache \
    # useful so that binaries can tell if they actually were compiled with debug symbols
    export DEBUG_STRIPPED="true" && \
    if [ "$BUILD_INDIVIDUALLY" = "true" ]; then \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build --release -p restate-cli && \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build --release -p restate-server && \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build --release -p restatectl; \
    else \
    just arch=$TARGETARCH libc=gnu features=$RESTATE_FEATURES build --release -p restate-cli -p restate-server -p restatectl; \
    fi && \
    just notice-file && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/release/restate-server target/restate-server && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/release/restatectl target/restatectl && \
    mv target/$(just arch=$TARGETARCH libc=gnu print-target)/release/restate target/restate && \
    $(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-objcopy --only-keep-debug target/restate-server target/restate-server.debug && \
    $(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-objcopy --strip-debug target/restate-server && \
    $(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-objcopy --add-gnu-debuglink=target/restate-server.debug target/restate-server && \
    $(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-objcopy --strip-debug target/restatectl && \
    $(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-objcopy --strip-debug target/restate && \
    parca-debuginfo upload --store-address=grpc.polarsignals.com:443 --bearer-token-file=/run/secrets/parca target/restate-server.debug && \
    rm target/restate-server.debug
RUN cp docker/scripts/download-restate-debug-symbols.sh target/ && \
    sed -i"" "s/BUILD_ID/$($(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-readelf -n target/restate-server | awk '/Build/{print $3}')/g" target/download-restate-debug-symbols.sh

FROM upload-$UPLOAD_DEBUGINFO AS upload

# We do not need the Rust toolchain to run the server binary!
FROM debian:trixie-slim AS runtime
# useful for health checks
RUN apt-get update && apt-get install --no-install-recommends -y jq curl && rm -rf /var/lib/apt/lists/*
COPY --from=upload /restate/NOTICE /NOTICE
COPY --from=upload /restate/LICENSE /LICENSE
# copy OS roots
COPY --from=upload /etc/ssl /etc/ssl
COPY --from=upload /restate/target/restate-server /usr/local/bin
COPY --from=upload /restate/target/download-restate-debug-symbols.sh /usr/local/bin
COPY --from=upload /restate/target/restatectl /usr/local/bin
COPY --from=upload /restate/target/restate /usr/local/bin
WORKDIR /
ENTRYPOINT ["/usr/local/bin/restate-server"]
