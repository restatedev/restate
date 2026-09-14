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
FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:1.18.0 AS planner
COPY . .
RUN just chef-prepare

FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:1.18.0 AS base
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
ARG UPLOAD_DEBUGINFO

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

# Cargo profile to build with. Must be a profile defined in the workspace Cargo.toml
# (e.g. release, release-lite, release-debug).
ARG CARGO_PROFILE=release
# Avoids feature unification by building the three binaries individually
ARG BUILD_INDIVIDUALLY=false
ARG RESTATE_FEATURES=''
RUN if [ "$UPLOAD_DEBUGINFO" = "true" ]; then \
    profile_env=$(printf '%s' "$CARGO_PROFILE" | tr '[:lower:]-' '[:upper:]_') && \
    export "CARGO_PROFILE_${profile_env}_DEBUG=true"; \
    fi && \
    if [ "$BUILD_INDIVIDUALLY" = "true" ]; then \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" chef-cook --profile="$CARGO_PROFILE" -p restate-cli && \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" chef-cook --profile="$CARGO_PROFILE" -p restate-server && \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" chef-cook --profile="$CARGO_PROFILE" -p restatectl; \
    else \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" chef-cook --profile="$CARGO_PROFILE" -p restate-cli -p restate-server -p restatectl; \
    fi
COPY . .

FROM builder AS upload-false
RUN --mount=type=cache,target=/var/cache/sccache \
    if [ "$BUILD_INDIVIDUALLY" = "true" ]; then \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" build --profile="$CARGO_PROFILE" -p restate-cli && \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" build --profile="$CARGO_PROFILE" -p restate-server && \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" build --profile="$CARGO_PROFILE" -p restatectl; \
    else \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" build --profile="$CARGO_PROFILE" -p restate-cli -p restate-server -p restatectl; \
    fi && \
    just notice-file && \
    case "$CARGO_PROFILE" in dev|test) profile_dir=debug ;; release|bench) profile_dir=release ;; *) profile_dir="$CARGO_PROFILE" ;; esac && \
    target_dir="target/$(just arch="$TARGETARCH" libc=gnu print-target)/$profile_dir" && \
    mv "$target_dir/restate-server" target/restate-server && \
    mv "$target_dir/restatectl" target/restatectl && \
    mv "$target_dir/restate" target/restate
RUN cp docker/scripts/no-restate-debug-symbols.sh target/download-restate-debug-symbols.sh

FROM builder AS upload-true
RUN --mount=type=secret,id=parca --mount=type=cache,target=/var/cache/sccache \
    # useful so that binaries can tell if they actually were compiled with debug symbols
    export DEBUG_STRIPPED="true" && \
    profile_env=$(printf '%s' "$CARGO_PROFILE" | tr '[:lower:]-' '[:upper:]_') && \
    export "CARGO_PROFILE_${profile_env}_DEBUG=true" && \
    if [ "$BUILD_INDIVIDUALLY" = "true" ]; then \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" build --profile="$CARGO_PROFILE" -p restate-cli && \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" build --profile="$CARGO_PROFILE" -p restate-server && \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" build --profile="$CARGO_PROFILE" -p restatectl; \
    else \
    just arch="$TARGETARCH" libc=gnu features="$RESTATE_FEATURES" build --profile="$CARGO_PROFILE" -p restate-cli -p restate-server -p restatectl; \
    fi && \
    just notice-file && \
    case "$CARGO_PROFILE" in dev|test) profile_dir=debug ;; release|bench) profile_dir=release ;; *) profile_dir="$CARGO_PROFILE" ;; esac && \
    target_dir="target/$(just arch="$TARGETARCH" libc=gnu print-target)/$profile_dir" && \
    mv "$target_dir/restate-server" target/restate-server && \
    mv "$target_dir/restatectl" target/restatectl && \
    mv "$target_dir/restate" target/restate && \
    $(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-objcopy --only-keep-debug target/restate-server target/restate-server.debug && \
    $(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-objcopy --strip-debug target/restate-server && \
    $(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-objcopy --add-gnu-debuglink=target/restate-server.debug target/restate-server && \
    $(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-objcopy --strip-debug target/restatectl && \
    $(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-objcopy --strip-debug target/restate && \
    parca-debuginfo upload --store-address=grpc.polarsignals.com:443 --bearer-token-file=/run/secrets/parca --no-extract --type=debuginfo target/restate-server.debug || \
    echo "parca-debuginfo upload failed; debug symbols will be available as build artifact"
RUN cp docker/scripts/download-restate-debug-symbols.sh target/ && \
    sed -i"" "s/BUILD_ID/$($(just --set arch $TARGETARCH --evaluate _arch)-linux-gnu-readelf -n target/restate-server | awk '/Build/{print $3}')/g" target/download-restate-debug-symbols.sh

FROM upload-$UPLOAD_DEBUGINFO AS upload

# A minimal stage containing only debug symbols, used for artifact extraction
# without copying the entire builder filesystem.
FROM scratch AS debug-symbols
COPY --from=upload-true /restate/target/restate-server.debug /

# We do not need the Rust toolchain to run the server binary!
# NOTE: When modifying this runtime stage, also update docker/Dockerfile.refresh to keep them in sync.
FROM debian:trixie-slim AS runtime
# useful for health checks
RUN apt-get update && apt-get install --no-install-recommends -y jq curl && rm -rf /var/lib/apt/lists/*
# Create symlink for debug symbols fallback path (for read-only /usr/local/bin scenarios)
RUN mkdir -p /usr/local/bin/.debug && \
    ln -s /tmp/.debug/restate-server.debug /usr/local/bin/.debug/restate-server.debug
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
