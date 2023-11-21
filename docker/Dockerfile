# Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
# All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:latest AS planner
COPY . .
RUN just chef-prepare

FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:latest AS builder
ARG TARGETARCH
# Overrides the behaviour of the release profile re including debug symbols, which in our repo is not to include them.
# Should be set to 'false' or 'true'. See https://doc.rust-lang.org/cargo/reference/environment-variables.html
ARG CARGO_PROFILE_RELEASE_DEBUG=false
COPY --from=planner /restate/recipe.json recipe.json
COPY justfile justfile
# Caching layer if nothing has changed
# Only build restate binary to avoid compiling unneeded crates
RUN CARGO_PROFILE_RELEASE_DEBUG=$CARGO_PROFILE_RELEASE_DEBUG just arch=$TARGETARCH chef-cook --release --bin restate-server
COPY . .
RUN CARGO_PROFILE_RELEASE_DEBUG=$CARGO_PROFILE_RELEASE_DEBUG just arch=$TARGETARCH build --release --bin restate-server && \
    just notice-file && \
    mv target/$(just arch=$TARGETARCH print-target)/release/restate-server target/restate-server

# We do not need the Rust toolchain to run the server binary!
FROM debian:bookworm-slim AS runtime
COPY --from=builder /restate/target/restate-server /usr/local/bin
COPY --from=builder /restate/NOTICE /NOTICE
COPY --from=builder /restate/LICENSE /LICENSE
# copy OS roots
COPY --from=builder /etc/ssl /etc/ssl
ENV RUST_LOG="warn,restate=info"
WORKDIR /
ENTRYPOINT ["/usr/local/bin/restate-server"]
