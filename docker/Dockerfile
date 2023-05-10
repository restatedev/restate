FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:latest AS planner
COPY . .
RUN just chef-prepare

FROM --platform=$BUILDPLATFORM ghcr.io/restatedev/dev-tools:latest AS builder
ARG TARGETARCH
COPY --from=planner /restate/recipe.json recipe.json
COPY justfile justfile
# Caching layer if nothing has changed
# Only build restate binary to avoid compiling unneeded crates
RUN just arch=$TARGETARCH chef-cook --release --bin restate
COPY . .
RUN just arch=$TARGETARCH build --release --bin restate && \
    just notice-file && \
    mv target/$(just arch=$TARGETARCH print-target)/release/restate target/restate

# We do not need the Rust toolchain to run the binary!
FROM debian:bookworm-slim AS runtime
COPY --from=builder /restate/target/restate /usr/local/bin
COPY --from=builder /restate/NOTICE /NOTICE
# copy OS roots
COPY --from=builder /etc/ssl /etc/ssl
ENV RUST_LOG="warn,restate=info"
WORKDIR /
ENTRYPOINT ["/usr/local/bin/restate"]
