FROM --platform=$BUILDPLATFORM rust:1.67.1-bookworm AS tools
# prepopulate cargo index
RUN cargo search --limit=0
RUN apt update && apt upgrade -y
RUN apt -y install \
    g++-x86-64-linux-gnu \
    g++-aarch64-linux-gnu \
    protobuf-compiler \
    libprotoc-dev \
    clang \
    llvm \
    ca-certificates
RUN curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin
COPY rust-toolchain.toml .
# Install toolchain based on rust-toolchain.toml. See for more details: https://github.com/paketo-community/rustup/issues/56
RUN rustup show
RUN rustup target add \
    x86_64-unknown-linux-gnu \
    aarch64-unknown-linux-gnu
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
ENV BINDGEN_EXTRA_CLANG_ARGS_x86_64_unknown_linux_gnu='--sysroot=/usr/x86_64-linux-gnu'
ENV BINDGEN_EXTRA_CLANG_ARGS_aarch64_unknown_linux_gnu='--sysroot=/usr/aarch64-linux-gnu'
RUN cargo install cargo-chef@0.1.59 cargo-license@0.5.1
WORKDIR /restate

FROM --platform=$BUILDPLATFORM tools AS planner
COPY . .
RUN just chef-prepare

FROM --platform=$BUILDPLATFORM tools as builder
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
