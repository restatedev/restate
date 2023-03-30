export RUST_BACKTRACE := env_var_or_default("RUST_BACKTRACE", "short")
export DOCKER_PROGRESS := env_var_or_default('DOCKER_PROGRESS', 'auto')

# Docker image name & tag.
docker_repo := "localhost/restatedev/restate"
docker_tag := `git rev-parse --abbrev-ref HEAD | sed 's|/|.|'` + "." + `git rev-parse --short HEAD`
docker_image := docker_repo + ":" + docker_tag

features := ""
libc := "gnu"
arch := "" # use the default architecture

_features := if features == "all" {
        "--all-features"
    } else if features != "" {
        "--features=" + features
    } else { "" }

_arch := if arch == "" {
        arch()
    } else if arch == "amd64" {
        "x86_64"
    } else if arch == "x86_64" {
        "x86_64"
    } else if arch == "arm64" {
        "aarch64"
    } else if  arch == "aarch64" {
        "aarch64"
    } else {
        error("unsupported arch=" + arch)
    }

_os := os()
_os_target := if _os == "macos" {
        "apple-darwin"
    } else if _os == "linux" {
        "unknown-linux"
    } else {
        error("unsupported os=" + _os)
    }

_target := _arch + "-" + _os_target + if _os == "linux" { "-" + libc } else { "" }
_target-option := if arch != "" { "--target " + _target } else { "" }

clean:
    cargo clean

fmt:
    cargo +nightly fmt --all

check-fmt:
    cargo +nightly fmt --all -- --check

clippy: (_target-installed _target)
    cargo clippy {{ _target-option }} --all-targets -- -D warnings

# Runs all lints (fmt, clippy, deny)
lint: check-fmt clippy check-deny

build *flags: (_target-installed _target)
    cargo build {{ _target-option }} {{ _features }} {{ flags }}

run *flags: (_target-installed _target)
    cargo run {{ _target-option }} {{ flags }}

test: (_target-installed _target)
    cargo test {{ _target-option }} --workspace --all-features

# Runs lints and tests
verify: lint test

docker:
    docker buildx build . --file docker/Dockerfile --tag={{ docker_image }} --progress='{{ DOCKER_PROGRESS }}'

notice-file:
    cargo license -d -a --avoid-build-deps --avoid-dev-deps {{ _features }} | (echo "Restate Runtime\nCopyright (c) 2023 Restate GmbH <stephan@restate.dev>\n" && cat) > NOTICE

check-deny:
    cargo deny check

_target-installed target:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! rustup target list --installed |grep -qF '{{ target }}' 2>/dev/null ; then
        rustup target add '{{ target }}'
    fi