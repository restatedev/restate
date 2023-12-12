export RUST_BACKTRACE := env_var_or_default("RUST_BACKTRACE", "short")
export DOCKER_PROGRESS := env_var_or_default('DOCKER_PROGRESS', 'auto')

dev_tools_image := "ghcr.io/restatedev/dev-tools:latest"

# Docker image name & tag.
docker_repo := "localhost/restatedev/restate"
docker_tag := if path_exists(justfile_directory() / ".git") == "true" {
        `git rev-parse --abbrev-ref HEAD | sed 's|/|.|'` + "." + `git rev-parse --short HEAD`
    } else {
        "unknown"
    }
docker_image := docker_repo + ":" + docker_tag

features := ""
libc := "gnu"
arch := "" # use the default architecture
os := "" # use the default os

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

_os := if os == "" {
        os()
    } else {
        os
    }

_os_target := if _os == "macos" {
        "apple-darwin"
    } else if _os == "linux" {
        "unknown-linux"
    } else {
        error("unsupported os=" + _os)
    }

_default_target := `rustc -vV | sed -n 's|host: ||p'`
target := _arch + "-" + _os_target + if _os == "linux" { "-" + libc } else { "" }
_resolved_target := if target != _default_target { target } else { "" }
_target-option := if _resolved_target != "" { "--target " + _resolved_target } else { "" }

_flamegraph_options := if os() == "macos" { "--root" } else { "" }

clean:
    cargo clean

fmt:
    cargo fmt --all

check-fmt:
    cargo fmt --all -- --check

clippy: (_target-installed target)
    cargo clippy {{ _target-option }} --all-targets --workspace -- -D warnings

# Runs all lints (fmt, clippy, deny)
lint: check-fmt clippy check-deny

# Extract dependencies
chef-prepare:
    cargo chef prepare --recipe-path recipe.json

# Compile dependencies
chef-cook *flags: (_target-installed target)
    cargo chef cook --recipe-path recipe.json {{ _target-option }} {{ _features }} {{ flags }}

build *flags: (_target-installed target)
    cargo build {{ _target-option }} {{ _features }} {{ flags }}

build-tools *flags: (_target-installed target)
    cd {{justfile_directory()}}/tools/xtask; cargo build {{ _target-option }} {{ _features }} {{ flags }}
    cd {{justfile_directory()}}/tools/service-protocol-wireshark-dissector; cargo build {{ _target-option }} {{ _features }} {{ flags }}

# Might be able to use cross-rs at some point but for now it could not handle a container image that
# has a rust toolchain installed. Alternatively, we can create a separate cross-rs builder image.
cross-build *flags:
    #!/usr/bin/env bash
    if [[ {{ target }} =~ "linux" ]]; then
      docker run --rm -v `pwd`:/restate:Z -w /restate {{ dev_tools_image }} just _resolved_target={{ target }} features={{ features }} build {{ flags }}
    elif [[ {{ target }} =~ "darwin" ]]; then
      if [[ {{ os() }} != "macos" ]]; then
        echo "Cannot built macos target on non-macos host";
      else
        just _resolved_target={{ target }} features={{ features }} build {{ flags }};
      fi
    else
      echo "Unsupported target: {{ target }}";
    fi

print-target:
    @echo {{ _resolved_target }}

run *flags: (_target-installed target)
    cargo run {{ _target-option }} {{ flags }}

test: (_target-installed target)
    cargo test {{ _target-option }} --all-features

# Runs lints and tests
verify: lint test

docker:
    docker buildx build . --file docker/Dockerfile --tag={{ docker_image }} --progress='{{ DOCKER_PROGRESS }}'

notice-file:
    cargo license -d -a --avoid-build-deps --avoid-dev-deps {{ _features }} | (echo "Restate Runtime\nCopyright (c) 2023 Restate Software, Inc., Restate GmbH <code@restate.dev>\n" && cat) > NOTICE

generate-config-schema:
    cargo xtask generate-config-schema > restate_config_schema.json

check-deny:
    cargo deny --all-features check

flamegraph *flags:
    cargo flamegraph {{ _flamegraph_options }} {{ flags }}

udeps *flags:
    RUSTC_BOOTSTRAP=1 cargo udeps --all-features --all-targets {{ flags }}

_target-installed target:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! rustup target list --installed |grep -qF '{{ target }}' 2>/dev/null ; then
        rustup target add '{{ target }}'
    fi

check-license-headers:
    tools/scripts/check-license-headers
