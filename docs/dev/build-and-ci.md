# Build Infrastructure and CI/CD

This document describes Restate's build infrastructure, CI pipelines, and release automation. For the release process itself (versioning policy, pre-release checklist, etc.), see [release.md](./release.md).

## Contents

- [Overview](#overview)
- [Rust Build Configuration](#rust-build-configuration)
- [cargo-hakari (workspace-hack)](#cargo-hakari-workspace-hack)
- [dev-tools Image](#dev-tools-image)
- [CI Workflows](#ci-workflows)
  - [Main CI Pipeline](#main-ci-pipeline-ciyml)
  - [Docker Image Artifact Flow](#docker-image-artifact-flow)
  - [Docker Build Workflow](#docker-build-workflow-dockeryml)
  - [Release Workflow](#release-workflow-releaseyml)
  - [Release Build Setup](#release-build-setup-stepsrelease-build-setupyml)
- [macOS Code Signing and Notarization](#macos-code-signing-and-notarization)
- [npm Distribution](#npm-distribution)
- [Homebrew Distribution](#homebrew-distribution)
- [Helm Chart](#helm-chart)
- [WarpBuild Runners](#warpbuild-runners)
- [Analytics and Observability](#analytics-and-observability)
- [Secrets Required](#secrets-required)

## Overview

Restate's build system produces:
- **Docker images**: Multi-arch (linux/amd64, linux/arm64) images for `restate-server` and `restate-cli`
- **Static binaries**: MUSL-linked Linux binaries and macOS binaries for both x86_64 and aarch64
- **Helm chart**: Kubernetes deployment chart published to GHCR

Distribution channels:
- **GitHub Releases**: Binary archives with checksums
- **Docker registries**: GHCR (`ghcr.io/restatedev/restate`, `ghcr.io/restatedev/restate-cli`) and Docker Hub (`docker.io/restatedev/restate`)
- **Homebrew**: Via the `restatedev/homebrew-tap` repository
- **npm**: `@restatedev/restate`, `@restatedev/restate-server`, `@restatedev/restatectl`
- **Helm OCI registry**: `ghcr.io/restatedev/restate-helm`

## Rust Build Configuration

The Rust build is configured through several files. When making changes, be aware of which files need to stay in sync.

| File | Purpose | Sync Requirements |
|------|---------|-------------------|
| `Cargo.toml` | Workspace members, shared package metadata (version, edition, rust-version), centralized dependencies, release profile settings | `workspace.package.version` updated for releases; `rust-version` must match `rust-toolchain.toml` |
| `rust-toolchain.toml` | Pins Rust version and components (rustfmt, clippy) for local development | Must match dev-tools image Rust version |
| `.cargo/config.toml` | Cargo behavior: `RUST_TEST_THREADS=1`, `xtask` alias, per-target RUSTFLAGS (unwind tables, `uuid_unstable`, `tokio_unstable`, frame pointers, static linking) | RUSTFLAGS must be duplicated in `release-build-setup.yml` ([cargo-dist#1571](https://github.com/axodotdev/cargo-dist/issues/1571)) |
| `.config/hakari.toml` | cargo-hakari configuration for workspace-hack generation | See [cargo-hakari section](#cargo-hakari-workspace-hack) |
| `.config/nextest.toml` | cargo-nextest test runner configuration (timeouts) | — |
| `deny.toml` | cargo-deny: license allowlist, advisory checks, dependency bans, source restrictions | Update when adding deps with new licenses or Git sources |
| `clippy.toml` | Clippy lint configuration (interior mutability exceptions, module inception) | — |

**When updating Rust version:**
1. Update `rust-toolchain.toml`
2. Update `workspace.package.rust-version` in `Cargo.toml`
3. Update the Rust version in the dev-tools Dockerfile
4. Release a new dev-tools image and update pins in `docker/Dockerfile` and `dist-workspace.toml`

## cargo-hakari (workspace-hack)

Restate uses [cargo-hakari](https://docs.rs/cargo-hakari/latest/cargo_hakari/) to optimize build times in the workspace.

### The Problem

In a Cargo workspace, different crates may depend on the same library with different features enabled. When you build crate A, Cargo compiles the dependency with A's features. When you then build crate B (which needs different features), Cargo must recompile the dependency with the union of features. This "feature unification" causes repeated recompilation during development.

### The Solution

cargo-hakari generates a `workspace-hack` crate that depends on all workspace dependencies with all their features unified. Every crate in the workspace depends on `workspace-hack`, so the first build compiles dependencies with all features, and subsequent builds reuse those artifacts.

### Maintenance

The `workspace-hack/Cargo.toml` is auto-generated. When dependencies change:

```bash
# Regenerate workspace-hack
cargo hakari generate

# Verify all crates depend on workspace-hack
cargo hakari manage-deps
```

CI runs `cargo hakari generate --diff` to verify the workspace-hack is up to date.

**Important:** Before running cargo commands in Docker or release builds, hakari is disabled (`cargo hakari disable`) because the unified features can cause issues with cross-compilation and feature-specific code paths.

## dev-tools Image

The `ghcr.io/restatedev/dev-tools` image is a build environment used for:
1. **Docker builds**: The Dockerfile uses dev-tools as a build stage
2. **Release binary builds**: cargo-dist uses dev-tools containers for Linux MUSL builds
3. **Local development**: Can be used for cross-compilation via `just cross-build`

### What's in dev-tools

The image (defined in [github.com/restatedev/dev-tools](https://github.com/restatedev/dev-tools)) contains:

- **Rust toolchain** with targets: `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`, `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`
- **Cross-compilation toolchains**: GCC for x86_64 and aarch64, both GNU and MUSL
- **Build tools**: `just`, `cargo-chef`, `cargo-license`, `cargo-hakari`, `protobuf-compiler`, `sccache`, `buf`, `parca-debuginfo`
- **Environment variables**: Pre-configured for cross-compilation (CC, CXX, AR, linker settings, bindgen flags)

### Versioning

The dev-tools image is versioned and pinned in consuming workflows. When updating:
1. Create a tag in the dev-tools repo (e.g., `v1.15.0`)
2. The release workflow builds and pushes the image
3. Update the pinned version in `docker/Dockerfile`, `debug.Dockerfile` and `dist-workspace.toml`

## CI Workflows

### Main CI Pipeline (`ci.yml`)

Triggered on: PRs, pushes to `main` and `release-*` branches, and as part of releases.

**Jobs:**

| Job | Runner | Purpose |
|-----|--------|---------|
| `workspace-hack-check` | warp-ubuntu-latest-x64-4x | Verifies `cargo-hakari` workspace-hack is up to date |
| `build-and-test` | warp-ubuntu-latest-x64-16x | Runs `just verify` (lint + test + doctest) |
| `docker` | warp-ubuntu-latest-x64-16x | Builds Docker image, uploads as tarball artifact |

**Integration Tests** (run after docker build):

| Job | Description |
|-----|-------------|
| `sdk-java` | SDK-Java integration tests against built image |
| `sdk-python` | SDK-Python integration tests |
| `sdk-go` | SDK-Go integration tests |
| `sdk-typescript` | SDK-TypeScript integration tests |
| `sdk-rust` | SDK-Rust integration tests |
| `e2e` | End-to-end tests from `restatedev/e2e` |
| `jepsen` | Distributed systems tests (main branch only, non-fork PRs) |

Experimental feature tests (`e2e-vqueues`, `e2e-experimental-kafka-ingestion`, etc.) run with `continue-on-error: true` and don't block merges.

### Docker Image Artifact Flow

Integration tests need a Docker image of the Restate server built from the PR/commit being tested. This is handled through GitHub Actions artifacts:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ ci.yml (restate repo)                                                   │
│                                                                         │
│  docker job ──builds──► restate.tar ──uploads──► GitHub Artifact        │
│       │                                              │                  │
│       └── needs ──────────────────────────────────────                  │
│                                                      │                  │
│  sdk-java job ◄── calls ── restatedev/sdk-java/.github/workflows/       │
│       │                    integration.yaml                             │
│       │                           │                                     │
│       │                           ▼                                     │
│       │                    downloads artifact (restate.tar)             │
│       │                    docker load                                  │
│       │                    runs tests against loaded image              │
│       │                                                                 │
│  e2e job                                                                │
│       │                                                                 │
│       ▼                                                                 │
│  downloads artifact directly (same workflow)                            │
│  docker load --input restate.tar                                        │
│  docker tag ... localhost/restatedev/restate-commit-download:latest     │
│  calls restatedev/e2e action                                            │
└─────────────────────────────────────────────────────────────────────────┘
```

**How it works:**

1. The `docker` job builds the image and saves it as an OCI tarball artifact named `restate.tar`
2. **For SDK tests**: The workflow calls the SDK repo's `integration.yaml` with `restateCommit: ${{ github.sha }}`. The SDK workflow uses `actions/download-artifact` to fetch the tarball from the *calling* workflow
3. **For e2e tests**: The e2e job downloads the artifact directly, loads it, and re-tags it to a known name

**Fragility points:**

- **Cross-workflow artifact access**: SDK integration workflows download artifacts from the restate workflow using `actions/download-artifact`. This relies on GitHub's artifact sharing between `workflow_call` caller and callee, which can be brittle across GitHub Actions updates
- **Artifact naming**: The artifact must be named exactly `restate.tar` - both the upload in `docker.yml` and downloads in SDK workflows must agree
- **Artifact retention**: Artifacts are retained for 1 day (`retention-days: 1`). Long-running or queued jobs may fail if artifacts expire
- **Image tagging**: e2e tests re-tag the image to `localhost/restatedev/restate-commit-download:latest`. If this tag format changes, e2e tests break
- **Docker containerd snapshotter**: OCI-format tarballs require the containerd snapshotter feature enabled in Docker. Jobs must configure this explicitly

**Jepsen special case:**

Jepsen tests don't use the artifact mechanism. Instead, they:
1. Wait for the docker job to complete and push to GHCR (on main branch, images are pushed to the registry)
2. Pull the image by commit SHA tag from GHCR

This avoids artifact transfer but only works for commits that push images to the registry (main branch, release tags).

### Docker Build Workflow (`docker.yml`)

A **reusable workflow** called by this repo and many others in the restatedev org. Any repo with a `docker/Dockerfile` can use it.

**Inputs:**
- `uploadImageAsTarball`: Save image as artifact for downstream jobs
- `platforms`: Target platforms (default: `linux/arm64,linux/amd64`)
- `debug`: Include debug symbols
- `parca`: Split debug symbols and upload to Polar Signals
- `buildIndividually`: Build binaries separately to avoid feature unification
- `features`: Cargo features to enable
- `pushToDockerHub`: Push to Docker Hub in addition to GHCR

**Build optimizations:**
- Uses `sccache` with GHA cache
- Multi-platform builds via Docker Buildx
- Caches Docker layers to GHA cache

### Release Workflow (`release.yml`)

Triggered by pushing a version tag (e.g., `v1.0.0`). Orchestrated by [cargo-dist](https://opensource.axo.dev/cargo-dist/), which is configured in `dist-workspace.toml`.
`release.yml` is generated with `dist generate`.

**Phases:**

1. **Plan**: Determines what to build based on `dist-workspace.toml`
2. **Build local artifacts**: Platform-specific binaries
   - macOS builds on `warp-macos-latest-arm64-6x` (x86_64 cross-compiled from ARM)
   - Linux MUSL builds in dev-tools containers on `warp-ubuntu-latest-*-32x`
3. **Custom CI**: Runs full test suite
4. **Docker build**: Builds release Docker images with debug symbols for Parca
5. **Build global artifacts**: Checksums, Homebrew formulae
6. **Notarize**: macOS binary notarization
7. **Host**: Creates GitHub Release, uploads artifacts
8. **Publish**: Homebrew, Docker push, npm, release notes
9. **Post-announce**: Helm chart

### Release Build Setup (`steps/release-build-setup.yml`)

The binary build jobs in the release workflow are configured via `.github/workflows/steps/release-build-setup.yml`, referenced in `dist-workspace.toml` as `github-build-setup`. This file runs before cargo-dist builds binaries and handles setup that cargo-dist doesn't do automatically.

**What it does:**

- **Installs tools** (macOS only, since Linux uses the dev-tools container): `cargo-license`, `just`
- **Disables hakari**: `cargo hakari disable` to avoid feature unification issues
- **Generates NOTICE file**: `just notice-file` creates the third-party license attribution
- **Sets environment variables**:
  - `MACOSX_DEPLOYMENT_TARGET=10.14.0` for macOS compatibility
  - `CODESIGN_OPTIONS=runtime` for hardened runtime (required for notarization)
  - `RUSTFLAGS` with `-C force-unwind-tables --cfg uuid_unstable --cfg tokio_unstable`
  - Cross-compilation variables for krb5 and sasl2 (`krb5_cv_attr_constructor_destructor`, etc.)

**Why this duplication exists:**

Some of this setup duplicates what's in `docker/Dockerfile` or `.cargo/config.toml`:

- The RUSTFLAGS are defined in `.cargo/config.toml`, but cargo-dist doesn't read that file ([cargo-dist#1571](https://github.com/axodotdev/cargo-dist/issues/1571))
- The cross-compilation env vars are set in both the Dockerfile and here because Linux MUSL builds run in the dev-tools container, but macOS builds run directly on runners
- Tool installation happens in dev-tools for Linux, but must be done explicitly for macOS

When adding new environment variables or build requirements, you may need to update both this file and the Dockerfile to keep them in sync.

## macOS Code Signing and Notarization

macOS binaries are signed and notarized for Gatekeeper compliance:

1. **Signing** (during build): Uses `CODESIGN_CERTIFICATE`, `CODESIGN_CERTIFICATE_PASSWORD`, `CODESIGN_IDENTITY` secrets
2. **Hardened runtime**: `CODESIGN_OPTIONS="runtime"` enables hardened runtime which is required for notarization
3. **Notarization** (`notarize.yml`): Submits signed binaries to Apple's notary service using `xcrun notarytool`

Required secrets:
- `CODESIGN_CERTIFICATE`: Base64-encoded .p12 certificate
- `CODESIGN_CERTIFICATE_PASSWORD`: Certificate password
- `CODESIGN_IDENTITY`: Team ID for signing
- `NOTARY_APPLE_ID`: Apple ID for notarization
- `NOTARY_APP_SPECIFIC_PASSWORD`: App-specific password for notarization

## npm Distribution

The npm packages use a platform-specific binary architecture:

```
@restatedev/restate           # Base package (TypeScript wrapper)
├── @restatedev/restate-linux-x64    # Optional dependencies
├── @restatedev/restate-linux-arm64
├── @restatedev/restate-darwin-x64
└── @restatedev/restate-darwin-arm64
```

The base package (`npm/restate/`) contains a TypeScript wrapper that:
1. Detects the current platform (`os.platform()`, `os.arch()`)
2. Resolves the appropriate binary from optional dependencies
3. Spawns the binary with passed arguments

**Why this approach?** Unlike cargo-dist's default npm support (which downloads binaries in a postinstall script), this approach:
- Hosts binaries directly on npm
- Works in environments that block postinstall network requests
- Supports `--tag next` for prereleases

The platform-specific packages are generated during release using `npm/package.json.tmpl`.

## Homebrew Distribution

Homebrew formulae are generated by cargo-dist and published to `restatedev/homebrew-tap`.

**Custom handling (`homebrew.yml`):**
1. Download URLs are rewritten to use Scarf gateway for anonymized download analytics
2. Formulae are styled with `brew style --fix`
3. Prereleases are committed but **not pushed** (Homebrew doesn't support prerelease versions)

## Helm Chart

The Helm chart (`charts/restate-helm/`) is published to GHCR as an OCI artifact.

**Version sync:** The chart version in `Chart.yaml` must match the release version. The `helm.yml` workflow validates this before publishing.

## WarpBuild Runners

All CI jobs use [WarpBuild](https://warpbuild.com/) runners instead of GitHub-hosted runners:

**Benefits:**
- Larger machines at lower cost (up to 32x runners for release builds)
- Faster checkout and artifact operations

**Runner naming:**
- `warp-ubuntu-latest-x64-{2,4,16,32}x`: Linux x64 with N cores
- `warp-ubuntu-latest-arm64-{2,4,32}x`: Linux ARM64
- `warp-macos-latest-arm64-6x`: macOS ARM64

## Analytics and Observability

### Scarf Gateway

Download URLs in Homebrew formulae and GitHub release notes use Scarf gateway (`restate.gateway.scarf.sh`) to collect anonymous download statistics without affecting functionality.

The npm packages use `@scarf/scarf` as a dependency for similar analytics.

### Polar Signals / Parca

Release Docker images have debug symbols uploaded to [Polar Signals](https://www.polarsignals.com/):

1. **During build**: `parca-debuginfo upload` sends symbols to `grpc.polarsignals.com`
2. **Symbols are stripped**: The binary in the image has symbols removed, with GNU debuglink preserved
3. **Download script**: `/usr/local/bin/download-restate-debug-symbols.sh` in the image can fetch symbols by build ID

**Benefits:**
- Smaller Docker images (stripped binaries)
- Rich profiling with Parca (symbols resolved automatically via debuginfod)
- Manual symbol download for debugging with other tools

## Secrets Required

| Secret | Purpose |
|--------|---------|
| `CODESIGN_CERTIFICATE` | macOS code signing certificate (base64 .p12) |
| `CODESIGN_CERTIFICATE_PASSWORD` | Certificate password |
| `CODESIGN_IDENTITY` | Apple Team ID |
| `NOTARY_APPLE_ID` | Apple ID for notarization |
| `NOTARY_APP_SPECIFIC_PASSWORD` | App-specific password |
| `NPM_TOKEN` | npm publish token |
| `HOMEBREW_TAP_TOKEN` | PAT for pushing to homebrew-tap |
| `PARCA_TOKEN` | Polar Signals API token |
| `DOCKER_USERNAME` | Docker Hub username |
| `DOCKER_PASSWORD` | Docker Hub password/token |
