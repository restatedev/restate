// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Build information
#![allow(dead_code)]

use std::sync::OnceLock;

/// The version of restate CLI.
pub const RESTATECTL_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const RESTATECTL_VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
pub const RESTATECTL_VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
pub const RESTATE_CLI_VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");
/// Pre-release version of restate.
pub const RESTATECTL_VERSION_PRE: &str = env!("CARGO_PKG_VERSION_PRE");

pub const RESTATECTL_BUILD_DATE: &str = env!("VERGEN_BUILD_DATE");
pub const RESTATECTL_BUILD_TIME: &str = env!("VERGEN_BUILD_TIMESTAMP");
pub const RESTATECTL_COMMIT_SHA: &str = env!("VERGEN_GIT_SHA");
pub const RESTATECTL_COMMIT_DATE: &str = env!("VERGEN_GIT_COMMIT_DATE");
pub const RESTATECTL_BRANCH: &str = env!("VERGEN_GIT_BRANCH");
/// The target triple.
pub const RESTATECTL_TARGET_TRIPLE: &str = env!("VERGEN_CARGO_TARGET_TRIPLE");
/// The build features
pub const RESTATECTL_BUILD_FEATURES: &str = env!("VERGEN_CARGO_FEATURES");

/// Returns build information, e.g: 0.0.1-dev (debug) (2ba1491 aarch64-apple-darwin 2023-11-21)
pub(crate) fn build_info() -> String {
    format!(
        "{RESTATECTL_VERSION}{} ({RESTATECTL_COMMIT_SHA} {RESTATECTL_TARGET_TRIPLE} {RESTATECTL_BUILD_DATE})",
        if is_debug() { " (debug)" } else { "" }
    )
}

static VERSION: OnceLock<String> = OnceLock::new();

pub fn version() -> &'static str {
    VERSION.get_or_init(build_info)
}

const RESTATECTL_DEBUG_STRIPPED: Option<&str> = option_env!("DEBUG_STRIPPED");
const RESTATECTL_DEBUG: &str = env!("VERGEN_CARGO_DEBUG");
/// Was the binary compiled with debug symbols
pub fn is_debug() -> bool {
    RESTATECTL_DEBUG == "true" && RESTATECTL_DEBUG_STRIPPED != Some("true")
}
