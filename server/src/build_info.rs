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

use std::str::FromStr;

use restate_types::SemanticRestateVersion;

/// The version of restate server.
pub const RESTATE_SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const RESTATE_SERVER_VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
pub const RESTATE_SERVER_VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
pub const RESTATE_SERVER_VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");
/// Pre-release version of restate.
pub const RESTATE_SERVER_VERSION_PRE: &str = env!("CARGO_PKG_VERSION_PRE");

pub const RESTATE_SERVER_BUILD_DATE: &str = env!("VERGEN_BUILD_DATE");
pub const RESTATE_SERVER_BUILD_TIME: &str = env!("VERGEN_BUILD_TIMESTAMP");
pub const RESTATE_SERVER_COMMIT_SHA: &str = env!("VERGEN_GIT_SHA");
pub const RESTATE_SERVER_COMMIT_DATE: &str = env!("VERGEN_GIT_COMMIT_DATE");
pub const RESTATE_SERVER_BRANCH: &str = env!("VERGEN_GIT_BRANCH");
// The target triple.
pub const RESTATE_SERVER_TARGET_TRIPLE: &str = env!("VERGEN_CARGO_TARGET_TRIPLE");

/// Returns build information, e.g: 0.5.0-dev (debug) (2ba1491 aarch64-apple-darwin 2023-11-21)
pub fn build_info() -> String {
    format!(
        "{RESTATE_SERVER_VERSION}{} ({RESTATE_SERVER_COMMIT_SHA} {RESTATE_SERVER_TARGET_TRIPLE} {RESTATE_SERVER_BUILD_DATE})",
        if is_debug() { " (debug)" } else { "" }
    )
}

const RESTATE_SERVER_DEBUG_STRIPPED: Option<&str> = option_env!("DEBUG_STRIPPED");
const RESTATE_SERVER_DEBUG: &str = env!("VERGEN_CARGO_DEBUG");
/// Was the binary compiled with debug symbols
pub fn is_debug() -> bool {
    RESTATE_SERVER_DEBUG == "true" && RESTATE_SERVER_DEBUG_STRIPPED != Some("true")
}

pub async fn check_if_latest_version() {
    let octocrab = octocrab::instance();
    let Ok(mut latest) = octocrab
        .repos("restatedev", "restate")
        .releases()
        .get_latest()
        .await
    else {
        return;
    };

    let current_version = SemanticRestateVersion::current().clone();
    if latest.tag_name.starts_with("v") {
        // ignore if the tag is not a version
        let version_str = latest.tag_name.split_off(1);
        let Ok(latest_release) = SemanticRestateVersion::from_str(&version_str) else {
            return;
        };

        if matches!(
            latest_release.cmp_precedence(&current_version),
            std::cmp::Ordering::Greater
        ) {
            tracing::info!(
                %current_version,
                "📣⬆️New Version Notice: v{} was released at {}, check it out at {}",
                latest_release.to_string(),
                latest
                    .published_at
                    .map(|d| d.to_string())
                    .unwrap_or("<unknown>".to_owned()),
                latest.html_url.to_string()
            );
        }
    }
}
