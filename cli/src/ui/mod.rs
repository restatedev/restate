// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indicatif::ProgressBar;

use restate_cli_util::CliContext;

pub mod datetime;
pub mod deployments;
pub mod fmt;
pub mod invocations;
pub mod service_handlers;

/// A spinner with `msg` while `f` runs; nothing when there's no terminal to show it on
/// (`--json`, CI), the spinner would just be noise there.
pub async fn with_progress<T>(msg: &'static str, f: impl Future<Output = T>) -> T {
    if !CliContext::get().is_interactive() {
        return f.await;
    }
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(msg);
    let result = f.await;
    progress.finish_and_clear();
    result
}
