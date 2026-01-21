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

pub mod datetime;
pub mod deployments;
pub mod invocations;
pub mod service_handlers;

pub async fn with_progress<T>(msg: &'static str, f: impl Future<Output = T>) -> T {
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(msg);
    let result = f.await;
    progress.finish_and_clear();
    result
}
