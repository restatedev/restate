// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::large_futures)]

#[macro_use]
mod help;

mod build_info;

mod app;
mod cli_env;
mod clients;
mod commands;
mod error;
mod error_report;
mod ui;
mod util;

pub use app::{CliApp, Command, command};
pub use commands::kafka_integration_notice;
pub use error_report::report_error;
pub(crate) use restate_cli_util::ui::console;

pub static EXIT_HANDLER: std::sync::Mutex<Option<Box<dyn Fn() + Send>>> =
    std::sync::Mutex::new(None);
