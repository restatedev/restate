// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod build_info;

mod app;
mod cli_env;
mod clients;
mod commands;
mod ui;

pub use app::CliApp;
pub(crate) use restate_cli_util::ui::console;

pub static EXIT_HANDLER: std::sync::Mutex<Option<Box<dyn Fn() + Send>>> =
    std::sync::Mutex::new(None);
