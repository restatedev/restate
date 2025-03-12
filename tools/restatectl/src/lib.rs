// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod app;
pub(crate) mod commands;
pub(crate) mod util;
use std::sync::LazyLock;

pub use app::CliApp;
use semver::Version;
mod build_info;
pub(crate) mod connection;
pub(crate) mod environment;

pub static VERSION: LazyLock<Version> = LazyLock::new(|| {
    Version::parse(env!("CARGO_PKG_VERSION")).expect("CARGO_PKG_VERSION is a valid semver")
});
