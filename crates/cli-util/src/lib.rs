// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod completions;
mod context;
pub mod lambda;
mod opts;
mod os_env;
pub mod ui;

pub use context::CliContext;
pub use opts::CommonOpts;
pub use os_env::OsEnv;

// Re-export comfy-table for console c_* macros
pub use comfy_table as _comfy_table;
pub use unicode_width as _unicode_width;
