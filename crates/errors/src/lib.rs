// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains all the relevant error codes used by the Restate Runtime.
//!
//! To add a new error code:
//! * Add the code name to the macro invocation below.
//! * Add a new markdown file under `error_codes/` with the same name of the error code. This file should contain the description of the error.
//! * Now you can use the code declaration in conjunction with the [`codederror::CodedError`] macro as follows:
//!
//! ```rust,ignore
//! use codederror::CodedError;
//! use thiserror::Error;
//!
//! #[derive(Error, CodedError, Debug)]
//! #[code(RT0001)]
//! #[error("my error")]
//! pub struct MyError;
//! ```
//!

#[macro_use]
pub mod fmt;
#[macro_use]
mod helper;

// RT are runtime related errors and can be used for both execution errors, or runtime configuration errors.
// META are meta related errors.

declare_restate_error_codes!(
    RT0001, RT0002, RT0003, RT0004, RT0005, RT0006, RT0007, RT0009, RT0010, RT0011, RT0012, RT0013,
    RT0014, RT0015, RT0016, RT0017, RT0018, RT0019, RT0020, RT0021, RT0022, META0003, META0004,
    META0005, META0006, META0009, META0010, META0011, META0012, META0013, META0014, META0015,
    META0016, META0017
);

// -- Some commonly used errors

#[derive(Debug, Clone, Copy)]
pub struct NotRunningError;

impl std::fmt::Display for NotRunningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "not running")
    }
}

impl std::error::Error for NotRunningError {}
