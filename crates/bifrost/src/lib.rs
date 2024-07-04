// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod bifrost;
mod error;
mod loglet;
#[cfg(test)]
mod loglet_tests;
pub mod loglets;
mod provider;
mod read_stream;
mod record;
mod service;
mod types;
mod watchdog;

pub use bifrost::Bifrost;
pub use error::{Error, ProviderError, Result};
pub use provider::*;
pub use read_stream::LogReadStream;
pub use record::*;
pub use service::BifrostService;
pub use types::*;

pub const SMALL_BATCH_THRESHOLD_COUNT: usize = 4;
