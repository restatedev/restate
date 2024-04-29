// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod analyzer;
pub mod context;
mod deployment;
mod idempotency;
mod inbox;
mod invocation_state;
mod invocation_status;
mod journal;
mod keyed_service_status;
mod partition_store_scanner;
mod physical_optimizer;
mod service;
mod state;
mod table_macro;
mod table_providers;
mod table_util;

pub use context::BuildError;

#[cfg(test)]
pub(crate) mod mocks;

#[cfg(test)]
mod tests;
