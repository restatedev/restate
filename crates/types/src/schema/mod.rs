// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! ## Schema
//!
//! This module contains the implementation of the schema registry.
//!
//! The schema registry takes care of:
//!
//! * Storing deployments, handle registration
//! * Storing service/handler configurations
//!
//! Check [`registry::SchemaRegistry`] for the schema registry implementation, implementing both read and write operations.
//!
//! Check the submodules [`deployment`], [`invocation_target`], [`service`] and [`subscriptions`] for the various read APIs.
//!
//! The [`Schema`] data structure is a serializable representation of this schema registry.

pub mod deployment;
pub mod info;
pub mod invocation_target;
pub mod kafka;
mod metadata;
pub mod registry;
pub mod service;
pub mod subscriptions;

pub use metadata::Schema;

#[derive(Clone, Copy)]
pub enum Redaction {
    Yes,
    No,
}
