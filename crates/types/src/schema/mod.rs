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
//! This module contains the APIs and the [`Schema`] data structure that represents all the metadata discovered during service registration,
//! and all the changes executed in the Admin API.
//!
//! Check the submodules [`deployment`], [`invocation_target`], [`service`] and [`subscriptions`] for the various schema registry access APIs.
//!
//! Check [`Schema`] and the [`updater`] package to store the schema registry, and the APIs to update it.

pub mod deployment;
pub mod invocation_target;
mod metadata;
pub mod service;
pub mod subscriptions;

pub use metadata::Schema;
pub use metadata::updater;
