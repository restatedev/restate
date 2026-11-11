// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logical table declarations shared by query adapters and storage implementations.

pub mod bifrost_read_streams;
pub mod deployment;
pub mod index;
pub mod invocation_state;
pub mod invocation_status;
pub mod journal;
pub mod journal_events;
pub mod locks;
pub mod partition_state;
pub mod promise;
pub mod rules;
pub mod scheduler;
pub mod service;
pub mod state;
pub mod stats;
pub mod user_limits;
pub mod vqueue_entry_status;
pub mod vqueue_meta;
