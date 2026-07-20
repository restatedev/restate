// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The partition-processor context and its capability traits.
//!
//! The shared capability-trait pattern (`Has<Cap>` / `<Cap>Access` / `<Cap>Mut`) and the
//! storage-backed capabilities used across crates — FSM, outbox, [`Processor`] — live in
//! [`restate_worker_api::processor`] and are re-exported here. See that module for the pattern.
//!
//! This module adds:
//!
//! - [`ProcessorRawContext`] — the concrete context that implements every capability.
//! - [`ProcessorContext`] — a convenience bundle of the full capability set required by the
//!   state-machine command handlers.
//! - The processor-local capabilities that are not shared through `worker-api`: command
//!   deduplication ([`HasDedup`]/[`HasDedupMut`]) and live processor status ([`HasStatusMut`]).

pub mod commands;
mod context;
mod dedup;
mod fsm;
pub mod leadership;
mod outbox;
mod status;

// Re-exports
pub use context::ProcessorRawContext;
pub use dedup::{DedupAccess, DedupMut, HasDedup, HasDedupMut};
pub use restate_vqueues::context::{HasVQueues, HasVQueuesMut};
pub use restate_worker_api::processor::*;
pub use status::HasStatusMut;

use self::fsm::Fsm;

/// The full set of processor-context capabilities required by the state-machine
/// command handlers (everything reachable through `StateMachineApplyContext`).
///
/// Blanket-implemented for any type that provides the underlying capabilities, so
/// `ProcessorRawContext` (and test doubles) qualify automatically. Handlers that
/// only need a narrower capability should bound on that capability directly (e.g.
/// [`HasOutboxMut`]) rather than on this bundle.
pub trait ProcessorContext: Processor + HasFsmMut + HasOutboxMut + HasVQueuesMut {}

impl<T: Processor + HasFsmMut + HasOutboxMut + HasVQueuesMut> ProcessorContext for T {}
