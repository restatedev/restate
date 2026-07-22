// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The capabilities a partition processor exposes to command handlers.
//!
//! The processor context is split into small **capability traits** so a handler can depend on
//! exactly what it uses instead of on a monolithic type. Every capability follows the same shape:
//!
//! - `Has<Cap>` — the context grants read access to the capability, handing out a `<Cap>Access`
//!   view.
//! - `Has<Cap>Mut` — extends `Has<Cap>` with mutating access, handing out a `<Cap>Mut` view.
//!   Mutations are staged into a storage transaction and only take effect when it commits.
//!
//! For example, [`HasFsm`]/[`HasFsmMut`] expose [`FsmAccess`]/[`FsmMut`] views over the partition
//! FSM cache, and [`HasOutbox`]/[`HasOutboxMut`] do the same for the outbox.
//!
//! A handler bounds on the narrowest set it needs (e.g. `P: HasOutboxMut`). The concrete context
//! implements every capability, and the blanket `&T`/`&mut T` impls forward each capability so a
//! borrowed context satisfies the same bounds — letting handlers take `&mut ctx`.
//!
//! [`Processor`] is the base trait every context implements.

use restate_types::identifiers::LeaderEpoch;
use restate_types::logs::LogId;
use restate_types::sharding::{KeyRange, PartitionId};

mod features;
mod fsm;
mod outbox;

pub use features::PartitionFeatures;
pub use fsm::{FsmAccess, FsmMut, HasFsm, HasFsmMut};
pub use outbox::{HasOutbox, HasOutboxMut, OutboxAccess, OutboxMut};

/// Identity and metadata of a running partition processor.
///
/// The base capability every context provides — partition identity plus the always-present FSM
/// capability ([`HasFsm`]). Combine with the other `Has*` traits for the remaining storage-backed
/// state (outbox, dedup, ...).
pub trait Processor: HasFsm {
    /// The log this partition consumes its commands from.
    fn log_id(&self) -> LogId;
    /// The partition this processor is responsible for.
    fn partition_id(&self) -> PartitionId;
    /// The leader epoch this processor is currently running under.
    fn current_leader_epoch(&self) -> LeaderEpoch;
    /// The partition-key range owned by this partition.
    fn key_range(&self) -> KeyRange;
}

impl<P: Processor> Processor for &mut P {
    #[inline]
    fn log_id(&self) -> LogId {
        (**self).log_id()
    }

    #[inline]
    fn partition_id(&self) -> PartitionId {
        (**self).partition_id()
    }

    #[inline]
    fn current_leader_epoch(&self) -> LeaderEpoch {
        (**self).current_leader_epoch()
    }

    #[inline]
    fn key_range(&self) -> KeyRange {
        (**self).key_range()
    }
}
