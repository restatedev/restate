// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::mpsc;
use tracing::trace;

use restate_platform::hash::HashMap;
use restate_types::identifiers::InvocationId;
use restate_types::sharding::KeyRange;
use restate_worker_api::invoker::{FencedEffect, invocation_reader::InvocationReader};

use crate::InvocationStateMachine;

/// Tree of [InvocationStateMachine] held by the [Service].
#[derive(Debug)]
pub(super) struct InvocationStateMachineManager<SR> {
    output_tx: mpsc::Sender<FencedEffect>,
    invocation_state_machines: HashMap<InvocationId, InvocationStateMachine>,
    _partition_key_range: KeyRange,
    storage_reader: SR,
}

impl<SR> InvocationStateMachineManager<SR>
where
    SR: InvocationReader + Clone + Send + Sync + 'static,
{
    #[inline]
    pub(super) fn new(
        partition_key_range: KeyRange,
        storage_reader: SR,
        sender: mpsc::Sender<FencedEffect>,
    ) -> Self {
        Self {
            output_tx: sender,
            invocation_state_machines: Default::default(),
            _partition_key_range: partition_key_range,
            storage_reader,
        }
    }

    #[inline]
    pub(super) fn storage_reader(&self) -> &SR {
        &self.storage_reader
    }

    #[inline]
    pub(super) fn partition_sender(&self) -> &mpsc::Sender<FencedEffect> {
        &self.output_tx
    }

    #[inline]
    pub(super) fn resolve_invocation(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Option<(&mpsc::Sender<FencedEffect>, &mut InvocationStateMachine)> {
        self.invocation_state_machines
            .get_mut(invocation_id)
            .map(|ism| (&self.output_tx, ism))
    }

    /// Returns `true` if the in-flight state machine for `invocation_id` exists
    /// but represents a *different* (newer) attempt than `fencing_token`.
    ///
    /// This fences output from an aborted task that is still draining out of the
    /// task→invoker channel after the invocation has already been restarted: such
    /// output must neither mutate the new attempt's state machine nor be emitted
    /// as an effect stamped with the new attempt's epoch.
    #[inline]
    pub(super) fn is_stale_fencing_token(
        &self,
        invocation_id: &InvocationId,
        fencing_token: restate_types::invocation::FencingToken,
    ) -> bool {
        self.invocation_state_machines
            .get(invocation_id)
            .is_some_and(|ism| ism.fencing_token != fencing_token)
    }

    #[inline]
    pub(super) fn handle_for_invocation<R>(
        &mut self,
        invocation_id: &InvocationId,
        f: impl FnOnce(&mpsc::Sender<FencedEffect>, &mut InvocationStateMachine) -> R,
    ) -> Option<R> {
        if let Some((tx, ism)) = self.resolve_invocation(invocation_id) {
            Some(f(tx, ism))
        } else {
            trace!("No state machine found for invocation {invocation_id}");
            None
        }
    }

    #[inline]
    pub(super) fn remove_invocation(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Option<(&mpsc::Sender<FencedEffect>, &SR, InvocationStateMachine)> {
        self.invocation_state_machines
            .remove(invocation_id)
            .map(|ism| (&self.output_tx, &self.storage_reader, ism))
    }

    #[inline]
    pub(super) fn remove_all(&mut self) -> HashMap<InvocationId, InvocationStateMachine> {
        std::mem::take(&mut self.invocation_state_machines)
    }

    #[inline]
    pub(super) fn register_invocation(&mut self, id: InvocationId, ism: InvocationStateMachine) {
        self.invocation_state_machines.insert(id, ism);
    }
}
