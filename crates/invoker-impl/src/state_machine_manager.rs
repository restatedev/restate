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
use restate_worker_api::invoker::{Effect, invocation_reader::InvocationReader};

use crate::InvocationStateMachine;

/// Tree of [InvocationStateMachine] held by the [Service].
#[derive(Debug)]
pub(super) struct InvocationStateMachineManager<SR> {
    output_tx: mpsc::Sender<Box<Effect>>,
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
        sender: mpsc::Sender<Box<Effect>>,
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
    pub(super) fn partition_sender(&self) -> &mpsc::Sender<Box<Effect>> {
        &self.output_tx
    }

    #[inline]
    pub(super) fn resolve_invocation(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Option<(&mpsc::Sender<Box<Effect>>, &mut InvocationStateMachine)> {
        self.invocation_state_machines
            .get_mut(invocation_id)
            .map(|ism| (&self.output_tx, ism))
    }

    #[inline]
    pub(super) fn handle_for_invocation<R>(
        &mut self,
        invocation_id: &InvocationId,
        f: impl FnOnce(&mpsc::Sender<Box<Effect>>, &mut InvocationStateMachine) -> R,
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
    ) -> Option<(&mpsc::Sender<Box<Effect>>, &SR, InvocationStateMachine)> {
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
