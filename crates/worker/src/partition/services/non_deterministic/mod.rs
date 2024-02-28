// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use crate::partition::storage::PartitionStorage;
use crate::partition::types::{create_response_message, OutboxMessageExt, ResponseMessage};
use bytes::Bytes;
use restate_pb::builtin_service::ManualResponseBuiltInService;
use restate_pb::restate::internal::IdempotentInvokerInvoker;
use restate_pb::restate::internal::RemoteContextInvoker;
use restate_pb::restate::IngressInvoker;
use restate_schema_impl::Schemas;
use restate_storage_api::invocation_status_table::NotificationTarget;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::errors::{InvocationError, UserErrorCode};
use restate_types::identifiers::{EntryIndex, FullInvocationId};
use restate_types::ingress::IngressResponse;
use restate_types::invocation::{
    ResponseResult, ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source,
};
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::effects::{BuiltinServiceEffect, BuiltinServiceEffects};
use std::collections::HashMap;
use std::ops::Deref;
use tokio::sync::mpsc;
use tracing::warn;

mod idempotent_invoker;
mod ingress;
mod remote_context;

// TODO Replace with bounded channels but this requires support for spilling on the sender side
pub(crate) type EffectsSender = mpsc::UnboundedSender<BuiltinServiceEffects>;
pub(crate) type EffectsReceiver = mpsc::UnboundedReceiver<BuiltinServiceEffects>;

pub(crate) struct ServiceInvoker<'a> {
    storage: PartitionStorage<RocksDBStorage>,
    effects_tx: EffectsSender,
    schemas: &'a Schemas,
}

impl<'a> ServiceInvoker<'a> {
    pub(crate) fn is_supported(service_name: &str) -> bool {
        // The reason we just check for the prefix is the following:
        //
        // * No user can register services starting with dev.restate. This is checked in the schema registry.
        // * We already checked in the previous step of the state machine whether the service is a deterministic built-in service
        // * Hence with this assertion we can 404 sooner in case the user inputs a bad built-in service name, avoiding to get it stuck in the invoker
        service_name.starts_with("dev.restate")
    }

    pub(crate) fn new(
        storage: PartitionStorage<RocksDBStorage>,
        schemas: &'a Schemas,
    ) -> (Self, EffectsReceiver) {
        let (effects_tx, effects_rx) = mpsc::unbounded_channel();

        (
            ServiceInvoker {
                storage,
                schemas,
                effects_tx,
            },
            effects_rx,
        )
    }

    pub(crate) async fn invoke(
        &mut self,
        full_invocation_id: FullInvocationId,
        method: &str,
        span_context: ServiceInvocationSpanContext,
        response_sink: Option<ServiceInvocationResponseSink>,
        argument: Bytes,
    ) {
        let mut out_effects = vec![];
        let mut state_and_journal_transitions = StateAndJournalTransitions::default();
        let invocation_context = InvocationContext {
            full_invocation_id: &full_invocation_id,
            span_context: &span_context,
            state_reader: &mut self.storage,
            schemas: self.schemas,
            response_sink: response_sink.as_ref(),
            effects_buffer: &mut out_effects,
            state_and_journal_transitions: &mut state_and_journal_transitions,
        };

        let result = match full_invocation_id.service_id.service_name.deref() {
            restate_pb::INGRESS_SERVICE_NAME => {
                IngressInvoker(invocation_context)
                    .invoke_builtin(method, argument)
                    .await
            }
            restate_pb::REMOTE_CONTEXT_SERVICE_NAME => {
                RemoteContextInvoker(invocation_context)
                    .invoke_builtin(method, argument)
                    .await
            }
            restate_pb::IDEMPOTENT_INVOKER_SERVICE_NAME => {
                IdempotentInvokerInvoker(invocation_context)
                    .invoke_builtin(method, argument)
                    .await
            }
            _ => Err(InvocationError::service_not_found(
                &full_invocation_id.service_id.service_name,
            )),
        };

        // Fill effect buffers with set/clear state, append journal and end effect
        state_and_journal_transitions.fill_effects_buffer(&mut out_effects);
        match result {
            Ok(()) => {
                // Just append End
                out_effects.push(BuiltinServiceEffect::End(None));
            }
            Err(e) => {
                warn!(
                    rpc.service = %full_invocation_id.service_id.service_name,
                    restate.invocation.id = %full_invocation_id,
                    "Invocation to built-in service failed with {}",
                    e);
                // Clear effects, and append end error
                out_effects.clear();
                out_effects.push(BuiltinServiceEffect::End(Some(e)))
            }
        }

        // Send the effects
        // the receiver channel should only be shut down if the system is shutting down
        let _ = self
            .effects_tx
            .send(BuiltinServiceEffects::new(full_invocation_id, out_effects));
    }
}

#[derive(Clone, Default)]
struct StateAndJournalTransitions {
    state_entries: HashMap<String, Option<Bytes>>,
    journal_entries: HashMap<(InvocationId, EntryIndex), EnrichedRawEntry>,
}

impl StateAndJournalTransitions {
    fn fill_effects_buffer(mut self, effects: &mut Vec<BuiltinServiceEffect>) {
        for (key, opt_value) in self.state_entries.into_iter() {
            if let Some(value) = opt_value {
                effects.push(BuiltinServiceEffect::SetState {
                    key: key.into(),
                    value,
                });
            } else {
                effects.push(BuiltinServiceEffect::ClearState(key.into()));
            }
        }

        // We need to send effects in order for journal entries.
        let mut order: Vec<_> = self.journal_entries.keys().cloned().collect();
        order.sort();
        for key in order {
            let ((invocation_id, entry_index), journal_entry) =
                self.journal_entries.remove_entry(&key).unwrap();
            effects.push(BuiltinServiceEffect::StoreEntry {
                invocation_id,
                entry_index,
                journal_entry,
            });
        }
    }
}

struct InvocationContext<'a, S> {
    // Invocation metadata
    full_invocation_id: &'a FullInvocationId,
    state_reader: &'a mut S,
    schemas: &'a Schemas,
    span_context: &'a ServiceInvocationSpanContext,
    response_sink: Option<&'a ServiceInvocationResponseSink>,

    effects_buffer: &'a mut Vec<BuiltinServiceEffect>,
    state_and_journal_transitions: &'a mut StateAndJournalTransitions,
}

impl<S: StateReader> InvocationContext<'_, S> {
    async fn load_journal_metadata(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<Option<JournalMetadata>, InvocationError> {
        self.state_reader
            .read_virtual_journal_metadata(invocation_id)
            .await
            .map_err(InvocationError::internal)
    }

    fn create_journal(
        &mut self,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        completion_notification_target: NotificationTarget,
        kill_notification_target: NotificationTarget,
    ) {
        self.effects_buffer
            .push(BuiltinServiceEffect::CreateJournal {
                invocation_id,
                span_context,
                completion_notification_target,
                kill_notification_target,
            });
    }

    async fn read_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
    ) -> Result<Option<EnrichedRawEntry>, InvocationError> {
        Ok(
            if let Some(enriched_entry) = self
                .state_and_journal_transitions
                .journal_entries
                .get(&(invocation_id.clone(), entry_index))
            {
                Some(enriched_entry.clone())
            } else {
                self.state_reader
                    .read_virtual_journal_entry(invocation_id, entry_index)
                    .await
                    .map_err(InvocationError::internal)?
            },
        )
    }

    fn store_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.state_and_journal_transitions
            .journal_entries
            .insert((invocation_id, entry_index), journal_entry);
    }

    fn drop_journal(&mut self, invocation_id: &InvocationId, journal_length: EntryIndex) {
        self.state_and_journal_transitions.journal_entries.clear();
        self.effects_buffer.push(BuiltinServiceEffect::DropJournal {
            invocation_id: invocation_id.clone(),
            journal_length,
        });
    }

    async fn load_state_raw<Serde>(
        &mut self,
        key: &StateKey<Serde>,
    ) -> Result<Option<Bytes>, InvocationError> {
        Ok(
            if let Some(opt_val) = self
                .state_and_journal_transitions
                .state_entries
                .get(key.0.as_ref())
            {
                opt_val.clone()
            } else {
                self.state_reader
                    .read_state(&self.full_invocation_id.service_id, &key.0)
                    .await
                    .map_err(InvocationError::internal)?
            },
        )
    }

    async fn load_state<Serde: StateSerde>(
        &mut self,
        key: &StateKey<Serde>,
    ) -> Result<Option<Serde::MaterializedType>, InvocationError> {
        self.load_state_raw(key)
            .await?
            .map(|value| Serde::decode(value))
            .transpose()
            .map_err(InvocationError::internal)
    }

    async fn pop_state<Serde: StateSerde>(
        &mut self,
        key: &StateKey<Serde>,
    ) -> Result<Option<Serde::MaterializedType>, InvocationError> {
        let res = self.load_state(key).await;

        if let Ok(Some(_)) = &res {
            self.clear_state(key)
        };

        res
    }

    async fn load_state_or_fail<Serde: StateSerde>(
        &mut self,
        key: &StateKey<Serde>,
    ) -> Result<Serde::MaterializedType, InvocationError> {
        self.load_state(key).await.and_then(|optional_value| {
            optional_value.ok_or_else(|| {
                InvocationError::internal(format!(
                    "Expected {} state to be present. This is most likely a Restate bug.",
                    key.0
                ))
            })
        })
    }

    fn set_state<Serde: StateSerde>(
        &mut self,
        key: &StateKey<Serde>,
        value: &Serde::MaterializedType,
    ) -> Result<(), InvocationError> {
        self.state_and_journal_transitions.state_entries.insert(
            key.0.to_string(),
            Some(Serde::encode(value).map_err(InvocationError::internal)?),
        );
        Ok(())
    }

    fn clear_state<Serde>(&mut self, key: &StateKey<Serde>) {
        self.state_and_journal_transitions
            .state_entries
            .insert(key.0.to_string(), None);
    }

    #[allow(clippy::too_many_arguments)]
    fn delay_invoke(
        &mut self,
        target_fid: FullInvocationId,
        target_method: String,
        argument: Bytes,
        source: Source,
        response_sink: Option<ServiceInvocationResponseSink>,
        time: MillisSinceEpoch,
        timer_index: EntryIndex,
    ) {
        // Perhaps we can internally keep track of the timer index here?
        self.effects_buffer
            .push(BuiltinServiceEffect::DelayedInvoke {
                target_fid,
                target_method,
                argument,
                source,
                response_sink,
                time,
                timer_index,
            });
    }

    fn send_response(&mut self, msg: ResponseMessage) {
        match msg {
            ResponseMessage::Outbox(outbox) => self.outbox_message(outbox),
            ResponseMessage::Ingress(ingress) => self.ingress_response(ingress),
        }
    }

    fn outbox_message(&mut self, msg: OutboxMessage) {
        self.effects_buffer
            .push(BuiltinServiceEffect::OutboxMessage(msg));
    }

    fn ingress_response(&mut self, response: IngressResponse) {
        self.effects_buffer
            .push(BuiltinServiceEffect::IngressResponse(response));
    }

    fn reply_to_caller(&mut self, res: ResponseResult) {
        if let Some(response_sink) = self.response_sink {
            self.send_response(create_response_message(
                self.full_invocation_id,
                response_sink.clone(),
                res,
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::services::tests::MockStateReader;
    use futures::future::LocalBoxFuture;
    use restate_types::GenerationalNodeId;

    impl MockStateReader {
        pub(super) fn apply_effects(&mut self, effects: &[BuiltinServiceEffect]) {
            for effect in effects {
                match effect {
                    BuiltinServiceEffect::SetState { key, value } => {
                        self.0.insert(key.to_string(), value.clone());
                    }
                    BuiltinServiceEffect::ClearState(key) => {
                        self.0.remove(&key.to_string());
                    }
                    BuiltinServiceEffect::CreateJournal { span_context, .. } => {
                        self.1 = Some((
                            JournalMetadata::new(0, span_context.clone()),
                            Vec::default(),
                        ));
                    }
                    BuiltinServiceEffect::StoreEntry {
                        journal_entry,
                        entry_index,
                        ..
                    } => {
                        let (meta, v) = self.1.as_mut().unwrap();
                        meta.length += 1;
                        v.insert(*entry_index as usize, journal_entry.clone())
                    }
                    BuiltinServiceEffect::DropJournal { journal_length, .. } => {
                        assert_eq!(self.1.as_ref().unwrap().0.length, *journal_length);
                        self.1 = None
                    }
                    _ => {}
                }
            }
        }
    }

    #[derive(Clone)]
    pub(super) struct TestInvocationContext {
        service_id: ServiceId,
        state_reader: MockStateReader,
        schemas: Schemas,
        response_sink: Option<ServiceInvocationResponseSink>,
    }

    impl TestInvocationContext {
        pub(super) fn new(service_name: &str) -> Self {
            Self::from_service_id(ServiceId::new(service_name, Bytes::new()))
        }

        pub(super) fn from_service_id(service_id: ServiceId) -> Self {
            Self {
                service_id,
                state_reader: Default::default(),
                schemas: Default::default(),
                response_sink: Some(ServiceInvocationResponseSink::Ingress(
                    GenerationalNodeId::new(1, 1),
                )),
            }
        }

        pub(super) fn with_schemas(mut self, schemas: Schemas) -> Self {
            self.schemas = schemas;
            self
        }

        pub(super) fn state(&self) -> &MockStateReader {
            &self.state_reader
        }

        pub(super) fn response_sink(&self) -> Option<&ServiceInvocationResponseSink> {
            self.response_sink.as_ref()
        }

        pub(super) fn state_mut(&mut self) -> &mut MockStateReader {
            &mut self.state_reader
        }

        pub(super) async fn invoke<'this, F>(
            &'this mut self,
            f: F,
        ) -> Result<(FullInvocationId, Vec<BuiltinServiceEffect>), InvocationError>
        where
            F: for<'fut> FnOnce(
                &'fut mut InvocationContext<'fut, MockStateReader>,
            ) -> LocalBoxFuture<'fut, Result<(), InvocationError>>,
        {
            let fid = FullInvocationId::generate(self.service_id.clone());
            let mut out_effects = vec![];
            let mut state_and_journal_transitions = StateAndJournalTransitions::default();
            let mut invocation_ctx = InvocationContext {
                full_invocation_id: &fid,
                span_context: &ServiceInvocationSpanContext::empty(),
                state_reader: &mut self.state_reader,
                schemas: &self.schemas,
                response_sink: self.response_sink.as_ref(),
                effects_buffer: &mut out_effects,
                state_and_journal_transitions: &mut state_and_journal_transitions,
            };

            f(&mut invocation_ctx).await?;

            // Apply effects
            state_and_journal_transitions.fill_effects_buffer(&mut out_effects);
            self.state_reader.apply_effects(&out_effects);

            Ok((fid, out_effects))
        }
    }
}
