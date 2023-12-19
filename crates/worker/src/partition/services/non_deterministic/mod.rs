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
use crate::partition::types::OutboxMessageExt;
use bytes::Bytes;
use restate_pb::builtin_service::ManualResponseBuiltInService;
use restate_pb::restate::internal::IdempotentInvokerInvoker;
use restate_pb::restate::internal::RemoteContextInvoker;
use restate_pb::restate::IngressInvoker;
use restate_schema_impl::Schemas;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::status_table::NotificationTarget;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::errors::{InvocationError, UserErrorCode};
use restate_types::identifiers::{EntryIndex, FullInvocationId, InvocationUuid};
use restate_types::invocation::{
    ResponseResult, ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source,
};
use restate_types::time::MillisSinceEpoch;
use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;
use tokio::sync::mpsc;
use tracing::warn;

mod idempotent_invoker;
mod ingress;
mod remote_context;

#[derive(Debug)]
pub struct Effects {
    full_invocation_id: FullInvocationId,
    effects: Vec<Effect>,
}

impl Effects {
    pub(crate) fn new(full_invocation_id: FullInvocationId, effects: Vec<Effect>) -> Self {
        Self {
            full_invocation_id,
            effects,
        }
    }

    pub(crate) fn into_inner(self) -> (FullInvocationId, Vec<Effect>) {
        (self.full_invocation_id, self.effects)
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Effect {
    CreateJournal {
        service_id: ServiceId,
        invocation_uuid: InvocationUuid,
        span_context: ServiceInvocationSpanContext,
        completion_notification_target: NotificationTarget,
        kill_notification_target: NotificationTarget,
    },
    StoreEntry {
        service_id: ServiceId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    },
    DropJournal {
        service_id: ServiceId,
        journal_length: EntryIndex,
    },

    SetState {
        key: Cow<'static, str>,
        value: Bytes,
    },
    ClearState(Cow<'static, str>),

    OutboxMessage(OutboxMessage),
    DelayedInvoke {
        target_fid: FullInvocationId,
        target_method: String,
        argument: Bytes,
        source: Source,
        response_sink: Option<ServiceInvocationResponseSink>,
        time: MillisSinceEpoch,
        timer_index: EntryIndex,
    },

    End(
        // NBIS can optionally fail, depending on the context the error might or might not be used.
        Option<InvocationError>,
    ),
}

// TODO Replace with bounded channels but this requires support for spilling on the sender side
pub(crate) type EffectsSender = mpsc::UnboundedSender<Effects>;
pub(crate) type EffectsReceiver = mpsc::UnboundedReceiver<Effects>;

pub(crate) struct ServiceInvoker<'a> {
    storage: &'a PartitionStorage<RocksDBStorage>,
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
        storage: &'a PartitionStorage<RocksDBStorage>,
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

    pub(crate) async fn invoke<'b: 'a>(
        &'a self,
        full_invocation_id: FullInvocationId,
        method: &'b str,
        span_context: ServiceInvocationSpanContext,
        response_sink: Option<ServiceInvocationResponseSink>,
        argument: Bytes,
    ) {
        let mut out_effects = vec![];
        let mut state_transitions = StateTransitions::default();
        let invocation_context = InvocationContext {
            full_invocation_id: &full_invocation_id,
            span_context: &span_context,
            state_reader: self.storage,
            schemas: self.schemas,
            response_sink: response_sink.as_ref(),
            effects_buffer: &mut out_effects,
            state_transitions: &mut state_transitions,
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

        // Fill effect buffers with set state and end effect
        state_transitions.fill_effects_buffer(&mut out_effects);
        match result {
            Ok(()) => {
                // Just append End
                out_effects.push(Effect::End(None));
            }
            Err(e) => {
                warn!(
                    rpc.service = %full_invocation_id.service_id.service_name,
                    restate.invocation.id = %full_invocation_id,
                    "Invocation to built-in service failed with {}",
                    e);
                // Clear effects, and append end error
                out_effects.clear();
                out_effects.push(Effect::End(Some(e)))
            }
        }

        // Send the effects
        // the receiver channel should only be shut down if the system is shutting down
        let _ = self
            .effects_tx
            .send(Effects::new(full_invocation_id, out_effects));
    }
}

#[derive(Clone, Default)]
struct StateTransitions(HashMap<String, Option<Bytes>>);

impl StateTransitions {
    fn fill_effects_buffer(self, effects: &mut Vec<Effect>) {
        for (key, opt_value) in self.0.into_iter() {
            if let Some(value) = opt_value {
                effects.push(Effect::SetState {
                    key: key.into(),
                    value,
                })
            } else {
                effects.push(Effect::ClearState(key.into()))
            }
        }
    }
}

struct InvocationContext<'a, S> {
    // Invocation metadata
    full_invocation_id: &'a FullInvocationId,
    state_reader: S,
    schemas: &'a Schemas,
    span_context: &'a ServiceInvocationSpanContext,
    response_sink: Option<&'a ServiceInvocationResponseSink>,

    effects_buffer: &'a mut Vec<Effect>,
    state_transitions: &'a mut StateTransitions,
}

impl<S: StateReader> InvocationContext<'_, S> {
    async fn load_journal_metadata(
        &self,
        service_id: &ServiceId,
    ) -> Result<Option<(InvocationUuid, JournalMetadata)>, InvocationError> {
        self.state_reader
            .read_virtual_journal_metadata(service_id)
            .await
            .map_err(InvocationError::internal)
    }

    async fn load_journal(
        &self,
        service_id: &ServiceId,
    ) -> Result<Option<(InvocationUuid, JournalMetadata, Vec<EnrichedRawEntry>)>, InvocationError>
    {
        if let Some((invocation_uuid, journal_metadata)) = self
            .state_reader
            .read_virtual_journal_metadata(service_id)
            .await
            .map_err(InvocationError::internal)?
        {
            let mut vec = Vec::with_capacity(journal_metadata.length as usize);
            for i in 0..journal_metadata.length {
                vec.insert(
                    i as usize,
                    self.state_reader
                        .read_virtual_journal_entry(service_id, i)
                        .await
                        .map_err(InvocationError::internal)?
                        .ok_or_else(|| {
                            InvocationError::internal(format!("Missing journal entry {}", i))
                        })?,
                );
            }
            Ok(Some((invocation_uuid, journal_metadata, vec)))
        } else {
            Ok(None)
        }
    }

    fn create_journal(
        &mut self,
        service_id: ServiceId,
        invocation_uuid: InvocationUuid,
        span_context: ServiceInvocationSpanContext,
        completion_notification_target: NotificationTarget,
        kill_notification_target: NotificationTarget,
    ) {
        self.effects_buffer.push(Effect::CreateJournal {
            service_id,
            invocation_uuid,
            span_context,
            completion_notification_target,
            kill_notification_target,
        });
    }

    fn store_journal_entry(
        &mut self,
        service_id: ServiceId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.effects_buffer.push(Effect::StoreEntry {
            service_id,
            entry_index,
            journal_entry,
        });
    }

    fn drop_journal(&mut self, service_id: ServiceId, journal_length: EntryIndex) {
        self.effects_buffer.push(Effect::DropJournal {
            service_id,
            journal_length,
        });
    }

    async fn load_state_raw<Serde>(
        &self,
        key: &StateKey<Serde>,
    ) -> Result<Option<Bytes>, InvocationError> {
        Ok(
            if let Some(opt_val) = self.state_transitions.0.get(key.0.as_ref()) {
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
        &self,
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
        &self,
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
        self.state_transitions.0.insert(
            key.0.to_string(),
            Some(Serde::encode(value).map_err(InvocationError::internal)?),
        );
        Ok(())
    }

    fn clear_state<Serde>(&mut self, key: &StateKey<Serde>) {
        self.state_transitions.0.insert(key.0.to_string(), None);
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
        self.effects_buffer.push(Effect::DelayedInvoke {
            target_fid,
            target_method,
            argument,
            source,
            response_sink,
            time,
            timer_index,
        });
    }

    fn send_message(&mut self, msg: OutboxMessage) {
        self.effects_buffer.push(Effect::OutboxMessage(msg))
    }

    fn reply_to_caller(&mut self, res: ResponseResult) {
        if let Some(response_sink) = self.response_sink {
            self.effects_buffer
                .push(Effect::OutboxMessage(OutboxMessage::from_response_sink(
                    self.full_invocation_id,
                    response_sink.clone(),
                    res,
                )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::services::tests::MockStateReader;
    use futures::future::LocalBoxFuture;
    use restate_types::identifiers::{IngressDispatcherId, InvocationUuid};

    impl MockStateReader {
        pub(super) fn apply_effects(&mut self, effects: &[Effect]) {
            for effect in effects {
                match effect {
                    Effect::SetState { key, value } => {
                        self.0.insert(key.to_string(), value.clone());
                    }
                    Effect::ClearState(key) => {
                        self.0.remove(&key.to_string());
                    }
                    Effect::CreateJournal {
                        span_context,
                        invocation_uuid,
                        ..
                    } => {
                        self.1 = Some((
                            *invocation_uuid,
                            JournalMetadata::new(0, span_context.clone()),
                            Vec::default(),
                        ));
                    }
                    Effect::StoreEntry {
                        journal_entry,
                        entry_index,
                        ..
                    } => {
                        let (_, meta, v) = self.1.as_mut().unwrap();
                        meta.length += 1;
                        v.insert(*entry_index as usize, journal_entry.clone())
                    }
                    Effect::DropJournal { journal_length, .. } => {
                        assert_eq!(self.1.as_ref().unwrap().1.length, *journal_length);
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
                    IngressDispatcherId::mock(),
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
        ) -> Result<(FullInvocationId, Vec<Effect>), InvocationError>
        where
            F: for<'fut> FnOnce(
                &'fut mut InvocationContext<'fut, &'fut MockStateReader>,
            ) -> LocalBoxFuture<'fut, Result<(), InvocationError>>,
        {
            let fid = FullInvocationId::with_service_id(
                self.service_id.clone(),
                InvocationUuid::now_v7(),
            );
            let mut out_effects = vec![];
            let mut state_transitions = StateTransitions::default();
            let mut invocation_ctx = InvocationContext {
                full_invocation_id: &fid,
                span_context: &ServiceInvocationSpanContext::empty(),
                state_reader: &self.state_reader,
                schemas: &self.schemas,
                response_sink: self.response_sink.as_ref(),
                effects_buffer: &mut out_effects,
                state_transitions: &mut state_transitions,
            };

            f(&mut invocation_ctx).await?;

            // Apply effects
            state_transitions.fill_effects_buffer(&mut out_effects);
            self.state_reader.apply_effects(&out_effects);

            Ok((fid, out_effects))
        }
    }
}
