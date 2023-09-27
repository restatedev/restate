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
use restate_pb::restate::IngressInvoker;
use restate_schema_impl::Schemas;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::errors::{InvocationError, UserErrorCode};
use restate_types::identifiers::{EntryIndex, FullInvocationId};
use restate_types::invocation::{
    ResponseResult, ServiceInvocationResponseSink, ServiceInvocationSpanContext,
};
use restate_types::time::MillisSinceEpoch;
use std::borrow::Cow;
use std::ops::Deref;
use tokio::sync::mpsc;

mod ingress;

#[derive(Debug)]
pub(crate) struct Effects {
    full_invocation_id: FullInvocationId,
    effects: Vec<Effect>,
}

impl Effects {
    pub(crate) fn into_inner(self) -> (FullInvocationId, Vec<Effect>) {
        (self.full_invocation_id, self.effects)
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum Effect {
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
        let invocation_context = InvocationContext {
            full_invocation_id: &full_invocation_id,
            span_context: &span_context,
            state_reader: self.storage,
            schemas: self.schemas,
            effects_buffer: &mut out_effects,
            response_sink: response_sink.as_ref(),
        };

        let result = match full_invocation_id.service_id.service_name.deref() {
            restate_pb::INGRESS_SERVICE_NAME => {
                IngressInvoker(invocation_context)
                    .invoke_builtin(method, argument)
                    .await
            }
            _ => Err(InvocationError::new(
                UserErrorCode::NotFound,
                format!("{} not found", full_invocation_id.service_id.service_name),
            )),
        };
        match result {
            Ok(()) => {
                // Just append End
                out_effects.push(Effect::End(None));
            }
            Err(e) => {
                // Clear effects, and append end error
                out_effects.clear();
                out_effects.push(Effect::End(Some(e)))
            }
        }

        // Send the effects
        // the receiver channel should only be shut down if the system is shutting down
        let _ = self.effects_tx.send(Effects {
            full_invocation_id,
            effects: out_effects,
        });
    }
}

struct InvocationContext<'a, S> {
    // Invocation metadata
    full_invocation_id: &'a FullInvocationId,
    span_context: &'a ServiceInvocationSpanContext,
    response_sink: Option<&'a ServiceInvocationResponseSink>,

    #[allow(unused)]
    state_reader: S,
    schemas: &'a Schemas,
    effects_buffer: &'a mut Vec<Effect>,
}

impl<S: StateReader> InvocationContext<'_, S> {
    async fn load_state<Serde: StateSerde>(
        &self,
        key: &StateKey<Serde>,
    ) -> Result<Option<Serde::MaterializedType>, InvocationError> {
        self.state_reader
            .read_state(&self.full_invocation_id.service_id, &key.0)
            .await
            .map_err(InvocationError::internal)?
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
        self.effects_buffer.push(Effect::SetState {
            key: key.0.clone(),
            value: Serde::encode(value).map_err(InvocationError::internal)?,
        });
        Ok(())
    }

    fn clear_state<Serde>(&mut self, key: &StateKey<Serde>) {
        self.effects_buffer.push(Effect::ClearState(key.0.clone()))
    }

    fn delay_invoke(
        &mut self,
        target_fid: FullInvocationId,
        target_method: String,
        argument: Bytes,
        response_sink: Option<ServiceInvocationResponseSink>,
        time: MillisSinceEpoch,
        timer_index: EntryIndex,
    ) {
        // Perhaps we can internally keep track of the timer index here?
        self.effects_buffer.push(Effect::DelayedInvoke {
            target_fid,
            target_method,
            argument,
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

        pub(super) fn with_state_reader(mut self, state_reader: MockStateReader) -> Self {
            self.state_reader = state_reader;
            self
        }

        pub(super) fn with_schemas(mut self, schemas: Schemas) -> Self {
            self.schemas = schemas;
            self
        }

        pub(super) fn without_response_sink(mut self) -> Self {
            self.response_sink = None;
            self
        }

        pub(super) fn state(&self) -> &MockStateReader {
            &self.state_reader
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
            let mut invocation_ctx = InvocationContext {
                full_invocation_id: &fid,
                span_context: &ServiceInvocationSpanContext::empty(),
                state_reader: &self.state_reader,
                schemas: &self.schemas,
                effects_buffer: &mut out_effects,
                response_sink: self.response_sink.as_ref(),
            };

            f(&mut invocation_ctx).await?;

            // Apply effects
            self.state_reader.apply_effects(&out_effects);

            Ok((fid, out_effects))
        }
    }
}
