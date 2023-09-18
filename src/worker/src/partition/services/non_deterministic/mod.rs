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
use restate_types::invocation::{ResponseResult, ServiceInvocationResponseSink};
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
#[derive(Debug)]
pub(crate) enum Effect {
    SetState {
        key: Cow<'static, str>,
        value: Bytes,
    },
    ClearState(Cow<'static, str>),
    OutboxMessage(OutboxMessage),
    RegisterTimer,
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
        argument: Bytes,
        response_sink: Option<ServiceInvocationResponseSink>,
    ) {
        let mut invocation_context = InvocationContext {
            full_invocation_id: &full_invocation_id,
            state_reader: self.storage,
            schemas: self.schemas,
            effects_buffer: vec![],
            response_sink,
        };

        let result: Result<(), InvocationError> =
            match full_invocation_id.service_id.service_name.deref() {
                restate_pb::INGRESS_SERVICE_NAME => {
                    IngressInvoker(&mut invocation_context)
                        .invoke_builtin(method, argument)
                        .await
                }
                _ => Err(InvocationError::new(
                    UserErrorCode::NotFound,
                    format!("{} not found", full_invocation_id.service_id.service_name),
                )),
            };

        // Add the end effect
        let mut effects = invocation_context.effects_buffer;
        effects.push(Effect::End(result.err()));

        // Send the effects
        // the receiver channel should only be shut down if the system is shutting down
        let _ = self.effects_tx.send(Effects {
            full_invocation_id,
            effects,
        });
    }
}

struct InvocationContext<'a, S> {
    full_invocation_id: &'a FullInvocationId,
    #[allow(unused)]
    state_reader: S,
    schemas: &'a Schemas,
    effects_buffer: Vec<Effect>,
    response_sink: Option<ServiceInvocationResponseSink>,
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

    fn store_state<Serde: StateSerde>(
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

    fn send_message(&mut self, msg: OutboxMessage) {
        self.effects_buffer.push(Effect::OutboxMessage(msg))
    }

    fn reply_to_caller(&mut self, res: ResponseResult) {
        if let Some(response_sink) = self.response_sink.as_ref() {
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
    use restate_test_util::assert;
    use std::fmt;

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

        pub(super) fn assert_has_state<Serde: StateSerde + fmt::Debug>(
            &self,
            key: &StateKey<Serde>,
        ) -> Serde::MaterializedType {
            Serde::decode(
                self.0
                    .get(key.0.as_ref())
                    .unwrap_or_else(|| panic!("{:?} must be non-empty", key))
                    .clone(),
            )
            .unwrap_or_else(|_| panic!("{:?} must deserialize correctly", key))
        }

        pub(super) fn assert_has_not_state<Serde: StateSerde + fmt::Debug>(
            &self,
            key: &StateKey<Serde>,
        ) {
            assert!(self.0.get(key.0.as_ref()).is_none());
        }
    }
}
