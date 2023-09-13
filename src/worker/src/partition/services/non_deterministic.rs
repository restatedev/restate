// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::effects::StateStorage;
use crate::partition::storage::PartitionStorage;
use crate::partition::types::OutboxMessageExt;
use bytes::Bytes;
use restate_pb::builtin_service::ManualResponseBuiltInService;
use restate_pb::restate::IngressInvoker;
use restate_schema_impl::Schemas;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::errors::{InvocationError, UserErrorCode};
use restate_types::identifiers::FullInvocationId;
use restate_types::invocation::{ResponseResult, ServiceInvocationResponseSink};
use std::borrow::Cow;
use std::ops::Deref;
use tokio::sync::mpsc;

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
            storage: self.storage,
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

struct InvocationContext<'a> {
    full_invocation_id: &'a FullInvocationId,
    #[allow(unused)]
    storage: &'a PartitionStorage<RocksDBStorage>,
    schemas: &'a Schemas,
    effects_buffer: Vec<Effect>,
    response_sink: Option<ServiceInvocationResponseSink>,
}

impl InvocationContext<'_> {
    #[allow(unused)]
    async fn load_state_raw(&self, key: &str) -> Result<Option<Bytes>, InvocationError> {
        self.storage
            .create_transaction()
            // TODO modify the load_state interface to get rid of the Bytes for the key
            .load_state(
                &self.full_invocation_id.service_id,
                &Bytes::copy_from_slice(key.as_bytes()),
            )
            .await
            .map_err(InvocationError::internal)
    }

    #[allow(unused)]
    fn store_state_raw(&mut self, key: impl Into<Cow<'static, str>>, value: Bytes) {
        self.effects_buffer.push(Effect::SetState {
            key: key.into(),
            value,
        })
    }

    #[allow(unused)]
    fn clear_state(&mut self, key: impl Into<Cow<'static, str>>) {
        self.effects_buffer.push(Effect::ClearState(key.into()))
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

mod ingress {
    use super::*;

    use restate_pb::builtin_service::ResponseSerializer;
    use restate_pb::restate::*;
    use restate_schema_api::key::KeyExtractor;
    use restate_types::identifiers::InvocationUuid;
    use restate_types::invocation::{ServiceInvocation, SpanRelation};

    #[async_trait::async_trait]
    impl<'a> IngressBuiltInService for &mut InvocationContext<'a> {
        async fn invoke(
            &mut self,
            request: InvokeRequest,
            response_serializer: ResponseSerializer<InvokeResponse>,
        ) -> Result<(), InvocationError> {
            // Extract the key
            let key = self
                .schemas
                .extract(&request.service, &request.method, request.argument.clone())
                .map_err(|err| match err {
                    restate_schema_api::key::KeyExtractorError::NotFound => InvocationError::new(
                        UserErrorCode::NotFound,
                        format!(
                            "Service method {}/{} not found",
                            request.service, request.method
                        ),
                    ),
                    err => InvocationError::new(UserErrorCode::InvalidArgument, err.to_string()),
                })?;

            let fid = FullInvocationId::new(request.service, key, InvocationUuid::now_v7());

            // Respond to caller
            self.reply_to_caller(response_serializer.serialize_success(InvokeResponse {
                id: fid.to_string(),
            }));

            // Invoke service
            self.send_message(OutboxMessage::ServiceInvocation(ServiceInvocation::new(
                fid,
                request.method,
                request.argument,
                None,
                SpanRelation::None,
            )));

            Ok(())
        }
    }
}
