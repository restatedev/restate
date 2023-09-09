// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::storage::PartitionStorage;
use bytes::Bytes;
use restate_pb::builtin_service::NonDeterministicBuiltInService;
use restate_pb::restate::services::IngressNonDeterministicInvoker;
use restate_schema_impl::Schemas;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::errors::{InvocationError, UserErrorCode};
use restate_types::identifiers::FullInvocationId;
use restate_types::invocation::ServiceInvocationResponseSink;
use std::ops::Deref;
use tokio::sync::mpsc;

pub(crate) fn is_built_in_service(service_name: &str) -> bool {
    // TODO how to deal with changing sets of built in services? Should we just say dev.restate is always a built-in service?
    service_name == restate_pb::INGRESS_SERVICE_NAME
}

#[derive(Debug)]
pub(crate) struct Effect {
    pub(crate) full_invocation_id: FullInvocationId,
    pub(crate) kind: Kind,
}

impl Effect {
    fn new(full_invocation_id: FullInvocationId, kind: Kind) -> Self {
        Effect {
            full_invocation_id,
            kind,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum Kind {
    SetState { key: String, value: Bytes },
    ClearState(String),
    OutboxMessage(OutboxMessage),
    RegisterTimer,
    End,
    Failed(InvocationError),
}

// TODO Replace with bounded channels but this requires support for spilling on the sender side
pub(crate) type OutputSender = mpsc::UnboundedSender<Effect>;
pub(crate) type OutputReceiver = mpsc::UnboundedReceiver<Effect>;

pub(crate) struct ServiceInvoker<'a> {
    _storage: &'a PartitionStorage<RocksDBStorage>,
    output_tx: OutputSender,
    schemas: &'a Schemas,
}

impl<'a> ServiceInvoker<'a> {
    pub(crate) fn new(
        storage: &'a PartitionStorage<RocksDBStorage>,
        schemas: &'a Schemas,
    ) -> (Self, OutputReceiver) {
        let (output_tx, output_rx) = mpsc::unbounded_channel();

        (
            ServiceInvoker {
                _storage: storage,
                schemas,
                output_tx,
            },
            output_rx,
        )
    }

    pub(crate) async fn invoke(
        &self,
        full_invocation_id: FullInvocationId,
        response_sink: Option<ServiceInvocationResponseSink>,
        method: &str,
        argument: Bytes,
    ) {
        if let Err(err) = self.invoke_builtin(&full_invocation_id, response_sink, method, argument)
        {
            // the receiver channel should only be shut down if the system is shutting down
            let _ = self
                .output_tx
                .send(Effect::new(full_invocation_id, Kind::Failed(err)));
        } else {
            // the receiver channel should only be shut down if the system is shutting down
            let _ = self
                .output_tx
                .send(Effect::new(full_invocation_id, Kind::End));
        }
    }

    fn invoke_builtin(
        &self,
        full_invocation_id: &FullInvocationId,
        response_sink: Option<ServiceInvocationResponseSink>,
        method: &str,
        argument: Bytes,
    ) -> Result<(), InvocationError> {
        match full_invocation_id.service_id.service_name.deref() {
            restate_pb::INGRESS_SERVICE_NAME => IngressNonDeterministicInvoker(self)
                .invoke_builtin(method, full_invocation_id, response_sink, argument),
            _ => Err(InvocationError::new(
                UserErrorCode::NotFound,
                format!("{} not found", full_invocation_id.service_id.service_name),
            )),
        }
    }
}

mod ingress {
    use crate::partition::services::non_deterministic::{Effect, Kind, ServiceInvoker};
    use prost_reflect::prost::Message;
    use restate_pb::restate::services::{
        IngressNonDeterministicBuiltInService, InvokeRequest, InvokeResponse,
    };
    use restate_schema_api::key::KeyExtractor;
    use restate_storage_api::outbox_table::OutboxMessage;
    use restate_types::errors::{InvocationError, UserErrorCode};
    use restate_types::identifiers::{FullInvocationId, InvocationUuid};
    use restate_types::invocation::{
        ResponseResult, ServiceInvocation, ServiceInvocationResponseSink, SpanRelation,
    };

    impl<'a> IngressNonDeterministicBuiltInService for ServiceInvoker<'a> {
        fn invoke(
            &self,
            full_invocation_id: &FullInvocationId,
            response_sink: Option<ServiceInvocationResponseSink>,
            input: InvokeRequest,
        ) -> Result<(), InvocationError> {
            if let Some(ServiceInvocationResponseSink::Ingress(ingress_dispatcher_id)) =
                response_sink
            {
                // Extract the key
                let key = self
                    .schemas
                    .extract(&input.service, &input.method, input.argument.clone())
                    .map_err(|err| match err {
                        restate_schema_api::key::KeyExtractorError::NotFound => {
                            InvocationError::new(
                                UserErrorCode::NotFound,
                                format!(
                                    "Service method {}/{} not found",
                                    input.service, input.method
                                ),
                            )
                        }
                        err => {
                            InvocationError::new(UserErrorCode::InvalidArgument, err.to_string())
                        }
                    })?;

                let fid = FullInvocationId::new(input.service, key, InvocationUuid::now_v7());

                let response = ResponseResult::Success(
                    InvokeResponse {
                        id: fid.to_string(),
                    }
                    .encode_to_vec()
                    .into(),
                );

                // TODO we could think about batching the effects of the nbis services
                // invoke service
                let _ = self.output_tx.send(Effect::new(
                    full_invocation_id.clone(),
                    Kind::OutboxMessage(OutboxMessage::ServiceInvocation(ServiceInvocation::new(
                        fid,
                        input.method,
                        input.argument,
                        None,
                        SpanRelation::None,
                    ))),
                ));

                // respond to caller
                let _ = self.output_tx.send(Effect::new(
                    full_invocation_id.clone(),
                    Kind::OutboxMessage(OutboxMessage::IngressResponse {
                        ingress_dispatcher_id,
                        full_invocation_id: full_invocation_id.clone(),
                        response,
                    }),
                ));

                Ok(())
            } else {
                Err(InvocationError::new(
                    UserErrorCode::FailedPrecondition,
                    format!(
                        "Built-in service '{}' must be invoked only from the ingress.",
                        restate_pb::INGRESS_SERVICE_NAME
                    ),
                ))
            }
        }
    }
}
