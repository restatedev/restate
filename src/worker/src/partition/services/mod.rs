// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::effects::Effects;
use bytes::Bytes;
use restate_pb::builtin_service::BuiltInService;
use restate_pb::restate::services::*;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_types::errors::{InvocationError, UserErrorCode};
use restate_types::identifiers::FullInvocationId;
use restate_types::message::MessageIndex;
use std::ops::Deref;

// -- Deterministic built-in services infra

/// Deterministic built-in services are executed by both leaders and followers, hence they must generate the same output.
pub(super) struct DeterministicBuiltInServiceInvoker<'a> {
    fid: &'a FullInvocationId,
    outbox_seq_number: &'a mut MessageIndex,
    effects: &'a mut Effects,
}

impl<'a> DeterministicBuiltInServiceInvoker<'a> {
    pub(super) fn is_supported(service_name: &str) -> bool {
        service_name == restate_pb::AWAKEABLES_SERVICE_NAME
    }

    pub(super) fn invoke(
        fid: &'a FullInvocationId,
        outbox_seq_number: &'a mut MessageIndex,
        effects: &'a mut Effects,
        method: &'a str,
        argument: Bytes,
    ) -> Result<Bytes, InvocationError> {
        let this: DeterministicBuiltInServiceInvoker<'a> = Self {
            fid,
            outbox_seq_number,
            effects,
        };

        this._invoke(method, argument)
    }

    fn send_message(&mut self, msg: OutboxMessage) {
        self.effects
            .enqueue_into_outbox(*self.outbox_seq_number, msg);
        *self.outbox_seq_number += 1;
    }
}

impl DeterministicBuiltInServiceInvoker<'_> {
    // Function that routes through the available built-in services
    fn _invoke(self, method: &str, argument: Bytes) -> Result<Bytes, InvocationError> {
        match self.fid.service_id.service_name.deref() {
            restate_pb::AWAKEABLES_SERVICE_NAME => {
                AwakeablesInvoker(self).invoke_builtin(method, argument)
            }
            _ => Err(InvocationError::new(
                UserErrorCode::NotFound,
                format!("{} not found", self.fid.service_id.service_name),
            )),
        }
    }
}

// Non-deterministic built-in services infra

pub(super) struct NonDeterministicBuiltInServiceInvoker<'a> {
    fid: &'a FullInvocationId,
    outbox_messages_buffer: &'a mut Vec<OutboxMessage>
}

impl<'a> NonDeterministicBuiltInServiceInvoker<'a> {
    pub(super) fn is_supported(service_name: &str) -> bool {
        // The reason we just check for the prefix is the following:
        //
        // * No user can register services starting with dev.restate
        // * We already checked in the previous step of the state machine whether the service is a deterministic built-in service
        // * Hence with this assertion we can 404 sooner in case the user inputs a bad built-in service name, avoiding to get it stuck in the invoker
        service_name.starts_with("dev.restate")
    }

    pub(super) fn invoke(
        fid: &'a FullInvocationId,
        outbox_messages_buffer: &'a mut Vec<OutboxMessage>,
        method: &'a str,
        argument: Bytes,
    ) -> Result<Bytes, InvocationError> {
        let this: NonDeterministicBuiltInServiceInvoker<'a> = Self {
            fid,
            outbox_messages_buffer,
        };

        this._invoke(method, argument)
    }

    fn send_message(&mut self, msg: OutboxMessage) {
        self.outbox_messages_buffer.push(msg);
    }
}

impl NonDeterministicBuiltInServiceInvoker<'_> {
    // Function that routes through the available built-in services
    fn _invoke(self, _method: &str, _argument: Bytes) -> Result<Bytes, InvocationError> {
        match self.fid.service_id.service_name.deref() {
            _ => Err(InvocationError::new(
                UserErrorCode::NotFound,
                format!("{} not found", self.fid.service_id.service_name),
            )),
        }
    }
}

mod awakeables {
    use super::*;

    use prost_reflect::ReflectMessage;
    use restate_pb::restate::services::AwakeablesBuiltInService;
    use restate_service_protocol::awakeable_id::AwakeableIdentifier;
    use restate_types::invocation::{InvocationResponse, MaybeFullInvocationId};

    impl AwakeablesBuiltInService for DeterministicBuiltInServiceInvoker<'_> {
        fn resolve(&mut self, req: ResolveAwakeableRequest) -> Result<(), InvocationError> {
            let (invocation_id, entry_index) = AwakeableIdentifier::decode(req.id)
                .map_err(|e| InvocationError::new(UserErrorCode::InvalidArgument, e.to_string()))?
                .into_inner();

            let result = match req.result {
                None => {
                    return Err(InvocationError::new_static(
                        UserErrorCode::InvalidArgument.into(),
                        "result must be non-empty",
                    ));
                }
                Some(resolve_awakeable_request::Result::BytesResult(bytes)) => bytes,
                Some(resolve_awakeable_request::Result::JsonResult(value)) => Bytes::from(
                    serde_json::to_vec(&value.transcode_to_dynamic()).map_err(|e| {
                        InvocationError::new(UserErrorCode::InvalidArgument, e.to_string())
                    })?,
                ),
            };

            self.send_message(OutboxMessage::ServiceResponse(InvocationResponse {
                entry_index,
                result: Ok(result).into(),
                id: MaybeFullInvocationId::Partial(invocation_id),
            }));

            Ok(())
        }

        fn reject(&mut self, req: RejectAwakeableRequest) -> Result<(), InvocationError> {
            let (invocation_id, entry_index) = AwakeableIdentifier::decode(req.id)
                .map_err(|e| InvocationError::new(UserErrorCode::InvalidArgument, e.to_string()))?
                .into_inner();

            self.send_message(OutboxMessage::ServiceResponse(InvocationResponse {
                entry_index,
                result: Err(InvocationError::new(UserErrorCode::Unknown, req.reason)).into(),
                id: MaybeFullInvocationId::Partial(invocation_id),
            }));

            Ok(())
        }
    }
}
