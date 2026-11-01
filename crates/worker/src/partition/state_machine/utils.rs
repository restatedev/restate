// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::{Span, debug_span, trace_span};

use restate_storage_api::{
    StorageError, blob_table::ReadBlobTable, invocation_status_table::ResponseResultRef,
};
use restate_types::{
    errors::GONE_INVOCATION_ERROR,
    identifiers::InvocationId,
    invocation::{InvocationTarget, ResponseResult},
};
use restate_wal_protocol::v2::CommandKind;

pub(super) trait SpanExt {
    fn record_invocation_id(&self, id: &InvocationId);
    fn record_invocation_target(&self, target: &InvocationTarget);
}

impl SpanExt for tracing::Span {
    fn record_invocation_id(&self, id: &InvocationId) {
        self.record("restate.invocation.id", tracing::field::display(id));
    }

    fn record_invocation_target(&self, target: &InvocationTarget) {
        self.record("restate.invocation.target", tracing::field::display(target));
        self.record(
            "rpc.service",
            tracing::field::display(target.service_name()),
        );
        self.record("rpc.method", tracing::field::display(target.handler_name()));
    }
}

pub(super) fn state_machine_apply_command_span(is_leader: bool, record_kind: CommandKind) -> Span {
    if is_leader {
        debug_span!(
            "apply_command",
            otel.name = format!("apply-command: {}", record_kind),
            restate.invocation.id = tracing::field::Empty,
            restate.invocation.target = tracing::field::Empty,
            rpc.service = tracing::field::Empty,
            rpc.method = tracing::field::Empty,
            restate.state_machine.command = tracing::field::display(record_kind),
        )
    } else {
        trace_span!(
            "apply_command",
            otel.name = format!("apply-command: {}", record_kind),
            restate.invocation.id = tracing::field::Empty,
            restate.invocation.target = tracing::field::Empty,
            rpc.service = tracing::field::Empty,
            rpc.method = tracing::field::Empty,
            restate.state_machine.command = tracing::field::display(record_kind),
        )
    }
}

// Resolves and cache a ResponseResultRef and return
// a copy of the cached result on resolve
pub(crate) struct CachedResponseResultRef {
    inner: ResponseResultRef,
    cached: Option<ResponseResult>,
}

impl From<ResponseResultRef> for CachedResponseResultRef {
    fn from(inner: ResponseResultRef) -> Self {
        Self {
            inner,
            cached: None,
        }
    }
}

impl CachedResponseResultRef {
    pub async fn resolve<S: ReadBlobTable>(
        &mut self,
        storage: &mut S,
    ) -> Result<ResponseResult, StorageError> {
        // can we store in an arc?
        match &self.cached {
            Some(cached) => Ok(cached.clone()),
            None => {
                let result = self
                    .inner
                    .resolve(storage)
                    .await?
                    .unwrap_or_else(|| ResponseResult::from(GONE_INVOCATION_ERROR));

                self.cached = Some(result);
                Ok(self.cached.clone().unwrap())
            }
        }
    }
}
