// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use tracing::{Level, Span, debug_span, event_enabled, trace_span};

use restate_types::invocation::InvocationEpoch;
use restate_types::{identifiers::InvocationId, invocation::InvocationTarget};
use restate_wal_protocol::v2::RecordKind;

pub(super) trait SpanExt {
    fn record_invocation_id(&self, id: &InvocationId);
    fn record_invocation_target(&self, target: &InvocationTarget);
    fn record_invocation_epoch(&self, target: &InvocationEpoch);
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

    fn record_invocation_epoch(&self, id: &InvocationEpoch) {
        self.record("restate.invocation.epoch", id);
    }
}

pub(super) fn state_machine_apply_command_span(is_leader: bool, cmd: RecordKind) -> Span {
    let span = if is_leader {
        debug_span!(
            "apply_command",
            otel.name = format!("apply-command: {}", cmd),
            restate.invocation.id = tracing::field::Empty,
            restate.invocation.epoch = tracing::field::Empty,
            restate.invocation.target = tracing::field::Empty,
            rpc.service = tracing::field::Empty,
            rpc.method = tracing::field::Empty,
            restate.state_machine.command = tracing::field::debug(cmd),
        )
    } else {
        trace_span!(
            "apply_command",
            otel.name = format!("apply-command: {}", cmd),
            restate.invocation.id = tracing::field::Empty,
            restate.invocation.epoch = tracing::field::Empty,
            restate.invocation.target = tracing::field::Empty,
            rpc.service = tracing::field::Empty,
            rpc.method = tracing::field::Empty,
            restate.state_machine.command = tracing::field::debug(cmd),
        )
    };
    if event_enabled!(Level::TRACE) {
        span.record("restate.state_machine.command", tracing::field::debug(cmd));
    }

    span
}
