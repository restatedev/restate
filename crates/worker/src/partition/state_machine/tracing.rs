// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::StateMachineContext;

use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationTarget;
use restate_wal_protocol::Command;
use tracing::{debug_span, event_enabled, trace_span, Level, Span};

pub(super) trait StateMachineSpanExt {
    fn record_invocation_id(invocation_id: &InvocationId);

    fn record_invocation_target(invocation_target: &InvocationTarget);
}

impl StateMachineSpanExt for Span {
    fn record_invocation_id(invocation_id: &InvocationId) {
        Span::current().record(
            "restate.invocation.id",
            tracing::field::display(invocation_id),
        );
    }

    fn record_invocation_target(invocation_target: &InvocationTarget) {
        Span::current()
            .record(
                "rpc.service",
                tracing::field::display(invocation_target.service_name()),
            )
            .record(
                "rpc.method",
                tracing::field::display(invocation_target.handler_name()),
            )
            .record(
                "restate.invocation.target",
                tracing::field::display(invocation_target),
            );
    }
}

pub(super) fn state_machine_apply_command_span(
    state_machine_context: &StateMachineContext,
    cmd: &Command,
) -> Span {
    let span = if state_machine_context.is_leader {
        debug_span!(
            "apply_command",
            restate.invocation.id = tracing::field::Empty,
            restate.invocation.target = tracing::field::Empty,
            rpc.service = tracing::field::Empty,
            rpc.method = tracing::field::Empty,
            restate.state_machine.command = tracing::field::Empty,
        )
    } else {
        trace_span!(
            "apply_command",
            restate.invocation.id = tracing::field::Empty,
            restate.invocation.target = tracing::field::Empty,
            rpc.service = tracing::field::Empty,
            rpc.method = tracing::field::Empty,
            restate.state_machine.command = tracing::field::Empty,
        )
    };
    if event_enabled!(Level::TRACE) {
        span.record("restate.state_machine.command", tracing::field::debug(cmd));
    }

    span
}
