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

pub(super) fn state_machine_apply_command_span(is_leader: bool, cmd: &Command) -> Span {
    let span = if is_leader {
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
