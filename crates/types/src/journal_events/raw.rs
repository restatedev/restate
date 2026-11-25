// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::errors::GenericError;
use crate::journal_events::{Event, EventType};
use anyhow::Context;
use bytes::Bytes;
use prost::Message;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawEvent {
    ty: EventType,
    value: Bytes,
}

impl RawEvent {
    pub fn new(ty: EventType, value: Bytes) -> Self {
        RawEvent { ty, value }
    }

    pub fn unknown() -> Self {
        RawEvent {
            ty: EventType::Unknown,
            value: Bytes::default(),
        }
    }

    pub fn ty(&self) -> EventType {
        self.ty
    }

    pub fn into_inner(self) -> (EventType, Bytes) {
        (self.ty, self.value)
    }

    pub fn into_event_or_unknown(self) -> Event {
        Event::try_from(self).unwrap_or(Event::Unknown)
    }
}

// The conversion RawEvent <-> Event is defined at this level directly.

impl TryFrom<RawEvent> for Event {
    type Error = GenericError;

    fn try_from(value: RawEvent) -> Result<Self, Self::Error> {
        decode(value.ty, value.value)
            .context("error when decoding event")
            .map_err(Into::into)
    }
}

impl From<Event> for RawEvent {
    fn from(value: Event) -> Self {
        encode(value)
    }
}

#[derive(Debug, thiserror::Error)]
enum EventDecodingError {
    #[error("decoding error: {0:?}")]
    Protobuf(#[from] prost::DecodeError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

fn decode(ty: EventType, value: Bytes) -> Result<Event, EventDecodingError> {
    match ty {
        EventType::TransientError => Ok(Event::TransientError(
            pb::TransientErrorEvent::decode(value)?.try_into()?,
        )),
        EventType::Paused => Ok(Event::Paused(pb::PausedEvent::decode(value)?.try_into()?)),
        EventType::Suspended => Ok(Event::Suspended(
            pb::SuspendedEvent::decode(value)?.try_into()?,
        )),
        EventType::AttemptStarted => Ok(Event::AttemptStarted(
            pb::AttemptStartedEvent::decode(value)?.try_into()?,
        )),
        EventType::AttemptEnded => Ok(Event::AttemptEnded(
            pb::AttemptEndedEvent::decode(value)?.try_into()?,
        )),
        EventType::ActionRecorded => Ok(Event::ActionRecorded(
            pb::ActionRecordedEvent::decode(value)?.try_into()?,
        )),
        EventType::SpanStarted => Ok(Event::SpanStarted(
            pb::SpanStartedEvent::decode(value)?.into(),
        )),
        EventType::SpanEnded => Ok(Event::SpanEnded(
            pb::SpanEndedEvent::decode(value)?.try_into()?,
        )),
        EventType::ResultDelivered => Ok(Event::ResultDelivered(
            pb::ResultDeliveredEvent::decode(value)?.try_into()?,
        )),
        EventType::Unknown => Ok(Event::Unknown),
    }
}

fn encode(event: Event) -> RawEvent {
    match event {
        Event::TransientError(e) => RawEvent::new(
            EventType::TransientError,
            pb::TransientErrorEvent::from(e).encode_to_vec().into(),
        ),
        Event::Paused(e) => RawEvent::new(
            EventType::Paused,
            pb::PausedEvent::from(e).encode_to_vec().into(),
        ),
        Event::Suspended(e) => RawEvent::new(
            EventType::Suspended,
            pb::SuspendedEvent::from(e).encode_to_vec().into(),
        ),
        Event::AttemptStarted(e) => RawEvent::new(
            EventType::AttemptStarted,
            pb::AttemptStartedEvent::from(e).encode_to_vec().into(),
        ),
        Event::AttemptEnded(e) => RawEvent::new(
            EventType::AttemptEnded,
            pb::AttemptEndedEvent::from(e).encode_to_vec().into(),
        ),
        Event::ActionRecorded(e) => RawEvent::new(
            EventType::ActionRecorded,
            pb::ActionRecordedEvent::from(e).encode_to_vec().into(),
        ),
        Event::SpanStarted(e) => RawEvent::new(
            EventType::SpanStarted,
            pb::SpanStartedEvent::from(e).encode_to_vec().into(),
        ),
        Event::SpanEnded(e) => RawEvent::new(
            EventType::SpanEnded,
            pb::SpanEndedEvent::from(e).encode_to_vec().into(),
        ),
        Event::ResultDelivered(e) => RawEvent::new(
            EventType::ResultDelivered,
            pb::ResultDeliveredEvent::from(e).encode_to_vec().into(),
        ),
        Event::Unknown => RawEvent::unknown(),
    }
}

mod pb {
    use crate::errors::InvocationErrorCode;
    use crate::journal_events as event;
    use crate::journal_v2;
    use crate::journal_v2::{NotificationId, SignalId, UnresolvedFuture};
    use anyhow::Context;

    include!(concat!(env!("OUT_DIR"), "/restate.journal.events.rs"));

    impl From<event::TransientErrorEvent> for TransientErrorEvent {
        fn from(
            event::TransientErrorEvent {
                error_code,
                error_message,
                error_stacktrace,
                restate_doc_error_code,
                related_command_index,
                related_command_name,
                related_command_type,
                lsn,
            }: event::TransientErrorEvent,
        ) -> Self {
            TransientErrorEvent {
                error_code: error_code.into(),
                error_message,
                error_stacktrace,
                restate_doc_error_code,
                related_command_index,
                related_command_name,
                related_command_type: related_command_type
                    .map(|ct| transient_error_event::CommandType::from(ct).into()),
                lsn,
            }
        }
    }

    impl TryFrom<TransientErrorEvent> for event::TransientErrorEvent {
        type Error = anyhow::Error;

        fn try_from(
            TransientErrorEvent {
                error_code,
                error_message,
                error_stacktrace,
                restate_doc_error_code,
                related_command_index,
                related_command_name,
                related_command_type,
                lsn,
            }: TransientErrorEvent,
        ) -> Result<Self, Self::Error> {
            Ok(event::TransientErrorEvent {
                error_code: InvocationErrorCode::new(error_code as u16),
                error_message,
                error_stacktrace,
                restate_doc_error_code,
                related_command_index,
                related_command_name,
                related_command_type: related_command_type
                    .map(|ct| {
                        transient_error_event::CommandType::try_from(ct)
                            .context("Unrecognized command type")
                            .map(Into::into)
                    })
                    .transpose()?,
                lsn,
            })
        }
    }

    impl From<journal_v2::CommandType> for transient_error_event::CommandType {
        fn from(value: journal_v2::CommandType) -> Self {
            match value {
                journal_v2::CommandType::Input => Self::Input,
                journal_v2::CommandType::Output => Self::Output,
                journal_v2::CommandType::GetLazyState => Self::GetLazyState,
                journal_v2::CommandType::SetState => Self::SetState,
                journal_v2::CommandType::ClearState => Self::ClearState,
                journal_v2::CommandType::ClearAllState => Self::ClearAllState,
                journal_v2::CommandType::GetLazyStateKeys => Self::GetLazyStateKeys,
                journal_v2::CommandType::GetEagerState => Self::GetEagerState,
                journal_v2::CommandType::GetEagerStateKeys => Self::GetEagerStateKeys,
                journal_v2::CommandType::GetPromise => Self::GetPromise,
                journal_v2::CommandType::PeekPromise => Self::PeekPromise,
                journal_v2::CommandType::CompletePromise => Self::CompletePromise,
                journal_v2::CommandType::Sleep => Self::Sleep,
                journal_v2::CommandType::Call => Self::Call,
                journal_v2::CommandType::OneWayCall => Self::OneWayCall,
                journal_v2::CommandType::SendSignal => Self::SendSignal,
                journal_v2::CommandType::Run => Self::Run,
                journal_v2::CommandType::AttachInvocation => Self::AttachInvocation,
                journal_v2::CommandType::GetInvocationOutput => Self::GetInvocationOutput,
                journal_v2::CommandType::CompleteAwakeable => Self::CompleteAwakeable,
            }
        }
    }

    impl From<transient_error_event::CommandType> for journal_v2::CommandType {
        fn from(value: transient_error_event::CommandType) -> Self {
            match value {
                transient_error_event::CommandType::Input => Self::Input,
                transient_error_event::CommandType::Output => Self::Output,
                transient_error_event::CommandType::GetLazyState => Self::GetLazyState,
                transient_error_event::CommandType::SetState => Self::SetState,
                transient_error_event::CommandType::ClearState => Self::ClearState,
                transient_error_event::CommandType::ClearAllState => Self::ClearAllState,
                transient_error_event::CommandType::GetLazyStateKeys => Self::GetLazyStateKeys,
                transient_error_event::CommandType::GetEagerState => Self::GetEagerState,
                transient_error_event::CommandType::GetEagerStateKeys => Self::GetEagerStateKeys,
                transient_error_event::CommandType::GetPromise => Self::GetPromise,
                transient_error_event::CommandType::PeekPromise => Self::PeekPromise,
                transient_error_event::CommandType::CompletePromise => Self::CompletePromise,
                transient_error_event::CommandType::Sleep => Self::Sleep,
                transient_error_event::CommandType::Call => Self::Call,
                transient_error_event::CommandType::OneWayCall => Self::OneWayCall,
                transient_error_event::CommandType::SendSignal => Self::SendSignal,
                transient_error_event::CommandType::Run => Self::Run,
                transient_error_event::CommandType::AttachInvocation => Self::AttachInvocation,
                transient_error_event::CommandType::GetInvocationOutput => {
                    Self::GetInvocationOutput
                }
                transient_error_event::CommandType::CompleteAwakeable => Self::CompleteAwakeable,
            }
        }
    }

    impl From<event::PausedEvent> for PausedEvent {
        fn from(event::PausedEvent { last_failure, lsn }: event::PausedEvent) -> Self {
            PausedEvent {
                last_failure: last_failure.map(Into::into),
                lsn,
            }
        }
    }

    impl TryFrom<PausedEvent> for event::PausedEvent {
        type Error = anyhow::Error;

        fn try_from(PausedEvent { last_failure, lsn }: PausedEvent) -> Result<Self, Self::Error> {
            Ok(event::PausedEvent {
                last_failure: last_failure.map(|f| f.try_into()).transpose()?,
                lsn,
            })
        }
    }

    impl From<event::SuspendedEvent> for SuspendedEvent {
        fn from(event::SuspendedEvent { awaiting_on }: event::SuspendedEvent) -> Self {
            SuspendedEvent {
                awaiting_on: Some(awaiting_on.into()),
            }
        }
    }

    impl TryFrom<SuspendedEvent> for event::SuspendedEvent {
        type Error = anyhow::Error;

        fn try_from(SuspendedEvent { awaiting_on }: SuspendedEvent) -> Result<Self, Self::Error> {
            let awaiting_on = awaiting_on
                .context("missing awaiting_on")?
                .try_into()
                .context("invalid awaiting_on")?;
            Ok(event::SuspendedEvent { awaiting_on })
        }
    }

    impl From<journal_v2::CombinatorType> for future::CombinatorType {
        fn from(value: journal_v2::CombinatorType) -> Self {
            match value {
                journal_v2::CombinatorType::Unknown => Self::Unknown,
                journal_v2::CombinatorType::FirstCompleted => Self::FirstCompleted,
                journal_v2::CombinatorType::AllCompleted => Self::AllCompleted,
                journal_v2::CombinatorType::FirstSucceededOrAllFailed => {
                    Self::FirstSucceededOrAllFailed
                }
                journal_v2::CombinatorType::AllSucceededOrFirstFailed => {
                    Self::AllSucceededOrFirstFailed
                }
            }
        }
    }

    impl From<future::CombinatorType> for journal_v2::CombinatorType {
        fn from(value: future::CombinatorType) -> Self {
            match value {
                future::CombinatorType::Unknown => Self::Unknown,
                future::CombinatorType::FirstCompleted => Self::FirstCompleted,
                future::CombinatorType::AllCompleted => Self::AllCompleted,
                future::CombinatorType::FirstSucceededOrAllFailed => {
                    Self::FirstSucceededOrAllFailed
                }
                future::CombinatorType::AllSucceededOrFirstFailed => {
                    Self::AllSucceededOrFirstFailed
                }
            }
        }
    }

    impl From<UnresolvedFuture> for Future {
        fn from(value: UnresolvedFuture) -> Self {
            let (combinator, notifications, nested) = value.split();

            let mut f = Self {
                combinator_type: future::CombinatorType::from(combinator).into(),
                nested_futures: nested.into_iter().map(Into::into).collect(),
                ..Default::default()
            };

            for notif in notifications {
                match notif {
                    NotificationId::CompletionId(v) => f.waiting_completions.push(v),
                    NotificationId::SignalIndex(v) => f.waiting_signals.push(v),
                    NotificationId::SignalName(v) => f.waiting_named_signals.push(v.into()),
                }
            }

            f
        }
    }

    impl TryFrom<Future> for UnresolvedFuture {
        type Error = anyhow::Error;

        fn try_from(value: Future) -> Result<Self, Self::Error> {
            let Future {
                combinator_type,
                nested_futures,
                waiting_completions,
                waiting_named_signals,
                waiting_signals,
            } = value;

            let combinator: journal_v2::CombinatorType =
                future::CombinatorType::try_from(combinator_type)
                    .context("Unrecognized combinator type")?
                    .into();

            let notifications = waiting_completions
                .into_iter()
                .map(NotificationId::for_completion)
                .chain(
                    waiting_signals
                        .into_iter()
                        .map(SignalId::for_index)
                        .map(NotificationId::for_signal),
                )
                .chain(
                    waiting_named_signals
                        .into_iter()
                        .map(|s| SignalId::for_name(s.into()))
                        .map(NotificationId::for_signal),
                );

            let mut builder = UnresolvedFuture::builder(combinator).futures(notifications);

            for fut in nested_futures {
                builder = builder.future(UnresolvedFuture::try_from(fut)?);
            }

            builder.build()
        }
    }

    // -- The attempt record. Enumerations are stored as their names.

    fn parse<T: std::str::FromStr>(name: &str, what: &'static str) -> anyhow::Result<T> {
        name.parse()
            .map_err(|_| anyhow::anyhow!("unrecognized {what} '{name}'"))
    }

    fn parse_opt<T: std::str::FromStr>(
        name: Option<String>,
        what: &'static str,
    ) -> anyhow::Result<Option<T>> {
        name.map(|n| parse(&n, what)).transpose()
    }

    impl From<event::AttemptStartedEvent> for AttemptStartedEvent {
        fn from(e: event::AttemptStartedEvent) -> Self {
            AttemptStartedEvent {
                attempt: e.attempt,
                deployment_id: e.deployment_id,
                protocol_version: e.protocol_version,
                cause: e.cause.map(|c| c.to_string()),
                woken_by: e.woken_by.map(|w| WokenBy {
                    kind: w.kind.to_string(),
                    result_id: w.result_id,
                    signal_name: w.signal_name,
                }),
                lsn: e.lsn,
                scheduled_at: e.scheduled_at,
            }
        }
    }

    impl TryFrom<AttemptStartedEvent> for event::AttemptStartedEvent {
        type Error = anyhow::Error;

        fn try_from(e: AttemptStartedEvent) -> Result<Self, Self::Error> {
            Ok(event::AttemptStartedEvent {
                attempt: e.attempt,
                deployment_id: e.deployment_id,
                protocol_version: e.protocol_version,
                cause: parse_opt(e.cause, "attempt cause")?,
                woken_by: e
                    .woken_by
                    .map(|w| {
                        Ok::<_, anyhow::Error>(event::WokenBy {
                            kind: parse(&w.kind, "wake-up kind")?,
                            result_id: w.result_id,
                            signal_name: w.signal_name,
                        })
                    })
                    .transpose()?,
                lsn: e.lsn,
                scheduled_at: e.scheduled_at,
            })
        }
    }

    impl From<event::EventFailure> for EventFailure {
        fn from(f: event::EventFailure) -> Self {
            EventFailure {
                code: f.code,
                message: f.message,
            }
        }
    }

    impl From<EventFailure> for event::EventFailure {
        fn from(f: EventFailure) -> Self {
            event::EventFailure {
                code: f.code,
                message: f.message,
            }
        }
    }

    impl From<event::AttemptEndedEvent> for AttemptEndedEvent {
        fn from(e: event::AttemptEndedEvent) -> Self {
            AttemptEndedEvent {
                attempt: e.attempt,
                reason: e.reason,
                outcome: e.outcome.map(|o| o.to_string()),
                rule: e.rule,
                failure: e.failure.map(Into::into),
                outstanding: e
                    .outstanding
                    .into_iter()
                    .map(|o| OutstandingResult {
                        result_id: o.result_id,
                        kind: o.kind.to_string(),
                        label: o.label,
                    })
                    .collect(),
                lsn: e.lsn,
                retry_at: e.retry_at,
            }
        }
    }

    impl TryFrom<AttemptEndedEvent> for event::AttemptEndedEvent {
        type Error = anyhow::Error;

        fn try_from(e: AttemptEndedEvent) -> Result<Self, Self::Error> {
            Ok(event::AttemptEndedEvent {
                attempt: e.attempt,
                reason: e.reason,
                outcome: parse_opt(e.outcome, "attempt outcome")?,
                rule: e.rule,
                failure: e.failure.map(Into::into),
                outstanding: e
                    .outstanding
                    .into_iter()
                    .map(|o| {
                        Ok(event::OutstandingResult {
                            result_id: o.result_id,
                            kind: parse(&o.kind, "outstanding kind")?,
                            label: o.label,
                        })
                    })
                    .collect::<anyhow::Result<_>>()?,
                lsn: e.lsn,
                retry_at: e.retry_at,
            })
        }
    }

    impl From<event::RecordedAction> for RecordedAction {
        fn from(a: event::RecordedAction) -> Self {
            let s = a.summary;
            RecordedAction {
                kind: a.kind,
                name: a.name,
                parent_span_id: a.parent_span_id,
                result_id: a.result_id,
                invocation_id_result_id: a.invocation_id_result_id,
                level: a.level.to_string(),
                summary: Some(ActionSummary {
                    scope: s.scope.map(|s| s.to_string()),
                    key: s.key,
                    target: s.target.map(|t| RecordedTarget {
                        service: t.service,
                        handler: t.handler,
                        key: t.key,
                    }),
                    invocation_id: s.invocation_id,
                    signal_name: s.signal_name,
                    signal_index: s.signal_index,
                    bytes: s.bytes,
                    span_id: s.span_id,
                    outcome: s.outcome.map(|o| o.to_string()),
                    failure: s.failure.map(Into::into),
                }),
                payload: a.payload,
                members: a.members.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl TryFrom<RecordedAction> for event::RecordedAction {
        type Error = anyhow::Error;

        fn try_from(a: RecordedAction) -> Result<Self, Self::Error> {
            let s = a.summary.unwrap_or_default();
            Ok(event::RecordedAction {
                kind: a.kind,
                name: a.name,
                parent_span_id: a.parent_span_id,
                result_id: a.result_id,
                invocation_id_result_id: a.invocation_id_result_id,
                level: parse(&a.level, "record level")?,
                summary: event::ActionSummary {
                    scope: parse_opt(s.scope, "state scope")?,
                    key: s.key,
                    target: s.target.map(|t| event::RecordedTarget {
                        service: t.service,
                        handler: t.handler,
                        key: t.key,
                    }),
                    invocation_id: s.invocation_id,
                    signal_name: s.signal_name,
                    signal_index: s.signal_index,
                    bytes: s.bytes,
                    span_id: s.span_id,
                    outcome: parse_opt(s.outcome, "span outcome")?,
                    failure: s.failure.map(Into::into),
                },
                payload: a.payload,
                members: a
                    .members
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<anyhow::Result<_>>()?,
            })
        }
    }

    impl From<event::ActionRecordedEvent> for ActionRecordedEvent {
        fn from(e: event::ActionRecordedEvent) -> Self {
            ActionRecordedEvent {
                attempt: e.attempt,
                action: Some((*e.action).into()),
                lsn: e.lsn,
            }
        }
    }

    impl TryFrom<ActionRecordedEvent> for event::ActionRecordedEvent {
        type Error = anyhow::Error;

        fn try_from(e: ActionRecordedEvent) -> Result<Self, Self::Error> {
            Ok(event::ActionRecordedEvent {
                attempt: e.attempt,
                action: Box::new(e.action.context("missing action")?.try_into()?),
                lsn: e.lsn,
            })
        }
    }

    impl From<event::SpanStartedEvent> for SpanStartedEvent {
        fn from(e: event::SpanStartedEvent) -> Self {
            SpanStartedEvent {
                attempt: e.attempt,
                span_id: e.span_id,
                parent_span_id: e.parent_span_id,
                name: e.name,
                lsn: e.lsn,
            }
        }
    }

    impl From<SpanStartedEvent> for event::SpanStartedEvent {
        fn from(e: SpanStartedEvent) -> Self {
            event::SpanStartedEvent {
                attempt: e.attempt,
                span_id: e.span_id,
                parent_span_id: e.parent_span_id,
                name: e.name,
                lsn: e.lsn,
            }
        }
    }

    impl From<event::SpanEndedEvent> for SpanEndedEvent {
        fn from(e: event::SpanEndedEvent) -> Self {
            SpanEndedEvent {
                attempt: e.attempt,
                span_id: e.span_id,
                outcome: e.outcome.to_string(),
                value: e.value,
                failure: e.failure.map(Into::into),
                lsn: e.lsn,
            }
        }
    }

    impl TryFrom<SpanEndedEvent> for event::SpanEndedEvent {
        type Error = anyhow::Error;

        fn try_from(e: SpanEndedEvent) -> Result<Self, Self::Error> {
            Ok(event::SpanEndedEvent {
                attempt: e.attempt,
                span_id: e.span_id,
                outcome: parse(&e.outcome, "span outcome")?,
                value: e.value,
                failure: e.failure.map(Into::into),
                lsn: e.lsn,
            })
        }
    }

    impl From<event::ResultDeliveredEvent> for ResultDeliveredEvent {
        fn from(e: event::ResultDeliveredEvent) -> Self {
            ResultDeliveredEvent {
                attempt: e.attempt,
                result_id: e.result_id,
                variant: e.variant.to_string(),
                lsn: e.lsn,
            }
        }
    }

    impl TryFrom<ResultDeliveredEvent> for event::ResultDeliveredEvent {
        type Error = anyhow::Error;

        fn try_from(e: ResultDeliveredEvent) -> Result<Self, Self::Error> {
            Ok(event::ResultDeliveredEvent {
                attempt: e.attempt,
                result_id: e.result_id,
                variant: parse(&e.variant, "result variant")?,
                lsn: e.lsn,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use crate::journal_events::{
        ActionRecordedEvent, ActionSummary, AttemptStartedEvent, RECORD_PAYLOAD_LIMIT, RecordLevel,
        RecordedAction, RecordedTarget, record_payload,
    };

    /// The attempt record as `sys_journal_events.event_json` shows it, back from JSON (the CLI)
    /// and through storage; an event written before the record's fields existed still decodes.
    #[test]
    fn attempt_record_json_and_storage() {
        let event = Event::ActionRecorded(ActionRecordedEvent {
            attempt: 1,
            action: Box::new(RecordedAction {
                kind: "Call".to_owned(),
                name: "ask".to_owned(),
                parent_span_id: None,
                result_id: Some(9),
                invocation_id_result_id: Some(8),
                level: RecordLevel::Full,
                summary: ActionSummary {
                    target: Some(RecordedTarget {
                        service: "S".to_owned(),
                        handler: "h".to_owned(),
                        key: None,
                    }),
                    bytes: Some(2),
                    ..Default::default()
                },
                payload: Some("hi".to_owned()),
                members: vec![],
            }),
            lsn: Some(4),
        });
        let value = serde_json::to_value(&event).unwrap();
        assert_eq!(
            value,
            json!({
                "ty": "ActionRecorded", "attempt": 1, "kind": "Call", "name": "ask",
                "result_id": 9, "invocation_id_result_id": 8, "level": "full",
                "summary": {"target": {"service": "S", "handler": "h"}, "bytes": 2},
                "payload": "hi", "lsn": 4
            })
        );
        assert_eq!(serde_json::from_value::<Event>(value).unwrap(), event);
        assert_eq!(RawEvent::from(event.clone()).into_event_or_unknown(), event);

        let old = pb::AttemptStartedEvent {
            attempt: 2,
            deployment_id: "dp".to_owned(),
            protocol_version: 1,
            ..Default::default()
        };
        assert_eq!(
            RawEvent::new(EventType::AttemptStarted, old.encode_to_vec().into())
                .into_event_or_unknown(),
            Event::AttemptStarted(AttemptStartedEvent {
                attempt: 2,
                deployment_id: "dp".to_owned(),
                protocol_version: 1,
                cause: None,
                woken_by: None,
                lsn: None,
                scheduled_at: None,
            })
        );

        // Payloads are cut at the limit, on a character boundary.
        let payload = record_payload("é".repeat(RECORD_PAYLOAD_LIMIT).as_bytes());
        assert_eq!(payload.len(), RECORD_PAYLOAD_LIMIT);
        assert!(payload.chars().all(|c| c == 'é'));
    }
}
