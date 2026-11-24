// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Validates an action message and turns it into what the runner does with it: an action to
//! propose with the results it owes, an answer at once, or a read.

use std::num::NonZeroU32;

use bytes::Bytes;
use bytestring::ByteString;

use restate_service_protocol_explicit_recovery::codec::Message;
use restate_service_protocol_explicit_recovery::proto::{
    self, StateKeys, Value, Void, atomic_group_action_message::action::Action as GroupMember,
    attach_invocation_action_message::Target as AttachTarget,
    result_notification_message::Result as NotificationResult,
};
use restate_types::Scope;
use restate_types::errors::{InvocationError, codes};
use restate_types::identifiers::{IdempotencyId, InvocationId, ServiceId};
use restate_types::invocation::{
    Header, InvocationQuery, InvocationTarget, InvocationTargetType, ResponseResult,
    ServiceInvocationSpanContext, SpanRelation,
};
use restate_types::journal_events::{
    ActionSummary, EventFailure, OutstandingKind, RecordLevel, RecordedAction, RecordedScope,
    RecordedTarget, SpanOutcome, record_payload,
};
use restate_types::journal_v2::{CallRequest, Failure, FailureMetadata, SignalResult};
use restate_types::limit_key::LimitKey;
use restate_types::schema::invocation_target::{DeploymentStatus, InvocationTargetResolver};
use restate_types::time::MillisSinceEpoch;
use restate_util_string::ReString;
use restate_worker_api::invoker::explicit_recovery::{ErAction, ErRecord, ErStateScope};

use super::book::{AttemptBook, Violation};

/// What the action is about: the invocation that runs it.
pub(crate) struct ActionContext<'a> {
    pub invocation_id: InvocationId,
    pub invocation_target: &'a InvocationTarget,
    pub span_context: &'a ServiceInvocationSpanContext,
}

/// A read: served once every action sent before it is applied (G6, G8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Read {
    GetState {
        scope: ErStateScope,
        key: Bytes,
        result_id: u32,
    },
    GetStateKeys {
        scope: ErStateScope,
        result_id: u32,
    },
    Signal {
        name: ByteString,
        index: u32,
        result_id: u32,
    },
    /// `Flush`, and the spans: a void result, if wanted, once everything before is applied.
    Barrier {
        result_id: u32,
    },
}

#[derive(Debug)]
pub(crate) enum Classified {
    /// Proposed as one effect. `internal` are sent once it is applied; `external` arrive later.
    Propose {
        action: ErAction,
        highest_result_id: Option<NonZeroU32>,
        internal: Vec<(u32, NotificationResult)>,
        external: Vec<u32>,
        record: Option<Box<RecordedAction>>,
    },
    /// A recorded span: proposed for the record only. `internal` are sent once it is applied.
    Record {
        record: ErRecord,
        internal: Vec<(u32, NotificationResult)>,
    },
    /// Answered at once, without proposing anything.
    Answer(Vec<(u32, NotificationResult)>),
    /// Served by the attempt. A recorded read is proposed for the record first.
    Read {
        read: Read,
        record: Option<Box<RecordedAction>>,
    },
}

/// The display fields every action has (`ActionTemplate`).
struct Display {
    name: String,
    parent_span_id: u32,
    record: i32,
}

impl Display {
    /// How much of the action is recorded. `RECORD_DEFAULT` records a summary, except of reads
    /// and `Flush` (`recorded_by_default` false).
    fn level(&self, recorded_by_default: bool) -> Option<RecordLevel> {
        match proto::RecordLevel::try_from(self.record).unwrap_or(proto::RecordLevel::RecordDefault)
        {
            proto::RecordLevel::RecordNone => None,
            proto::RecordLevel::RecordSummary => Some(RecordLevel::Summary),
            proto::RecordLevel::RecordFull => Some(RecordLevel::Full),
            proto::RecordLevel::RecordDefault => {
                recorded_by_default.then_some(RecordLevel::Summary)
            }
        }
    }

    /// The handler asked for nothing to be kept of the action.
    fn is_none(&self) -> bool {
        self.record == proto::RecordLevel::RecordNone as i32
    }

    /// The record of the action, if any. The payload is kept only at [`RecordLevel::Full`].
    fn record(
        &self,
        kind: &str,
        recorded_by_default: bool,
        summary: ActionSummary,
        payload: Option<&[u8]>,
    ) -> Option<RecordedAction> {
        let level = self.level(recorded_by_default)?;
        Some(RecordedAction {
            kind: kind.to_owned(),
            name: self.name.clone(),
            parent_span_id: NonZeroU32::new(self.parent_span_id).map(NonZeroU32::get),
            result_id: None,
            invocation_id_result_id: None,
            level,
            summary,
            payload: payload
                .filter(|_| level == RecordLevel::Full)
                .map(record_payload),
            members: vec![],
        })
    }

    /// The label of a result the attempt may wait for: the name, else `default`.
    fn label(&self, default: impl FnOnce() -> String) -> String {
        if self.name.is_empty() {
            default()
        } else {
            self.name.clone()
        }
    }
}

macro_rules! display {
    ($m:expr) => {
        Display {
            name: std::mem::take(&mut $m.name),
            parent_span_id: $m.parent_span_id,
            record: $m.record,
        }
    };
}

fn with_result_id(mut record: RecordedAction, result_id: u32) -> RecordedAction {
    record.result_id = NonZeroU32::new(result_id).map(NonZeroU32::get);
    record
}

fn recorded_scope(scope: ErStateScope) -> RecordedScope {
    match scope {
        ErStateScope::Object => RecordedScope::Object,
        ErStateScope::Invocation => RecordedScope::Invocation,
    }
}

fn state_summary(scope: ErStateScope, key: Option<&[u8]>) -> ActionSummary {
    ActionSummary {
        scope: Some(recorded_scope(scope)),
        key: key.map(|k| String::from_utf8_lossy(k).into_owned()),
        ..Default::default()
    }
}

fn event_failure(failure: &proto::Failure) -> EventFailure {
    EventFailure {
        code: failure.code,
        message: failure.message.clone(),
    }
}

/// A span inside an atomic group, as a member of the group's record.
fn span_member(record: ErRecord, display: &Display, level: RecordLevel) -> RecordedAction {
    let (kind, summary, payload) = match record {
        ErRecord::SpanStart { span_id, .. } => (
            "SpanStart",
            ActionSummary {
                span_id: Some(span_id),
                ..Default::default()
            },
            None,
        ),
        ErRecord::SpanEnd {
            span_id,
            outcome,
            value,
            failure,
        } => (
            "SpanEnd",
            ActionSummary {
                span_id: Some(span_id),
                outcome: Some(outcome),
                failure,
                ..Default::default()
            },
            value,
        ),
        ErRecord::Action(_) | ErRecord::ResultDelivered { .. } => {
            unreachable!("only spans are recorded this way")
        }
    };
    RecordedAction {
        kind: kind.to_owned(),
        name: display.name.clone(),
        parent_span_id: NonZeroU32::new(display.parent_span_id).map(NonZeroU32::get),
        result_id: None,
        invocation_id_result_id: None,
        level,
        summary,
        payload,
        members: vec![],
    }
}

pub(crate) fn void() -> NotificationResult {
    NotificationResult::Void(Void {})
}

pub(crate) fn value(content: Bytes) -> NotificationResult {
    NotificationResult::Value(Value { content })
}

pub(crate) fn from_response_result(result: ResponseResult) -> NotificationResult {
    match result {
        ResponseResult::Success(content) => value(content),
        ResponseResult::Failure(err) => NotificationResult::Failure(proto::Failure {
            code: u32::from(u16::from(err.code())),
            message: err.message().to_owned(),
            metadata: err
                .metadata()
                .iter()
                .map(|(key, value)| proto::FailureMetadata {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }),
    }
}

pub(crate) fn from_signal_result(result: SignalResult) -> NotificationResult {
    match result {
        SignalResult::Void => void(),
        SignalResult::Success(content) => value(content),
        SignalResult::Failure(failure) => NotificationResult::Failure(proto::Failure {
            code: u32::from(u16::from(failure.code)),
            message: failure.message.to_string(),
            metadata: failure
                .metadata
                .into_iter()
                .map(|m| proto::FailureMetadata {
                    key: m.key.to_string(),
                    value: m.value.to_string(),
                })
                .collect(),
        }),
    }
}

pub(crate) fn to_invocation_error(failure: proto::Failure) -> InvocationError {
    InvocationError::new(clamp_code(failure.code), failure.message).with_metadata_vec(
        failure
            .metadata
            .into_iter()
            .map(|m| (m.key, m.value))
            .collect(),
    )
}

fn clamp_code(code: u32) -> u16 {
    u16::try_from(code).unwrap_or(500)
}

fn to_signal_failure(failure: proto::Failure) -> Failure {
    Failure {
        code: clamp_code(failure.code).into(),
        message: failure.message.into(),
        metadata: failure
            .metadata
            .into_iter()
            .map(|m| FailureMetadata {
                key: m.key.into(),
                value: m.value.into(),
            })
            .collect(),
    }
}

fn content(value: Option<Value>) -> Bytes {
    value.map(|v| v.content).unwrap_or_default()
}

fn scope_of(scope: i32) -> Result<ErStateScope, Violation> {
    match proto::StateScope::try_from(scope) {
        Ok(proto::StateScope::Object) => Ok(ErStateScope::Object),
        Ok(proto::StateScope::Invocation) => Ok(ErStateScope::Invocation),
        _ => Err(Violation::new(format!(
            "state scope {scope} is not a scope"
        ))),
    }
}

fn parse_invocation_id(id: &str) -> Result<InvocationId, Violation> {
    id.parse()
        .map_err(|e| Violation::new(format!("malformed invocation id '{id}': {e}")))
}

fn required(result_id: u32, what: &str) -> Result<u32, Violation> {
    if result_id == 0 {
        return Err(Violation::new(format!("{what} without its result id")));
    }
    Ok(result_id)
}

impl ActionContext<'_> {
    fn check_object_scope(&self, scope: ErStateScope, write: bool) -> Result<(), Violation> {
        if scope != ErStateScope::Object {
            return Ok(());
        }
        let ty = self.invocation_target.invocation_target_ty();
        if self.invocation_target.key().is_none() {
            return Err(Violation::new(
                "object state used by a handler of a target without a key",
            ));
        }
        if write && !ty.can_write_state() {
            return Err(Violation::new("object state written by a shared handler"));
        }
        Ok(())
    }

    /// Classifies an action message. Checks its result ids in wire order, and the spans.
    pub(crate) fn classify(
        &self,
        message: Message,
        book: &mut AttemptBook,
        resolver: &impl InvocationTargetResolver,
    ) -> Result<Classified, Violation> {
        Ok(match message {
            Message::GetStateAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let scope = scope_of(m.scope)?;
                self.check_object_scope(scope, false)?;
                let result_id = required(m.result_id, "GetState")?;
                book.use_id(result_id)?;
                let record = display!(m)
                    .record("GetState", false, state_summary(scope, Some(&m.key)), None)
                    .map(|r| Box::new(with_result_id(r, result_id)));
                Classified::Read {
                    read: Read::GetState {
                        scope,
                        key: m.key,
                        result_id,
                    },
                    record,
                }
            }
            Message::GetStateKeysAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let scope = scope_of(m.scope)?;
                self.check_object_scope(scope, false)?;
                let result_id = required(m.result_id, "GetStateKeys")?;
                book.use_id(result_id)?;
                let record = display!(m)
                    .record("GetStateKeys", false, state_summary(scope, None), None)
                    .map(|r| Box::new(with_result_id(r, result_id)));
                Classified::Read {
                    read: Read::GetStateKeys { scope, result_id },
                    record,
                }
            }
            Message::ReadSignalAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let result_id = required(m.result_id, "ReadSignal")?;
                book.use_id(result_id)?;
                let display = display!(m);
                let record = display
                    .record(
                        "ReadSignal",
                        false,
                        ActionSummary {
                            signal_name: Some(m.signal_name.clone()),
                            signal_index: Some(m.signal_index),
                            ..Default::default()
                        },
                        None,
                    )
                    .map(|r| Box::new(with_result_id(r, result_id)));
                if !display.is_none() {
                    book.label(
                        result_id,
                        OutstandingKind::SignalRead,
                        format!("{}#{}", m.signal_name, m.signal_index),
                        record.is_some(),
                    );
                }
                Classified::Read {
                    read: Read::Signal {
                        name: m.signal_name.into(),
                        index: m.signal_index,
                        result_id,
                    },
                    record,
                }
            }
            Message::FlushAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let result_id = required(m.result_id, "Flush")?;
                book.use_id(result_id)?;
                let record = display!(m)
                    .record("Flush", false, ActionSummary::default(), None)
                    .map(|r| Box::new(with_result_id(r, result_id)));
                Classified::Read {
                    read: Read::Barrier { result_id },
                    record,
                }
            }
            Message::SpanStartAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                book.start_span(m.span_id)?;
                book.use_id(m.result_id)?;
                let display = display!(m);
                let record = ErRecord::SpanStart {
                    span_id: m.span_id,
                    parent_span_id: NonZeroU32::new(display.parent_span_id).map(NonZeroU32::get),
                    name: display.name,
                };
                span(record, display.record, m.result_id)
            }
            Message::SpanEndAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                book.end_span(m.span_id)?;
                book.use_id(m.result_id)?;
                let display = display!(m);
                let full = display.level(true) == Some(RecordLevel::Full);
                let (outcome, value, failure) = match &m.result {
                    None | Some(proto::span_end_action_message::Result::Void(_)) => {
                        (SpanOutcome::Ok, None, None)
                    }
                    Some(proto::span_end_action_message::Result::Value(v)) => (
                        SpanOutcome::Value,
                        full.then(|| record_payload(&v.content)),
                        None,
                    ),
                    Some(proto::span_end_action_message::Result::Failure(f)) => {
                        (SpanOutcome::Failure, None, Some(event_failure(f)))
                    }
                };
                let record = ErRecord::SpanEnd {
                    span_id: m.span_id,
                    outcome,
                    value,
                    failure,
                };
                span(record, display.record, m.result_id)
            }
            Message::AttachInvocationAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let result_id = required(m.result_id, "AttachInvocation")?;
                book.use_id(result_id)?;
                let display = display!(m);
                let query = attach_query(m.target)?;
                if query.to_invocation_id() == self.invocation_id {
                    // D55: an attach to itself fails at once.
                    return Ok(Classified::Answer(vec![(
                        result_id,
                        NotificationResult::Failure(proto::Failure {
                            code: u32::from(u16::from(codes::BAD_REQUEST)),
                            message: "an invocation cannot attach to itself".to_owned(),
                            metadata: vec![],
                        }),
                    )]));
                }
                let target_id = query.to_invocation_id().to_string();
                let record = display
                    .record(
                        "AttachInvocation",
                        true,
                        ActionSummary {
                            invocation_id: Some(target_id.clone()),
                            target: attach_target(&query),
                            ..Default::default()
                        },
                        None,
                    )
                    .map(|r| Box::new(with_result_id(r, result_id)));
                if !display.is_none() {
                    book.label(
                        result_id,
                        OutstandingKind::Attach,
                        display.label(|| target_id),
                        record.is_some(),
                    );
                }
                Classified::Propose {
                    action: ErAction::Attach {
                        query,
                        result_id: NonZeroU32::new(result_id).expect("checked"),
                    },
                    highest_result_id: NonZeroU32::new(result_id),
                    internal: vec![],
                    external: vec![result_id],
                    record,
                }
            }
            Message::AtomicGroupAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                if m.actions.is_empty() {
                    return Err(Violation::new("an atomic group with no member"));
                }
                let display = display!(m);
                let level = display.level(true);
                let mut members = Vec::with_capacity(m.actions.len());
                let mut recorded_members = vec![];
                let mut internal = vec![];
                let mut external = vec![];
                for member in m.actions {
                    let Some(member) = member.action else {
                        return Err(Violation::new("a member of a group with no action set"));
                    };
                    // A span's display fields, kept for its member record.
                    let span_display = match &member {
                        GroupMember::SpanStart(s) => Some(Display {
                            name: s.name.clone(),
                            parent_span_id: s.parent_span_id,
                            record: s.record,
                        }),
                        GroupMember::SpanEnd(s) => Some(Display {
                            name: s.name.clone(),
                            parent_span_id: s.parent_span_id,
                            record: s.record,
                        }),
                        _ => None,
                    };
                    let message = match member {
                        GroupMember::SetState(m) => Message::SetStateAction(m),
                        GroupMember::ClearState(m) => Message::ClearStateAction(m),
                        GroupMember::ClearAllState(m) => Message::ClearAllStateAction(m),
                        GroupMember::Call(m) => Message::CallAction(m),
                        GroupMember::CancelInvocation(m) => Message::CancelInvocationAction(m),
                        GroupMember::SendSignal(m) => Message::SendSignalAction(m),
                        GroupMember::SpanStart(m) => Message::SpanStartAction(m),
                        GroupMember::SpanEnd(m) => Message::SpanEndAction(m),
                    };
                    match self.classify(message, book, resolver)? {
                        Classified::Propose {
                            action,
                            internal: member_internal,
                            external: member_external,
                            record,
                            ..
                        } => {
                            members.push(action);
                            internal.extend(member_internal);
                            external.extend(member_external);
                            recorded_members.extend(record.map(|r| *r));
                        }
                        // A span in a group: its void result comes with the group's, and it is
                        // recorded as a member of the group (one event per record).
                        Classified::Record {
                            record,
                            internal: member_internal,
                        } => {
                            internal.extend(member_internal);
                            let display = span_display.expect("a span");
                            let member_level = display.level(true).expect("recorded");
                            recorded_members.push(span_member(record, &display, member_level));
                        }
                        Classified::Read {
                            read: Read::Barrier { result_id },
                            ..
                        } => {
                            if result_id != 0 {
                                internal.push((result_id, void()));
                            }
                        }
                        Classified::Answer(_) | Classified::Read { .. } => {
                            unreachable!("only the members a group may hold are classified")
                        }
                    }
                }
                book.use_id(m.result_id)?;
                if m.result_id != 0 {
                    internal.push((m.result_id, void()));
                }
                let record = level.map(|level| {
                    Box::new(RecordedAction {
                        kind: "AtomicGroup".to_owned(),
                        name: display.name,
                        parent_span_id: NonZeroU32::new(display.parent_span_id)
                            .map(NonZeroU32::get),
                        result_id: NonZeroU32::new(m.result_id).map(NonZeroU32::get),
                        invocation_id_result_id: None,
                        level,
                        summary: ActionSummary::default(),
                        payload: None,
                        members: recorded_members,
                    })
                });
                Classified::Propose {
                    action: ErAction::Group(members),
                    highest_result_id: NonZeroU32::new(book.last_result_id()),
                    internal,
                    external,
                    record,
                }
            }
            Message::SetStateAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let scope = scope_of(m.scope)?;
                self.check_object_scope(scope, true)?;
                book.use_id(m.result_id)?;
                let value = content(m.value.take());
                let record = display!(m).record(
                    "SetState",
                    true,
                    ActionSummary {
                        bytes: Some(value.len() as u64),
                        ..state_summary(scope, Some(&m.key))
                    },
                    Some(&value),
                );
                propose_void(
                    ErAction::SetState {
                        scope,
                        key: m.key,
                        value,
                    },
                    m.result_id,
                    record,
                )
            }
            Message::ClearStateAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let scope = scope_of(m.scope)?;
                self.check_object_scope(scope, true)?;
                book.use_id(m.result_id)?;
                let record = display!(m).record(
                    "ClearState",
                    true,
                    state_summary(scope, Some(&m.key)),
                    None,
                );
                propose_void(
                    ErAction::ClearState { scope, key: m.key },
                    m.result_id,
                    record,
                )
            }
            Message::ClearAllStateAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let scope = scope_of(m.scope)?;
                self.check_object_scope(scope, true)?;
                book.use_id(m.result_id)?;
                let record =
                    display!(m).record("ClearAllState", true, state_summary(scope, None), None);
                propose_void(ErAction::ClearAllState { scope }, m.result_id, record)
            }
            Message::CancelInvocationAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let target = parse_invocation_id(&m.invocation_id)?;
                book.use_id(m.result_id)?;
                let record = display!(m).record(
                    "CancelInvocation",
                    true,
                    ActionSummary {
                        invocation_id: Some(m.invocation_id),
                        ..Default::default()
                    },
                    None,
                );
                propose_void(ErAction::CancelInvocation { target }, m.result_id, record)
            }
            Message::SendSignalAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let target = parse_invocation_id(&m.target_invocation_id)?;
                book.use_id(m.result_id)?;
                let result = match m.result.take() {
                    None | Some(proto::send_signal_action_message::Result::Void(_)) => {
                        SignalResult::Void
                    }
                    Some(proto::send_signal_action_message::Result::Value(v)) => {
                        SignalResult::Success(v.content)
                    }
                    Some(proto::send_signal_action_message::Result::Failure(f)) => {
                        SignalResult::Failure(to_signal_failure(f))
                    }
                };
                let value = match &result {
                    SignalResult::Success(v) => Some(v.as_ref()),
                    _ => None,
                };
                let record = display!(m).record(
                    "SendSignal",
                    true,
                    ActionSummary {
                        invocation_id: Some(m.target_invocation_id),
                        signal_name: Some(m.signal_name.clone()),
                        bytes: value.map(|v| v.len() as u64),
                        ..Default::default()
                    },
                    value,
                );
                propose_void(
                    ErAction::SendSignal {
                        target,
                        name: m.signal_name.into(),
                        result,
                    },
                    m.result_id,
                    record,
                )
            }
            Message::CallAction(mut m) => {
                book.check_parent_span(m.parent_span_id)?;
                let id_result_id = required(m.invocation_id_result_id, "Call")?;
                book.use_id_pair(id_result_id, m.result_id)?;
                let result_id = NonZeroU32::new(m.result_id);
                let execution_time =
                    (m.invoke_time != 0).then(|| MillisSinceEpoch::new(m.invoke_time));
                let display = display!(m);
                let target = RecordedTarget {
                    service: m.service_name.clone(),
                    handler: m.handler_name.clone(),
                    key: (!m.key.is_empty()).then(|| m.key.clone()),
                };
                let (request, uninvokable) = self.resolve_call(resolver, m);
                let parameter_len = request.parameter.len() as u64;
                let record = display
                    .record(
                        "Call",
                        true,
                        ActionSummary {
                            target: Some(target.clone()),
                            invocation_id: Some(request.invocation_id.to_string()),
                            bytes: Some(parameter_len),
                            ..Default::default()
                        },
                        Some(&request.parameter),
                    )
                    .map(|mut r| {
                        r.result_id = result_id.map(NonZeroU32::get);
                        r.invocation_id_result_id = Some(id_result_id);
                        Box::new(r)
                    });
                let mut internal = vec![(
                    id_result_id,
                    NotificationResult::InvocationId(request.invocation_id.to_string()),
                )];
                let mut external = vec![];
                if let Some(result_id) = result_id {
                    match &uninvokable {
                        // D22: the target cannot be invoked: its result is a failure, right
                        // after the id.
                        Some(err) => internal.push((
                            result_id.get(),
                            from_response_result(ResponseResult::Failure(err.clone())),
                        )),
                        None => {
                            external.push(result_id.get());
                            if !display.is_none() {
                                book.label(
                                    result_id.get(),
                                    OutstandingKind::Call,
                                    display
                                        .label(|| format!("{}/{}", target.service, target.handler)),
                                    record.is_some(),
                                );
                            }
                        }
                    }
                }
                Classified::Propose {
                    action: ErAction::Call {
                        request: Box::new(request),
                        execution_time,
                        result_id,
                        uninvokable,
                    },
                    highest_result_id: NonZeroU32::new(id_result_id.max(m_result(result_id))),
                    internal,
                    external,
                    record,
                }
            }
            other => {
                return Err(Violation::new(format!(
                    "unexpected message {:?}",
                    other.ty()
                )));
            }
        })
    }

    /// Resolves the target of a call, as the durable execution protocol does. A target that
    /// cannot be invoked still gets an invocation id (D22, step 1).
    fn resolve_call(
        &self,
        resolver: &impl InvocationTargetResolver,
        m: proto::CallActionMessage,
    ) -> (CallRequest, Option<InvocationError>) {
        let headers: Vec<Header> = m
            .headers
            .into_iter()
            .map(|h| Header::new(h.key, h.value))
            .collect();
        let idempotency_key: Option<ByteString> = m.idempotency_key.map(Into::into);
        let service_name: ByteString = m.service_name.into();
        let handler_name: ByteString = m.handler_name.into();
        let key: ByteString = m.key.into();

        let resolved = (|| {
            let meta = resolver
                .resolve_latest_invocation_target(&service_name, &handler_name)
                .ok_or_else(|| {
                    InvocationError::new(
                        codes::NOT_FOUND,
                        format!("the service handler {service_name}/{handler_name} was not found"),
                    )
                })?;
            if let DeploymentStatus::Deprecated(dp_id) = meta.deployment_status {
                return Err(InvocationError::new(
                    codes::NOT_FOUND,
                    format!(
                        "the service {service_name} is exposed by the deprecated deployment {dp_id}"
                    ),
                ));
            }
            if !key.is_empty() && !meta.target_ty.is_keyed() {
                return Err(InvocationError::new(
                    codes::BAD_REQUEST,
                    format!("the service {service_name} has no key, but the call has one"),
                ));
            }
            let scope = match m.scope.as_deref() {
                Some(scope) if !scope.is_empty() => {
                    Some(Scope::try_non_interned(scope).map_err(|e| {
                        InvocationError::new(
                            codes::BAD_REQUEST,
                            format!("invalid scope '{scope}': {e}"),
                        )
                    })?)
                }
                _ => None,
            };
            let target = match meta.target_ty {
                InvocationTargetType::Service => {
                    InvocationTarget::service(service_name.clone(), handler_name.clone())
                }
                InvocationTargetType::VirtualObject(h_ty) => InvocationTarget::virtual_object(
                    service_name.clone(),
                    key.clone(),
                    handler_name.clone(),
                    h_ty,
                ),
                InvocationTargetType::Workflow(h_ty) => InvocationTarget::workflow(
                    service_name.clone(),
                    key.clone(),
                    handler_name.clone(),
                    h_ty,
                ),
            }
            .with_scope(scope);
            if idempotency_key.as_ref().is_some_and(|k| k.is_empty()) {
                return Err(InvocationError::new(
                    codes::BAD_REQUEST,
                    "the idempotency key is empty",
                ));
            }
            let limit_key: LimitKey<ReString> = match &m.limit_key {
                Some(limit_key) => limit_key.parse().map_err(|e| {
                    InvocationError::new(
                        codes::BAD_REQUEST,
                        format!("invalid limit key '{limit_key}': {e}"),
                    )
                })?,
                None => LimitKey::None,
            };
            if !limit_key.is_empty() && target.scope().is_none() {
                return Err(InvocationError::new(
                    codes::BAD_REQUEST,
                    "a limit key needs a scope",
                ));
            }
            Ok((target, meta, limit_key))
        })();

        match resolved {
            Ok((target, meta, limit_key)) => {
                let retention = meta.compute_retention(idempotency_key.is_some());
                let invocation_id = InvocationId::generate(&target, idempotency_key.as_deref());
                let span_context = ServiceInvocationSpanContext::start(
                    &invocation_id,
                    SpanRelation::parent(self.span_context.span_context().clone()),
                );
                (
                    CallRequest {
                        invocation_id,
                        invocation_target: target,
                        span_context,
                        parameter: content(m.parameter),
                        headers,
                        idempotency_key,
                        completion_retention_duration: retention.completion_retention,
                        journal_retention_duration: retention.journal_retention,
                        limit_key,
                        protocol_family: meta.protocol_family,
                    },
                    None,
                )
            }
            Err(err) => {
                // Mint the id from the names, so that a key or an idempotency key still gives a
                // deterministic partition.
                let target = if key.is_empty() {
                    InvocationTarget::service(service_name, handler_name)
                } else {
                    InvocationTarget::virtual_object(
                        service_name,
                        key,
                        handler_name,
                        restate_types::invocation::VirtualObjectHandlerType::Exclusive,
                    )
                };
                let invocation_id = InvocationId::generate(
                    &target,
                    idempotency_key.as_deref().filter(|k| !k.is_empty()),
                );
                let request = CallRequest {
                    invocation_id,
                    invocation_target: target,
                    span_context: ServiceInvocationSpanContext::empty(),
                    parameter: Bytes::new(),
                    headers,
                    idempotency_key,
                    completion_retention_duration: Default::default(),
                    journal_retention_duration: Default::default(),
                    limit_key: LimitKey::None,
                    protocol_family: Default::default(),
                };
                (request, Some(err))
            }
        }
    }
}

fn m_result(result_id: Option<NonZeroU32>) -> u32 {
    result_id.map(NonZeroU32::get).unwrap_or(0)
}

fn void_result(result_id: u32) -> Vec<(u32, NotificationResult)> {
    if result_id != 0 {
        vec![(result_id, void())]
    } else {
        vec![]
    }
}

fn propose_void(action: ErAction, result_id: u32, record: Option<RecordedAction>) -> Classified {
    Classified::Propose {
        action,
        highest_result_id: NonZeroU32::new(result_id),
        internal: void_result(result_id),
        external: vec![],
        record: record.map(|r| Box::new(with_result_id(r, result_id))),
    }
}

/// A span: proposed for the record unless it is `RECORD_NONE`, else a barrier.
fn span(record: ErRecord, level: i32, result_id: u32) -> Classified {
    if level == proto::RecordLevel::RecordNone as i32 {
        Classified::Read {
            read: Read::Barrier { result_id },
            record: None,
        }
    } else {
        Classified::Record {
            record,
            internal: void_result(result_id),
        }
    }
}

/// The target of an attach by idempotency key or workflow, for the record.
fn attach_target(query: &InvocationQuery) -> Option<RecordedTarget> {
    match query {
        InvocationQuery::Invocation(_) => None,
        InvocationQuery::IdempotencyId(id) => Some(RecordedTarget {
            service: id.service_name.to_string(),
            handler: id.service_handler.to_string(),
            key: id.service_key.as_ref().map(|k| k.to_string()),
        }),
        InvocationQuery::Workflow(service_id) => Some(RecordedTarget {
            service: service_id.service_name.to_string(),
            handler: String::new(),
            key: Some(service_id.key.to_string()),
        }),
    }
}

fn attach_query(target: Option<AttachTarget>) -> Result<InvocationQuery, Violation> {
    let parse_scope = |scope: Option<String>| -> Result<Option<Scope>, Violation> {
        match scope {
            Some(scope) if !scope.is_empty() => Scope::try_non_interned(&scope)
                .map(Some)
                .map_err(|e| Violation::new(format!("invalid scope '{scope}': {e}"))),
            _ => Ok(None),
        }
    };
    Ok(match target {
        None => return Err(Violation::new("an AttachInvocation with no target")),
        Some(AttachTarget::InvocationId(id)) => {
            InvocationQuery::Invocation(parse_invocation_id(&id)?)
        }
        Some(AttachTarget::IdempotentRequestTarget(t)) => {
            InvocationQuery::IdempotencyId(IdempotencyId::new(
                t.service_name.into(),
                t.service_key.map(Into::into),
                t.handler_name.into(),
                t.idempotency_key.into(),
                parse_scope(t.scope)?,
            ))
        }
        Some(AttachTarget::WorkflowTarget(t)) => InvocationQuery::Workflow(ServiceId::new(
            parse_scope(t.scope)?,
            t.workflow_name,
            t.workflow_key,
        )),
    })
}

/// The keys of a scope, as a result.
pub(crate) fn state_keys(keys: Vec<Bytes>) -> NotificationResult {
    NotificationResult::StateKeys(StateKeys { keys })
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_types::invocation::ServiceType;
    use restate_types::invocation::VirtualObjectHandlerType;
    use restate_types::journal_events::OutstandingResult;
    use restate_types::schema::invocation_target::InvocationTargetMetadata;
    use restate_types::schema::invocation_target::test_util::MockInvocationTargetResolver;

    fn set_state(scope: proto::StateScope, result_id: u32) -> proto::SetStateActionMessage {
        proto::SetStateActionMessage {
            scope: scope.into(),
            key: Bytes::from_static(b"k"),
            value: Some(Value {
                content: Bytes::from_static(b"v"),
            }),
            result_id,
            ..Default::default()
        }
    }

    #[test]
    fn groups_calls_and_violations() {
        let target = InvocationTarget::mock_virtual_object();
        let span_context = ServiceInvocationSpanContext::empty();
        let ctx = ActionContext {
            invocation_id: InvocationId::mock_generate(&target),
            invocation_target: &target,
            span_context: &span_context,
        };
        let mut resolver = MockInvocationTargetResolver::default();
        resolver.add(
            "callee",
            ServiceType::VirtualObject,
            [(
                "h",
                InvocationTargetMetadata::mock(InvocationTargetType::VirtualObject(
                    VirtualObjectHandlerType::Exclusive,
                )),
            )],
        );
        let mut book = AttemptBook::new(5, false, 2);

        // A group: a write of each scope and a call; ids in wire order, the group's last.
        let call = proto::CallActionMessage {
            service_name: "callee".into(),
            handler_name: "h".into(),
            key: "k".into(),
            invocation_id_result_id: 8,
            result_id: 7,
            ..Default::default()
        };
        // The invocation-scope write is not recorded; the span is a member of the group's record.
        let group = proto::AtomicGroupActionMessage {
            actions: [
                GroupMember::SetState(set_state(proto::StateScope::Object, 5)),
                GroupMember::SetState(proto::SetStateActionMessage {
                    record: proto::RecordLevel::RecordNone.into(),
                    ..set_state(proto::StateScope::Invocation, 0)
                }),
                GroupMember::Call(call.clone()),
                GroupMember::SpanStart(proto::SpanStartActionMessage {
                    span_id: 1,
                    name: "step".into(),
                    ..Default::default()
                }),
            ]
            .into_iter()
            .map(|a| proto::atomic_group_action_message::Action { action: Some(a) })
            .collect(),
            result_id: 9,
            name: "commit".into(),
            ..Default::default()
        };
        let Classified::Propose {
            action: ErAction::Group(members),
            highest_result_id,
            internal,
            external,
            record: Some(record),
        } = ctx
            .classify(Message::AtomicGroupAction(group), &mut book, &resolver)
            .unwrap()
        else {
            panic!("a group is proposed");
        };
        assert_eq!(members.len(), 3);
        assert_eq!(highest_result_id, NonZeroU32::new(9));
        assert_eq!(
            (record.kind.as_str(), record.name.as_str(), record.result_id),
            ("AtomicGroup", "commit", Some(9))
        );
        assert_eq!(
            record
                .members
                .iter()
                .map(|m| (m.kind.as_str(), m.summary.span_id))
                .collect::<Vec<_>>(),
            vec![("SetState", None), ("Call", None), ("SpanStart", Some(1))]
        );
        let call_record = &record.members[1];
        assert_eq!(
            (call_record.result_id, call_record.invocation_id_result_id),
            (Some(7), Some(8))
        );
        assert_eq!(
            call_record
                .summary
                .target
                .as_ref()
                .map(|t| t.key.as_deref()),
            Some(Some("k"))
        );
        // Summary level: no payload.
        assert!(record.members[0].payload.is_none());
        // The call's result is outstanding once proposed, labelled by its target.
        book.propose(vec![], external.clone());
        assert_eq!(
            book.outstanding(),
            vec![OutstandingResult {
                result_id: 7,
                kind: OutstandingKind::Call,
                label: "callee/h".to_owned(),
            }]
        );
        assert_eq!(
            internal.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![5, 8, 9]
        );
        assert_eq!(external, vec![7]);

        // A call to a target that cannot be invoked: its id, then a failure, nothing external.
        let unknown = proto::CallActionMessage {
            service_name: "unknown".into(),
            invocation_id_result_id: 10,
            result_id: 11,
            ..call
        };
        let Classified::Propose {
            action: ErAction::Call { uninvokable, .. },
            internal,
            external,
            ..
        } = ctx
            .classify(Message::CallAction(unknown), &mut book, &resolver)
            .unwrap()
        else {
            panic!("a call is proposed");
        };
        assert!(uninvokable.is_some());
        assert!(matches!(
            internal.as_slice(),
            [
                (10, NotificationResult::InvocationId(_)),
                (11, NotificationResult::Failure(_))
            ]
        ));
        assert!(external.is_empty());

        // Violations: an id out of order, a required id missing, an empty group, scope 0.
        for message in [
            Message::SetStateAction(set_state(proto::StateScope::Object, 11)),
            Message::FlushAction(proto::FlushActionMessage::default()),
            Message::AtomicGroupAction(proto::AtomicGroupActionMessage::default()),
            Message::SetStateAction(set_state(proto::StateScope::Unspecified, 0)),
        ] {
            assert!(ctx.classify(message, &mut book, &resolver).is_err());
        }

        // Attach to itself: answered at once with a failure.
        assert!(matches!(
            ctx.classify(
                Message::AttachInvocationAction(proto::AttachInvocationActionMessage {
                    target: Some(AttachTarget::InvocationId(ctx.invocation_id.to_string())),
                    result_id: 20,
                    ..Default::default()
                }),
                &mut book,
                &resolver
            )
            .unwrap(),
            Classified::Answer(_)
        ));
    }
}
