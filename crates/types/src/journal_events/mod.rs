// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains the data model of journal events.

use crate::errors::InvocationErrorCode;
use crate::journal_v2::{CommandType, UnresolvedFuture};
use serde::{Deserialize, Serialize};
use strum::EnumString;

pub mod raw;

#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    EnumString,
    strum::Display,
    Serialize,
    Deserialize,
    strum::FromRepr,
)]
#[repr(u8)]
pub enum EventType {
    Unknown = 0,
    TransientError = 1,
    Paused = 2,
    Suspended = 3,
    AttemptStarted = 4,
    AttemptEnded = 5,
    ActionRecorded = 6,
    SpanStarted = 7,
    SpanEnded = 8,
    ResultDelivered = 9,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, derive_more::From, strum::EnumTryAs,
)]
#[serde(tag = "ty")]
pub enum Event {
    TransientError(TransientErrorEvent),
    Paused(PausedEvent),
    Suspended(SuspendedEvent),
    /// An attempt of an explicit recovery invocation started.
    AttemptStarted(AttemptStartedEvent),
    /// An attempt of an explicit recovery invocation ended.
    AttemptEnded(AttemptEndedEvent),
    /// An action of an explicit recovery attempt, as its record level asks.
    ActionRecorded(ActionRecordedEvent),
    /// A span of an explicit recovery attempt started.
    SpanStarted(SpanStartedEvent),
    /// A span of an explicit recovery attempt ended.
    SpanEnded(SpanEndedEvent),
    /// An external result reached a running explicit recovery attempt.
    ResultDelivered(ResultDeliveredEvent),
    /// This is used when it's not possible to parse in this Restate version the event.
    Unknown,
}

// --- The individual events

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransientErrorEvent {
    pub error_code: InvocationErrorCode,
    pub error_message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_stacktrace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restate_doc_error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_command_index: Option<crate::journal_v2::CommandIndex>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_command_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_command_type: Option<CommandType>,
    /// Set only for an invocation on the explicit recovery protocol family, as in its attempt
    /// record: the log position of the record that wrote the event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PausedEvent {
    /// This is potentially empty if the service was manually paused
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<TransientErrorEvent>,
    /// As in [`TransientErrorEvent`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuspendedEvent {
    pub awaiting_on: UnresolvedFuture,
}

// --- The record of the attempts of an explicit recovery invocation (docs/14).
//
// Every event carries `lsn`, the log position of the record that wrote it: events of one
// millisecond are ordered by it. Absent in events written before it existed.

/// Enumerations of the attempt record: serialized, in JSON and in storage, as snake_case names.
macro_rules! record_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, strum::Display, EnumString,
        )]
        #[serde(rename_all = "snake_case")]
        #[strum(serialize_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }
    };
}

record_enum!(
    /// Why an attempt started.
    AttemptCause {
        First,
        Retry,
        Restart,
        WakeUp,
        Resume
    }
);
record_enum!(
    /// What woke a suspended (or paused) invocation.
    WakeUpKind {
        Result,
        Signal,
        Cancel
    }
);
record_enum!(
    /// How an attempt ended.
    AttemptOutcome {
        Returned,
        Failed,
        Restarted,
        Suspended,
        Paused,
        Killed,
        Lost
    }
);
record_enum!(
    /// What a suspended attempt was waiting for.
    OutstandingKind {
        Call,
        Attach,
        SignalRead
    }
);
record_enum!(
    /// How much of an action is recorded.
    RecordLevel { Summary, Full }
);
record_enum!(
    /// The state scope of a recorded action.
    RecordedScope { Object, Invocation }
);
record_enum!(
    /// How a span ended.
    SpanOutcome { Ok, Value, Failure }
);
record_enum!(
    /// What a delivered result carries.
    ResultVariant {
        Value,
        Failure,
        Void
    }
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptStartedEvent {
    pub attempt: u32,
    pub deployment_id: String,
    pub protocol_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<AttemptCause>,
    /// Only when `cause` is `wake_up`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub woken_by: Option<WokenBy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
    /// When the scheduler ran the attempt (gave it a slot), in milliseconds since the UNIX epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WokenBy {
    pub kind: WakeUpKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptEndedEvent {
    pub attempt: u32,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<AttemptOutcome>,
    /// The restart or suspend rule of the spec, 1 to 6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<EventFailure>,
    /// What a suspended attempt was waiting on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outstanding: Vec<OutstandingResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
    /// A failed attempt whose retry is delayed: the next one runs not before then, in
    /// milliseconds since the UNIX epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventFailure {
    pub code: u32,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutstandingResult {
    pub result_id: u32,
    pub kind: OutstandingKind,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionRecordedEvent {
    pub attempt: u32,
    #[serde(flatten)]
    pub action: Box<RecordedAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
}

/// An action as the record keeps it. A member of an atomic group is one of these too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedAction {
    /// The action's message name without `Action`: `SetState`, `Call`, `AtomicGroup`...
    pub kind: String,
    /// The action's display name, may be empty.
    #[serde(default)]
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id_result_id: Option<u32>,
    pub level: RecordLevel,
    #[serde(default)]
    pub summary: ActionSummary,
    /// Only at [`RecordLevel::Full`]: UTF-8 lossy, truncated to [`RECORD_PAYLOAD_LIMIT`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<RecordedAction>,
}

/// The largest payload, in bytes, the record keeps of an action or a span.
pub const RECORD_PAYLOAD_LIMIT: usize = 4 * 1024;

/// `bytes` as UTF-8 lossy, cut at [`RECORD_PAYLOAD_LIMIT`] bytes on a character boundary.
pub fn record_payload(bytes: &[u8]) -> String {
    let lossy = String::from_utf8_lossy(&bytes[..bytes.len().min(RECORD_PAYLOAD_LIMIT)]);
    let mut end = lossy.len().min(RECORD_PAYLOAD_LIMIT);
    while !lossy.is_char_boundary(end) {
        end -= 1;
    }
    lossy[..end].to_owned()
}

/// What applies of: the state key, the target, the callee, the signal, the payload size, and for
/// a span inside an atomic group its id and how it ended.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<RecordedScope>,
    /// UTF-8 lossy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<RecordedTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<SpanOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<EventFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedTarget {
    pub service: String,
    pub handler: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanStartedEvent {
    pub attempt: u32,
    pub span_id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<u32>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanEndedEvent {
    pub attempt: u32,
    pub span_id: u32,
    pub outcome: SpanOutcome,
    /// Only at [`RecordLevel::Full`]: UTF-8 lossy, truncated to [`RECORD_PAYLOAD_LIMIT`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<EventFailure>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultDeliveredEvent {
    pub attempt: u32,
    pub result_id: u32,
    pub variant: ResultVariant,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<u64>,
}
