// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::{HeaderName, HeaderValue};
use restate_invoker_api::InvocationErrorReport;
use restate_service_client::ServiceClientError;
use restate_service_protocol::message::{EncodingError, MessageType};
use restate_time_util::FriendlyDuration;
use restate_types::errors::{InvocationError, InvocationErrorCode, codes};
use restate_types::identifiers::DeploymentId;
use restate_types::invocation::InvocationEpoch;
use restate_types::journal::raw::RawEntryCodecError;
use restate_types::journal::{EntryIndex, EntryType};
use restate_types::journal_v2;
use restate_types::journal_v2::CommandIndex;
use restate_types::service_protocol::{
    MAX_INFLIGHT_SERVICE_PROTOCOL_VERSION, MIN_INFLIGHT_SERVICE_PROTOCOL_VERSION,
    ServiceProtocolVersion,
};
use std::collections::HashSet;
use std::error::Error as StdError;
use std::fmt;
use std::ops::RangeInclusive;
use std::time::Duration;
use tokio::task::JoinError;

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub(crate) enum InvokerError {
    #[error("no deployment was found to process the invocation")]
    #[code(restate_errors::RT0011)]
    NoDeploymentForService,
    #[error(
        "the invocation has a deployment id associated, but it was not found in the registry. This might indicate that a deployment was forcefully removed from the registry, but there are still in-flight invocations pinned to it"
    )]
    #[code(restate_errors::RT0011)]
    UnknownDeployment(DeploymentId),

    #[error("unexpected http status code: {0}")]
    #[code(restate_errors::RT0012)]
    UnexpectedResponse(http::StatusCode),
    #[error("cannot start the invocation because the SDK doesn't support the protocol version '{}' negotiated at discovery time", .0.as_repr())]
    #[code(restate_errors::RT0015)]
    BadNegotiatedServiceProtocolVersion(ServiceProtocolVersion),
    #[error("service replied with content too large")]
    #[code(restate_errors::RT0019)]
    ContentTooLarge,

    #[error("unexpected content type '{0:?}'; expected content type '{1:?}'")]
    #[code(restate_errors::RT0012)]
    UnexpectedContentType(Option<HeaderValue>, HeaderValue),
    #[error("received unexpected message: {0:?}")]
    #[code(restate_errors::RT0012)]
    UnexpectedMessage(MessageType),
    #[error("received unexpected message: {0:?}")]
    #[code(restate_errors::RT0012)]
    UnexpectedMessageV4(restate_service_protocol_v4::message_codec::MessageType),
    #[error("message encoding error: {0}")]
    Encoding(
        #[from]
        #[code]
        EncodingError,
    ),
    #[error("message encoding error: {0}")]
    #[code(restate_errors::RT0012)]
    EncodingV2(#[from] journal_v2::encoding::DecodingError),
    #[error("message encoding error: {0}")]
    EncoderV2(
        #[from]
        #[code]
        restate_service_protocol_v4::message_codec::EncodingError,
    ),
    #[error("cannot decode entry: {0}")]
    #[code(restate_errors::RT0012)]
    RawEntry(#[from] RawEntryCodecError),
    #[error("cannot decode entry: {0}")]
    #[code(restate_errors::RT0012)]
    RawEntryV2(#[from] journal_v2::raw::RawEntryError),
    #[error(
        "Unexpected end of invocation stream, after EndMessage or ErrorMessage or SuspensionMessage no other bytes should be received"
    )]
    #[code(restate_errors::RT0012)]
    WriteAfterEndOfStream,
    #[error("Cannot decode header '{0}': {1}")]
    #[code(restate_errors::RT0012)]
    BadHeader(HeaderName, #[source] http::header::ToStrError),
    #[error("got empty SuspensionMessage")]
    #[code(restate_errors::RT0012)]
    EmptySuspensionMessage,
    #[error(
        "got bad SuspensionMessage, suspending on journal indexes {0:?}, but journal length is {1}"
    )]
    #[code(restate_errors::RT0012)]
    BadSuspensionMessage(HashSet<EntryIndex>, EntryIndex),
    #[error("malformed ProposeRunCompletionMessage, missing result field")]
    #[code(restate_errors::RT0012)]
    MalformedProposeRunCompletion,

    #[error("error when trying to read the journal: {0}")]
    #[code(restate_errors::RT0006)]
    JournalReader(anyhow::Error),
    #[error("error when trying to read the journal: not invoked")]
    #[code(unknown)]
    NotInvoked,
    #[error("error when trying to read the service instance state: {0}")]
    #[code(restate_errors::RT0006)]
    StateReader(anyhow::Error),
    #[error(
        "error when reading the journal: actual epoch {actual} != expected epoch {expected}. This is expected to happen while a trim and restart is being processed."
    )]
    #[code(restate_errors::RT0006)]
    StaleJournalRead {
        actual: InvocationEpoch,
        expected: InvocationEpoch,
    },

    #[error(transparent)]
    #[code(restate_errors::RT0010)]
    Client(Box<ServiceClientError>),
    #[error("unexpected error while reading the response body: {0}")]
    #[code(restate_errors::RT0010)]
    ClientBody(Box<dyn std::error::Error + Send + Sync>),
    #[error("unexpected join error, looks like hyper panicked: {0}")]
    #[code(restate_errors::RT0010)]
    UnexpectedJoinError(#[from] JoinError),
    #[error("unexpected closed request stream while trying to write a message")]
    #[code(restate_errors::RT0010)]
    UnexpectedClosedRequestStream,

    #[error("the invocation stream was closed after the 'abort timeout' ({0}) fired.")]
    #[code(restate_errors::RT0001)]
    AbortTimeoutFired(FriendlyDuration),

    #[error("cannot process entry {1} (index {0}) because of a failed precondition: {2}")]
    #[code(restate_errors::RT0017)]
    EntryEnrichment(EntryIndex, EntryType, #[source] InvocationError),
    #[error("cannot process command {1} (command index {0}) because of a failed precondition: {2}")]
    CommandPrecondition(
        CommandIndex,
        journal_v2::EntryType,
        #[source]
        #[code]
        CommandPreconditionError,
    ),

    #[error(transparent)]
    Sdk(
        #[from]
        #[code]
        SdkInvocationError,
    ),
    #[error(transparent)]
    SdkV2(
        #[from]
        #[code]
        SdkInvocationErrorV2,
    ),

    #[error("cannot talk to service endpoint '{0}' because its service protocol versions [{start}, {end}] are incompatible with the server's service protocol versions [{min}, {max}].", start = .1.start(), end = .1.end(), min = i32::from(MIN_INFLIGHT_SERVICE_PROTOCOL_VERSION), max = i32::from(MAX_INFLIGHT_SERVICE_PROTOCOL_VERSION))]
    #[code(restate_errors::RT0013)]
    IncompatibleServiceEndpoint(DeploymentId, RangeInclusive<i32>),
    #[error("cannot resume invocation because it was created with an incompatible service protocol version '{}' and the server does not support upgrading versions yet", .0.as_repr())]
    #[code(restate_errors::RT0014)]
    ResumeWithWrongServiceProtocolVersion(ServiceProtocolVersion),

    #[error("service is temporary unavailable '{0}'")]
    #[code(restate_errors::RT0010)]
    ServiceUnavailable(http::StatusCode),
}

impl InvokerError {
    pub(crate) fn error_stacktrace(&self) -> Option<&str> {
        match self {
            InvokerError::Sdk(s) => s
                .error
                .stacktrace()
                .and_then(|s| if s.is_empty() { None } else { Some(s) }),
            InvokerError::SdkV2(s) => s
                .error
                .stacktrace()
                .and_then(|s| if s.is_empty() { None } else { Some(s) }),
            _ => None,
        }
    }

    pub(crate) fn is_transient(&self) -> bool {
        !matches!(self, InvokerError::NotInvoked)
    }

    pub(crate) fn should_bump_start_message_retry_count_since_last_stored_entry(&self) -> bool {
        !matches!(
            self,
            InvokerError::NotInvoked
                | InvokerError::JournalReader(_)
                | InvokerError::StateReader(_)
                | InvokerError::NoDeploymentForService
                | InvokerError::BadNegotiatedServiceProtocolVersion(_)
                | InvokerError::UnknownDeployment(_)
                | InvokerError::ResumeWithWrongServiceProtocolVersion(_)
                | InvokerError::IncompatibleServiceEndpoint(_, _)
        )
    }

    pub(crate) fn next_retry_interval_override(&self) -> Option<Duration> {
        match self {
            InvokerError::Sdk(SdkInvocationError {
                next_retry_interval_override,
                ..
            }) => *next_retry_interval_override,
            InvokerError::SdkV2(SdkInvocationErrorV2 {
                next_retry_interval_override,
                ..
            }) => *next_retry_interval_override,
            _ => None,
        }
    }

    pub(crate) fn into_invocation_error(self) -> InvocationError {
        match self {
            InvokerError::Sdk(sdk_error) => sdk_error.error,
            InvokerError::SdkV2(sdk_error) => sdk_error.error,
            InvokerError::EntryEnrichment(entry_index, entry_type, e) => {
                let msg = format!(
                    "Error when processing entry {} of type {}: {}",
                    entry_index,
                    entry_type,
                    e.message()
                );
                let mut err = InvocationError::new(e.code(), msg);
                if let Some(desc) = e.into_stacktrace() {
                    err = err.with_stacktrace(desc);
                }
                err
            }
            e @ InvokerError::BadNegotiatedServiceProtocolVersion(_) => {
                InvocationError::new(codes::UNSUPPORTED_MEDIA_TYPE, e.to_string())
            }
            e => InvocationError::internal(e.to_string()),
        }
    }

    pub(crate) fn into_invocation_error_report(mut self) -> InvocationErrorReport {
        let doc_error_code = codederror::CodedError::code(&self);
        let maybe_related_entry = match self {
            InvokerError::Sdk(SdkInvocationError {
                ref mut related_entry,
                ..
            }) => related_entry.take(),
            InvokerError::SdkV2(SdkInvocationErrorV2 {
                related_command: ref mut related_entry,
                ..
            }) => related_entry
                .take()
                .map(InvocationErrorRelatedCommandV2::best_effort_into_v1),
            _ => None,
        }
        .unwrap_or_default();
        let err = self.into_invocation_error();

        InvocationErrorReport {
            doc_error_code,
            err,
            related_entry_index: maybe_related_entry.related_entry_index,
            related_entry_name: maybe_related_entry.related_entry_name,
            related_entry_type: maybe_related_entry.related_entry_type,
        }
    }
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(restate_errors::RT0017)]
pub(crate) enum CommandPreconditionError {
    #[error("the service handler {0}/{1} was not found")]
    #[code(restate_errors::RT0018)]
    ServiceHandlerNotFound(String, String),
    #[error("the request key is not a valid UTF-8 string: {0}")]
    BadRequestKey(#[from] std::str::Utf8Error),
    #[error(
        "a key was provided, but the callee '{0}' is a Service. Only VirtualObjects or Workflows accept a key"
    )]
    UnexpectedKey(String),
    #[error("the idempotency key provided in the request is empty")]
    EmptyIdempotencyKey,
    #[error("unsupported entry type, only Workflow services support it")]
    NoWorkflowOperations,
    #[error("unsupported entry type, this service type doesn't have state access")]
    NoStateOperations,
    #[error("unsupported entry type, this handler type cannot write state")]
    NoWriteStateOperations,
}

#[derive(Debug)]
pub(crate) struct SdkInvocationError {
    pub(crate) related_entry: Option<InvocationErrorRelatedEntry>,
    pub(crate) next_retry_interval_override: Option<Duration>,
    pub(crate) error: InvocationError,
}

impl SdkInvocationError {
    pub(crate) fn unknown() -> Self {
        Self {
            related_entry: None,
            next_retry_interval_override: None,
            error: Default::default(),
        }
    }
}

impl fmt::Display for SdkInvocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.error.code() == codes::JOURNAL_MISMATCH {
            writeln!(
                f,
                "Detected journal mismatch. Either some code within the handler is non-deterministic, or the code was updated without registering a new service deployment."
            )?;
        } else {
            writeln!(
                f,
                "Got a (transient) error from the service while processing invocation."
            )?;
        }

        // We don't use the default formatting of error, as we want to display the stacktrace at the bottom
        writeln!(f, "[{}] {}.", self.error.code(), self.error.message())?;

        if let Some(related_entry) = &self.related_entry {
            if related_entry.entry_was_committed {
                write!(f, "> This error originated after executing {related_entry}")?;
            } else {
                write!(
                    f,
                    "> This error originated while processing {related_entry}"
                )?;
            }
        }
        Ok(())
    }
}

impl StdError for SdkInvocationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.error)
    }

    fn cause(&self) -> Option<&dyn StdError> {
        Some(&self.error)
    }
}

impl codederror::CodedError for SdkInvocationError {
    fn code(&self) -> Option<&'static codederror::Code> {
        Some(specialized_restate_error_code_for_invocation_error(
            self.error.code(),
        ))
    }
}

#[derive(Debug, Default)]
pub(crate) struct InvocationErrorRelatedEntry {
    pub(crate) related_entry_index: Option<EntryIndex>,
    pub(crate) related_entry_name: Option<String>,
    pub(crate) related_entry_type: Option<EntryType>,
    pub(crate) entry_was_committed: bool,
}

impl fmt::Display for InvocationErrorRelatedEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(t) = self.related_entry_type {
            write!(f, "{t}")?;
        } else {
            write!(f, "Unknown")?;
        }
        if let Some(n) = &self.related_entry_name
            && !n.is_empty()
        {
            write!(f, "(\"{n}\")")?;
        }
        if let Some(i) = self.related_entry_index {
            write!(f, "[index {i}]")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct SdkInvocationErrorV2 {
    pub(crate) related_command: Option<InvocationErrorRelatedCommandV2>,
    pub(crate) next_retry_interval_override: Option<Duration>,
    pub(crate) error: InvocationError,
}

impl SdkInvocationErrorV2 {
    pub(crate) fn unknown() -> Self {
        Self {
            related_command: None,
            next_retry_interval_override: None,
            error: Default::default(),
        }
    }
}

impl fmt::Display for SdkInvocationErrorV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.error.code() == codes::JOURNAL_MISMATCH {
            writeln!(
                f,
                "Detected journal mismatch. Either some code within the handler is non-deterministic, or the code was updated without registering a new service deployment."
            )?;
        } else {
            writeln!(
                f,
                "Got a (transient) error from the service while processing invocation."
            )?;
        }

        // We don't use the default formatting of error, as we wanna display the stacktrace at the bottom
        writeln!(f, "[{}] {}.", self.error.code(), self.error.message())?;

        if let Some(related_command) = &self.related_command {
            if related_command.command_was_committed {
                write!(
                    f,
                    "> This error originated after executing {related_command}"
                )?;
            } else {
                write!(
                    f,
                    "> This error originated while processing {related_command}"
                )?;
            }
        }
        Ok(())
    }
}

impl StdError for SdkInvocationErrorV2 {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.error)
    }

    fn cause(&self) -> Option<&dyn StdError> {
        Some(&self.error)
    }
}

impl codederror::CodedError for SdkInvocationErrorV2 {
    fn code(&self) -> Option<&'static codederror::Code> {
        Some(specialized_restate_error_code_for_invocation_error(
            self.error.code(),
        ))
    }
}

#[derive(Debug, Default)]
pub(crate) struct InvocationErrorRelatedCommandV2 {
    pub(crate) related_command_index: Option<EntryIndex>,
    pub(crate) related_command_name: Option<String>,
    pub(crate) related_entry_type: Option<journal_v2::EntryType>,
    pub(crate) command_was_committed: bool,
}

impl InvocationErrorRelatedCommandV2 {
    pub(crate) fn best_effort_into_v1(self) -> InvocationErrorRelatedEntry {
        InvocationErrorRelatedEntry {
            related_entry_index: self.related_command_index,
            related_entry_name: self.related_command_name,
            // Until the invoker status handle doesn't support the v2 types, let's migrate some common entry types to v1
            related_entry_type: match self.related_entry_type {
                Some(journal_v2::EntryType::Command(journal_v2::CommandType::Run)) => {
                    Some(EntryType::Run)
                }
                Some(journal_v2::EntryType::Command(journal_v2::CommandType::Call)) => {
                    Some(EntryType::Call)
                }
                Some(journal_v2::EntryType::Command(journal_v2::CommandType::OneWayCall)) => {
                    Some(EntryType::OneWayCall)
                }
                Some(journal_v2::EntryType::Command(
                    journal_v2::CommandType::CompleteAwakeable,
                )) => Some(EntryType::CompleteAwakeable),
                Some(journal_v2::EntryType::Command(journal_v2::CommandType::SetState)) => {
                    Some(EntryType::SetState)
                }
                Some(journal_v2::EntryType::Command(journal_v2::CommandType::GetLazyState)) => {
                    Some(EntryType::GetState)
                }
                Some(journal_v2::EntryType::Command(journal_v2::CommandType::GetEagerState)) => {
                    Some(EntryType::GetState)
                }
                Some(journal_v2::EntryType::Command(journal_v2::CommandType::GetLazyStateKeys)) => {
                    Some(EntryType::GetStateKeys)
                }
                Some(journal_v2::EntryType::Command(
                    journal_v2::CommandType::GetEagerStateKeys,
                )) => Some(EntryType::GetStateKeys),
                _ => None,
            },
            entry_was_committed: self.command_was_committed,
        }
    }
}

impl fmt::Display for InvocationErrorRelatedCommandV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(t) = self.related_entry_type {
            write!(f, "{t}")?;
        } else {
            write!(f, "Unknown")?;
        }
        if let Some(n) = &self.related_command_name
            && !n.is_empty()
        {
            write!(f, "(named \"{n}\")")?;
        }
        if let Some(i) = self.related_command_index {
            write!(f, "[command index {i}]")?;
        }
        Ok(())
    }
}

fn specialized_restate_error_code_for_invocation_error(
    invocation_error_code: InvocationErrorCode,
) -> &'static codederror::Code {
    match invocation_error_code {
        codes::JOURNAL_MISMATCH => &restate_errors::RT0016,
        codes::PROTOCOL_VIOLATION => &restate_errors::RT0012,
        _ => &restate_errors::RT0007,
    }
}
