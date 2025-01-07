use crate::identifiers::InvocationId;
use crate::invocation::{Header, InvocationTarget, ServiceInvocationSpanContext};
use crate::journal_v2::notification::Failure;
use crate::journal_v2::raw::{TryFromEntry, TryFromEntryError};
use crate::journal_v2::{CompletionNotificationIndex, Entry, EntryMetadata, EntryType};
use crate::time::MillisSinceEpoch;
use bytes::Bytes;
use bytestring::ByteString;
use std::fmt;
use strum::EnumDiscriminants;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandMetadata {
    /// User provided name, if any.
    pub(crate) name: ByteString,
}

impl CommandMetadata {
    pub fn new(name: impl Into<ByteString>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub metadata: CommandMetadata,
    pub inner: CommandInner,
}

impl Command {
    pub fn new(metadata: CommandMetadata, inner: impl Into<CommandInner>) -> Self {
        Self {
            metadata,
            inner: inner.into(),
        }
    }
}

impl EntryMetadata for Command {
    fn ty(&self) -> EntryType {
        self.inner.ty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, EnumDiscriminants)]
#[strum_discriminants(vis(pub))]
#[strum_discriminants(name(CommandType))]
#[strum_discriminants(derive(serde::Serialize, serde::Deserialize))]
#[enum_delegate::implement(EntryMetadata)]
pub enum CommandInner {
    Input(InputCommand),
    Run(RunCommand),
    Sleep(SleepCommand),
    Call(CallCommand),
    OneWayCall(OneWayCallCommand),
    Output(OutputCommand),
}

impl fmt::Display for CommandType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

// Little macro to reduce boilerplate for CompletableCommandAccessor, TryFromEntry and EntryMetadata.
macro_rules! impl_entry_accessors {
    ($ty:ident -> []) => {
        // End of macro
    };
    ($ty:ident -> [@from_entry $($tail:tt)*]) => {
        impl TryFromEntry for (CommandMetadata, paste::paste! { [< $ty Command >] }) {
            fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
                match entry {
                    super::Entry::Command(Command {
                        inner: CommandInner::$ty(e),
                        metadata
                    }) => Ok((metadata, e)),
                    e => Err(TryFromEntryError {
                        expected: EntryType::Command(CommandType::$ty),
                        actual: e.ty(),
                    }),
                }
            }
        }
        impl TryFromEntry for paste::paste! { [< $ty Command >] } {
            fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
                match entry {
                    super::Entry::Command(Command {
                        inner: CommandInner::$ty(e),
                        metadata: _
                    }) => Ok(e),
                    e => Err(TryFromEntryError {
                        expected: EntryType::Command(CommandType::$ty),
                        actual: e.ty(),
                    }),
                }
            }
        }
        impl_entry_accessors!($ty -> [$($tail)*]);
    };
    ($ty:ident -> [@metadata $($tail:tt)*]) => {
        impl EntryMetadata for paste::paste! { [< $ty Command >] } {
            fn ty(&self) -> EntryType {
                EntryType::Command(CommandType::$ty)
            }
        }
        impl_entry_accessors!($ty -> [$($tail)*]);
    };

    // Entrypoints of the macro
    ($ty:ident: [$($tokens:tt)*]) => {
        impl_entry_accessors!($ty -> [$($tokens)*]);
    };
    ($ty:ident) => {
        impl_entry_accessors!($ty -> [@metadata @from_entry]);
    };
}

// --- Actual implementation of individual commands

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputCommand {
    pub headers: Vec<Header>,
    pub payload: Bytes,
}
impl_entry_accessors!(Input);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCommand {
    pub notification_idx: CompletionNotificationIndex,
}
impl_entry_accessors!(Run);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallRequest {
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub span_context: ServiceInvocationSpanContext,
    pub parameter: Bytes,
    pub headers: Vec<Header>,
    pub idempotency_key: Option<ByteString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleepCommand {
    pub wake_up_time: MillisSinceEpoch,
    pub notification_idx: CompletionNotificationIndex,
}
impl_entry_accessors!(Sleep);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallCommand {
    pub request: CallRequest,
    pub invocation_id_notification_idx: CompletionNotificationIndex,
    pub result_notification_idx: CompletionNotificationIndex,
}
impl_entry_accessors!(Call);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneWayCallCommand {
    pub request: CallRequest,
    pub invoke_time: MillisSinceEpoch,
    pub invocation_id_notification_idx: CompletionNotificationIndex,
}
impl_entry_accessors!(OneWayCall);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCommand {
    pub result: OutputResult,
}
impl_entry_accessors!(Output);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputResult {
    Success(Bytes),
    Failure(Failure),
}
