use crate::errors::UserErrorCode;
use crate::partitioner::HashPartitioner;
use base64::display::Base64Display;
use base64::prelude::*;
use bytes::Bytes;
use bytestring::ByteString;
use opentelemetry_api::trace::{SpanContext, TraceContextExt};
use std::fmt;
use std::fmt::Display;
use std::ops::Add;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use tracing::{info_span, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

/// Identifying a member of a raft group
pub type PeerId = u64;

/// Identifying the leader epoch of a raft group leader
pub type LeaderEpoch = u64;

/// Identifying the partition
pub type PartitionId = u64;

/// The leader epoch of a given partition
pub type PartitionLeaderEpoch = (PartitionId, LeaderEpoch);

pub type EntryIndex = u32;

/// Identifying to which partition a key belongs. This is unlike the [`PartitionId`]
/// which identifies a consecutive range of partition keys.
pub type PartitionKey = u32;

/// Discriminator for invocation instances
pub type InvocationId = Uuid;

/// Id of a single service invocation.
///
/// A service invocation id is composed of a [`ServiceId`] and an [`InvocationId`]
/// that makes the id unique.
#[derive(Eq, Hash, PartialEq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ServiceInvocationId {
    /// Identifies the invoked service
    pub service_id: ServiceId,
    /// Uniquely identifies this invocation instance
    pub invocation_id: InvocationId,
}

impl ServiceInvocationId {
    pub fn new(
        service_name: impl Into<ByteString>,
        key: impl Into<Bytes>,
        invocation_id: impl Into<InvocationId>,
    ) -> Self {
        Self::with_service_id(ServiceId::new(service_name, key), invocation_id)
    }

    pub fn with_service_id(service_id: ServiceId, invocation_id: impl Into<InvocationId>) -> Self {
        Self {
            service_id,
            invocation_id: invocation_id.into(),
        }
    }
}

impl Display for ServiceInvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.service_id.service_name,
            Base64Display::new(&self.service_id.key, &BASE64_STANDARD),
            self.invocation_id.as_simple()
        )
    }
}

#[derive(Debug, Default, thiserror::Error)]
#[error("cannot parse the opaque id, bad format")]
pub struct ParseError {
    #[source]
    cause: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl ParseError {
    pub fn from_cause(cause: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            cause: Some(Box::new(cause)),
        }
    }
}

impl FromStr for ServiceInvocationId {
    type Err = ParseError;

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        // This encoding is based on the fact that neither invocation_id
        // nor service_name can contain the '-' character
        // Invocation id is serialized as simple
        // Service name follows the fullIdent ABNF here:
        // https://protobuf.dev/reference/protobuf/proto3-spec/#identifiers
        let mut splits: Vec<&str> = str.splitn(3, '-').collect();
        if splits.len() != 3 {
            return Err(ParseError::default());
        }
        let invocation_id: Uuid = splits
            .pop()
            .unwrap()
            .parse()
            .map_err(ParseError::from_cause)?;
        let key = BASE64_STANDARD
            .decode(splits.pop().unwrap())
            .map_err(ParseError::from_cause)?;
        let service_name = splits.pop().unwrap().to_string();

        Ok(ServiceInvocationId::new(service_name, key, invocation_id))
    }
}

/// Id of a keyed service instance.
///
/// Services are isolated by key. This means that there cannot be two concurrent
/// invocations for the same service instance (service name, key).
#[derive(Eq, Hash, PartialEq, PartialOrd, Ord, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ServiceId {
    /// Identifies the grpc service
    pub service_name: ByteString,
    /// Identifies the service instance for the given service name
    pub key: Bytes,

    partition_key: PartitionKey,
}

impl ServiceId {
    pub fn new(service_name: impl Into<ByteString>, key: impl Into<Bytes>) -> Self {
        let key = key.into();
        let partition_key = HashPartitioner::compute_partition_key(&key);
        Self::with_partition_key(partition_key, service_name, key)
    }

    /// # Important
    /// The `partition_key` must be hash of the `key` computed via [`HashPartitioner`].
    pub fn with_partition_key(
        partition_key: PartitionKey,
        service_name: impl Into<ByteString>,
        key: impl Into<Bytes>,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            key: key.into(),
            partition_key,
        }
    }

    pub fn partition_key(&self) -> PartitionKey {
        self.partition_key
    }
}

/// Representing a service invocation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceInvocation {
    pub id: ServiceInvocationId,
    pub method_name: ByteString,
    pub argument: Bytes,
    pub response_sink: Option<ServiceInvocationResponseSink>,
    pub span_context: ServiceInvocationSpanContext,
}

impl ServiceInvocation {
    /// Create a new [`ServiceInvocation`].
    ///
    /// This method returns the [`Span`] associated to the created [`ServiceInvocation`].
    /// It is not required to keep this [`Span`] around for the whole lifecycle of the invocation.
    /// On the contrary, it is encouraged to drop it as soon as possible,
    /// to let the exporter commit this span to jaeger/zipkin to visualize intermediate results of the invocation.
    pub fn new(
        id: ServiceInvocationId,
        method_name: ByteString,
        argument: Bytes,
        response_sink: Option<ServiceInvocationResponseSink>,
        related_span: SpanRelation,
    ) -> (Self, Span) {
        let (span_context, span) =
            ServiceInvocationSpanContext::start(&id, &method_name, related_span);
        (
            Self {
                id,
                method_name,
                argument,
                response_sink,
                span_context,
            },
            span,
        )
    }
}

/// Representing a response for a caller
#[derive(Debug, Clone, PartialEq)]
pub struct InvocationResponse {
    pub id: ServiceInvocationId,
    pub entry_index: EntryIndex,
    pub result: ResponseResult,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseResult {
    Success(Bytes),
    Failure(UserErrorCode, ByteString),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub struct IngressId(pub std::net::SocketAddr);

/// Definition of the sink where to send the result of a service invocation.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ServiceInvocationResponseSink {
    /// The invocation has been created by a partition processor and is expecting a response.
    PartitionProcessor {
        caller: ServiceInvocationId,
        entry_index: EntryIndex,
    },
    /// The invocation has been generated by a request received at an ingress, and the client is expecting a response back.
    Ingress(IngressId),
}

/// This struct contains the relevant span information for a [`ServiceInvocation`].
/// It can be used to create related spans, such as child spans,
/// using [`ServiceInvocationSpanContext::as_cause`] or [`ServiceInvocationSpanContext::as_parent`].
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ServiceInvocationSpanContext(SpanContext);

impl ServiceInvocationSpanContext {
    pub fn new(span_context: SpanContext) -> Self {
        ServiceInvocationSpanContext(span_context)
    }

    pub fn empty() -> Self {
        ServiceInvocationSpanContext(SpanContext::empty_context())
    }

    /// See [`ServiceInvocation::new`] for more details.
    pub fn start(
        service_invocation_id: &ServiceInvocationId,
        method_name: &str,
        related_span: SpanRelation,
    ) -> (ServiceInvocationSpanContext, Span) {
        // Create the span
        let span = info_span!(
            "service_invocation",
            rpc.system = "restate",
            rpc.service = %service_invocation_id.service_id.service_name,
            rpc.method = method_name,
            restate.invocation.sid = %service_invocation_id);

        // Attach the related span.
        // Note: As it stands with tracing_opentelemetry 0.18 there seems to be
        // an ordering relationship between using OpenTelemetrySpanExt::context() and
        // OpenTelemetrySpanExt::set_parent().
        // If we invert the order, the spans won't link correctly because they'll have a different Trace ID.
        // This is the reason why this method gets a SpanRelation, rather than letting the caller
        // link the spans.
        // https://github.com/tokio-rs/tracing/issues/2520
        related_span.attach_to_span(&span);

        // Retrieve the OTEL SpanContext we want to propagate
        let span_context = span.context().span().span_context().clone();

        (ServiceInvocationSpanContext(span_context), span)
    }

    pub fn as_cause(&self) -> SpanRelation {
        SpanRelation::CausedBy(self.0.clone())
    }

    pub fn as_parent(&self) -> SpanRelation {
        SpanRelation::Parent(self.0.clone())
    }
}

impl From<ServiceInvocationSpanContext> for SpanContext {
    fn from(value: ServiceInvocationSpanContext) -> Self {
        value.0
    }
}

/// Span relation, used to propagate tracing contexts.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SpanRelation {
    None,
    Parent(SpanContext),
    CausedBy(SpanContext),
}

impl SpanRelation {
    /// Attach this [`SpanRelation`] to the given [`Span`]
    pub fn attach_to_span(self, span: &Span) {
        match self {
            SpanRelation::Parent(parent) => {
                span.set_parent(opentelemetry_api::Context::new().with_remote_span_context(parent))
            }
            SpanRelation::CausedBy(cause) => span.add_link(cause),
            _ => {}
        };
    }
}

/// Wrapper that extends a message with its target peer to which the message should be sent.
pub type PeerTarget<Msg> = (PeerId, Msg);

/// Index type used messages in the runtime
pub type MessageIndex = u64;

#[derive(Debug, Clone, Copy)]
pub enum AckKind {
    Acknowledge(MessageIndex),
    Duplicate {
        // Sequence number of the duplicate message.
        seq_number: MessageIndex,
        // Currently last known sequence number by the receiver for a producer.
        // See `DeduplicatingStateMachine` for more details.
        last_known_seq_number: MessageIndex,
    },
}

/// Milliseconds since the unix epoch
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MillisSinceEpoch(u64);

impl MillisSinceEpoch {
    pub const UNIX_EPOCH: MillisSinceEpoch = MillisSinceEpoch::new(0);
    pub const MAX: MillisSinceEpoch = MillisSinceEpoch::new(u64::MAX);

    pub const fn new(millis_since_epoch: u64) -> Self {
        MillisSinceEpoch(millis_since_epoch)
    }

    pub fn now() -> Self {
        SystemTime::now().into()
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for MillisSinceEpoch {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl From<SystemTime> for MillisSinceEpoch {
    fn from(value: SystemTime) -> Self {
        MillisSinceEpoch::new(
            u64::try_from(
                value
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("duration since Unix epoch should be well-defined")
                    .as_millis(),
            )
            .expect("millis since Unix epoch should fit in u64"),
        )
    }
}

impl From<MillisSinceEpoch> for SystemTime {
    fn from(value: MillisSinceEpoch) -> Self {
        SystemTime::UNIX_EPOCH.add(Duration::from_millis(value.as_u64()))
    }
}

impl Display for MillisSinceEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ms since epoch", self.0)
    }
}

/// Metadata associated with a journal
#[derive(Debug, Clone, PartialEq)]
pub struct JournalMetadata {
    pub length: EntryIndex,
    pub method: String,
    pub span_context: ServiceInvocationSpanContext,
}

impl JournalMetadata {
    pub fn new(
        method: impl Into<String>,
        span_context: ServiceInvocationSpanContext,
        length: EntryIndex,
    ) -> Self {
        Self {
            method: method.into(),
            span_context,
            length,
        }
    }
}

/// This struct represents a serialized journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEntry<H> {
    // TODO can we get rid of these pub here?
    pub header: H,
    pub entry: Bytes,
}

impl<H> RawEntry<H> {
    pub const fn new(header: H, entry: Bytes) -> Self {
        Self { header, entry }
    }

    pub fn into_inner(self) -> (H, Bytes) {
        (self.header, self.entry)
    }

    // TODO fn ty(&self) -> EntryType could be useful, but we need the header interface.
    //  Probably can be fixed with https://github.com/restatedev/restate/issues/420
}

/// Result of the target service resolution
#[derive(Debug, Clone)]
pub struct ResolutionResult {
    pub invocation_id: InvocationId,
    pub service_key: Bytes,
    // When resolving the service and generating its id, we also generate the associated span
    pub span_context: ServiceInvocationSpanContext,
}

/// Enriched variant of the journal headers to store additional runtime specific information
/// for the journal entries.
#[derive(Debug, Clone)]
pub enum EnrichedEntryHeader {
    PollInputStream {
        is_completed: bool,
    },
    OutputStream,
    GetState {
        is_completed: bool,
    },
    SetState,
    ClearState,
    Sleep {
        is_completed: bool,
    },
    Invoke {
        is_completed: bool,
        // None if invoke entry is completed by service endpoint
        resolution_result: Option<ResolutionResult>,
    },
    BackgroundInvoke {
        resolution_result: ResolutionResult,
    },
    Awakeable {
        is_completed: bool,
    },
    CompleteAwakeable,
    Custom {
        code: u16,
        requires_ack: bool,
    },
}

pub type EnrichedRawEntry = RawEntry<EnrichedEntryHeader>;

// TODO From<EnrichedRawEntry> for PlainRawEntry can be useful
//  Probably can be fixed with https://github.com/restatedev/restate/issues/420

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionResult {
    Ack,
    Empty,
    Success(Bytes),
    Failure(UserErrorCode, ByteString),
}

impl From<ResponseResult> for CompletionResult {
    fn from(value: ResponseResult) -> Self {
        match value {
            ResponseResult::Success(bytes) => CompletionResult::Success(bytes),
            ResponseResult::Failure(error_code, error_msg) => {
                CompletionResult::Failure(error_code, error_msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod service_invocation_id {
        use super::*;

        macro_rules! roundtrip_test {
            ($test_name:ident, $sid:expr) => {
                mod $test_name {
                    use super::*;

                    #[test]
                    fn roundtrip() {
                        let expected_sid = $sid;
                        let opaque_sid: String = expected_sid.to_string();
                        let actual_sid: ServiceInvocationId = opaque_sid.parse().unwrap();
                        assert_eq!(expected_sid, actual_sid);
                    }
                }
            };
        }

        roundtrip_test!(
            keyed_service_with_hyphens,
            ServiceInvocationId::new(
                "my.example.Service",
                "-------stuff------".as_bytes().to_vec(),
                Uuid::now_v7(),
            )
        );
        roundtrip_test!(
            unkeyed_service,
            ServiceInvocationId::new(
                "my.example.Service",
                Uuid::now_v7().as_bytes().to_vec(),
                Uuid::now_v7(),
            )
        );
        roundtrip_test!(
            empty_key,
            ServiceInvocationId::new("my.example.Service", "".as_bytes().to_vec(), Uuid::now_v7(),)
        );
    }

    mod time {
        use crate::types::MillisSinceEpoch;
        use std::time::SystemTime;

        #[test]
        fn millis_should_not_overflow() {
            let t: SystemTime = MillisSinceEpoch::new(u64::MAX).into();
            println!("{:?}", t);
        }
    }
}
