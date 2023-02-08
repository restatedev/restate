use bytes::Bytes;
use bytestring::ByteString;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
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

/// Discriminator for invocation instances
pub type InvocationId = Uuid;

/// Id of a single service invocation.
///
/// A service invocation id is composed of a [`ServiceId`] and an [`InvocationId`]
/// that makes the id unique.
#[derive(Eq, Hash, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct ServiceInvocationId {
    /// Identifies the invoked service
    pub service_id: ServiceId,
    /// Uniquely identifies this invocation instance
    pub invocation_id: InvocationId,
}

impl Display for ServiceInvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{:?}]({})",
            self.service_id.service_name, self.service_id.key, self.invocation_id
        )
    }
}

impl ServiceInvocationId {
    pub fn new(
        service_name: impl Into<ByteString>,
        key: impl Into<Bytes>,
        invocation_id: impl Into<InvocationId>,
    ) -> Self {
        Self {
            service_id: ServiceId {
                service_name: service_name.into(),
                key: key.into(),
            },
            invocation_id: invocation_id.into(),
        }
    }
}

/// Id of a keyed service instance.
///
/// Services are isolated by key. This means that there cannot be two concurrent
/// invocations for the same service instance (service name, key).
#[derive(Eq, Hash, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct ServiceId {
    /// Identifies the grpc service
    pub service_name: ByteString,
    /// Identifies the service instance for the given service name
    pub key: Bytes,
}

impl ServiceId {
    pub fn new(service_name: impl Into<ByteString>, key: impl Into<Bytes>) -> Self {
        Self {
            service_name: service_name.into(),
            key: key.into(),
        }
    }
}

/// Representing a service invocation
#[derive(Debug)]
pub struct ServiceInvocation {
    pub id: ServiceInvocationId,
    pub method_name: ByteString,
    pub argument: Bytes,
}

/// Representing a response for a caller
#[derive(Debug)]
pub struct Response {
    pub id: ServiceInvocationId,
    pub entry_index: EntryIndex,
    pub result: ResponseResult,
}

#[derive(Debug)]
pub enum ResponseResult {
    Success(Bytes),
    Failure(i32, ByteString),
}
