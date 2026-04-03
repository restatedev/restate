// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_util_string::ReString;

use crate::identifiers::partitioner::HashPartitioner;
use crate::identifiers::{PartitionKey, WithPartitionKey};
use crate::{Scope, ServiceName};

/// Errors returned when parsing a [`LockName`] from a string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("malformed lock name: {0}")]
    Malformed(ReString),
    #[error("lock name cannot be empty")]
    Empty,
    #[error("lock name must have a non-empty key: {0}")]
    EmptyKey(ReString),
    #[error("lock name must have a non-empty service name: {0}")]
    EmptyServiceName(ReString),
}

/// A type that represents a locking key for a virtual object or a workflow.
#[derive(
    Clone, derive_more::Debug, derive_more::Display, Hash, Eq, PartialEq, bilrost::Message,
)]
#[debug("{service_name}/{key}")]
#[display("{service_name}/{key}")]
pub struct LockName {
    service_name: ServiceName,
    key: ReString,
}

impl LockName {
    /// Creates a new lock name from pre-validated components.
    pub const fn new(service_name: ServiceName, key: ReString) -> Self {
        Self { service_name, key }
    }

    /// Parses a `"service_name/key"` string into a [`LockName`].
    ///
    /// Both the service name and key segments must be non-empty.
    pub fn parse(data: &str) -> Result<Self, ParseError> {
        let mut it = data.match_indices('/');
        let Some((key_offset, _)) = it.next() else {
            return Err(ParseError::Malformed(data.into()));
        };
        let service_name = &data[..key_offset];
        if service_name.is_empty() {
            return Err(ParseError::EmptyServiceName(data.into()));
        }
        let key = &data[(key_offset + 1)..];
        if key.is_empty() {
            return Err(ParseError::EmptyKey(data.into()));
        }
        Ok(Self {
            service_name: ServiceName::new(service_name),
            key: ReString::new_owned(key),
        })
    }

    /// Returns the key segment of this lock name.
    #[inline]
    pub const fn key(&self) -> &ReString {
        &self.key
    }

    /// Returns the service name segment of this lock name.
    #[inline]
    pub const fn service_name(&self) -> &ServiceName {
        &self.service_name
    }
}

/// A borrowed view over a [`LockName`] combined with an optional [`Scope`].
///
/// Used to compute the partition key and to produce a canonical display form:
/// scoped locks render as `s/<scope>/<service>/<key>`, unscoped as `u/<service>/<key>`.
pub struct CanonicalLockId<'a> {
    pub scope: &'a Option<Scope>,
    pub lock_name: &'a LockName,
}

impl<'a> WithPartitionKey for CanonicalLockId<'a> {
    fn partition_key(&self) -> PartitionKey {
        if let Some(scope) = self.scope {
            // the partition key is calculated from the scope only
            HashPartitioner::compute_partition_key(scope)
        } else {
            // for backward compatibility, we use the "key" only as the source for
            // calculating the partition key
            HashPartitioner::compute_partition_key(self.lock_name.key())
        }
    }
}

impl<'a> std::fmt::Display for CanonicalLockId<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(scope) = self.scope {
            write!(f, "s/{scope}/{}", self.lock_name)
        } else {
            write!(f, "u/{}", self.lock_name)
        }
    }
}

#[cfg(test)]
mod tests {
    use bytestring::ByteString;

    use super::*;

    #[test]
    fn parse_lock_name() {
        let lock = LockName::parse("my-service/my-key").unwrap();
        assert_eq!(lock.service_name().to_string(), "my-service");
        assert_eq!(lock.key().to_string(), "my-key");
    }

    #[test]
    fn parse_lock_name_errors() {
        assert!(LockName::parse("no-slash").is_err());
        assert!(LockName::parse("").is_err());
        assert!(LockName::parse("/empty-service").is_err());
        assert!(LockName::parse("empty-key/").is_err());
    }

    #[test]
    fn lock_name_display() {
        let lock = LockName::parse("my-service/my-key").unwrap();
        assert_eq!(lock.to_string(), "my-service/my-key");
    }

    #[test]
    fn canonical_lock_id_display() {
        let lock = LockName::parse("my-service/my-key").unwrap();

        let scope: Option<Scope> = Some(Scope::new("my-scope"));
        let scoped = CanonicalLockId {
            scope: &scope,
            lock_name: &lock,
        };
        assert_eq!(scoped.to_string(), "s/my-scope/my-service/my-key");

        let no_scope: Option<Scope> = None;
        let unscoped = CanonicalLockId {
            scope: &no_scope,
            lock_name: &lock,
        };
        assert_eq!(unscoped.to_string(), "u/my-service/my-key");
    }

    #[test]
    fn unscoped_partition_key_matches_bytestring_key() {
        let lock = LockName::parse("my-service/my-key").unwrap();
        let no_scope: Option<Scope> = None;
        let canonical = CanonicalLockId {
            scope: &no_scope,
            lock_name: &lock,
        };
        let key = ByteString::from("my-key");
        assert_eq!(
            canonical.partition_key(),
            HashPartitioner::compute_partition_key(&key),
        );
    }

    #[test]
    fn scoped_partition_key_uses_scope() {
        let lock = LockName::parse("my-service/my-key").unwrap();
        let scope: Option<Scope> = Some(Scope::new("my-scope"));
        let canonical = CanonicalLockId {
            scope: &scope,
            lock_name: &lock,
        };
        assert_eq!(
            canonical.partition_key(),
            HashPartitioner::compute_partition_key(Scope::new("my-scope")),
        );
    }
}
