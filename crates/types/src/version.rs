// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_encoding::NetSerde;

/// A type used for versioned metadata.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
    derive_more::Debug,
    derive_more::AddAssign,
    serde::Serialize,
    serde::Deserialize,
    restate_encoding::BilrostNewType,
    NetSerde,
)]
#[display("v{}", _0)]
#[debug("v{}", _0)]
pub struct Version(u32);

impl Version {
    pub const INVALID: Version = Version(0);
    pub const MIN: Version = Version(1);

    pub fn next(self) -> Self {
        Version(self.0 + 1)
    }

    #[cfg(feature = "test-util")]
    pub fn prev(self) -> Self {
        Version(self.0.saturating_sub(1))
    }

    pub fn invalid() -> Self {
        Version::INVALID
    }
}

impl From<crate::protobuf::common::Version> for Version {
    fn from(version: crate::protobuf::common::Version) -> Self {
        crate::Version::from(version.value)
    }
}

impl From<Version> for crate::protobuf::common::Version {
    fn from(version: Version) -> Self {
        crate::protobuf::common::Version {
            value: version.into(),
        }
    }
}

/// A trait for all metadata types that have a version.
pub trait Versioned {
    /// Returns the version of the versioned value
    fn version(&self) -> Version;

    /// Is this a valid version?
    fn valid(&self) -> bool {
        self.version() >= Version::MIN
    }
}

impl<T: Versioned> Versioned for &T {
    fn version(&self) -> Version {
        (**self).version()
    }
}

impl<T: Versioned> Versioned for &mut T {
    fn version(&self) -> Version {
        (**self).version()
    }
}

impl<T: Versioned> Versioned for Box<T> {
    fn version(&self) -> Version {
        (**self).version()
    }
}
