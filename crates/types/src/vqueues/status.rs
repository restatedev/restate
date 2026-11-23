// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Processing status. Native ordering follows the stored discriminants; it is
/// neither a lifecycle progression nor the ordering of the SQL display names.
#[derive(
    Debug,
    strum::Display,
    strum::EnumCount,
    strum::FromRepr,
    strum::VariantArray,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    bilrost::Enumeration,
)]
#[repr(u8)]
#[strum(serialize_all = "kebab-case")]
pub enum Status {
    #[bilrost(0)]
    Unknown = 0,
    #[bilrost(1)]
    New = 1,
    #[bilrost(2)]
    Scheduled = 2,
    /// Invocation has started running with at least one attempt.
    #[bilrost(3)]
    Started = 3,
    /// Invocation has previously started but has been placed back on the inbox stage
    /// due to an attempt error.
    #[bilrost(4)]
    BackingOff = 4,
    /// Invocation has previously started but has been placed back on the inbox stage.
    /// This does not mean that the invocation attempt has failed, it just means that
    /// it has been evicted from the run queue and will be resumed later.
    #[bilrost(5)]
    Yielded = 5,
    ///
    /// -- Terminal states, invocation cannot transition back to any of the previous
    /// statuses
    ///
    #[bilrost(6)]
    Killed = 6,
    #[bilrost(7)]
    Cancelled = 7,
    #[bilrost(8)]
    Failed = 8,
    #[bilrost(9)]
    Succeeded = 9,
}

impl Status {
    /// Every stored status but the `Unknown` sentinel, which decoding produces
    /// for a status this binary doesn't know: the statuses a query can name.
    pub const VALUES: [Status; 9] = [
        Status::New,
        Status::Scheduled,
        Status::Started,
        Status::BackingOff,
        Status::Yielded,
        Status::Killed,
        Status::Cancelled,
        Status::Failed,
        Status::Succeeded,
    ];

    /// Its name, or none for the `Unknown` sentinel, which is no value.
    pub const fn name(self) -> Option<&'static str> {
        match self {
            Status::Unknown => None,
            status => Some(status.as_str()),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Status::Unknown => "unknown",
            Status::New => "new",
            Status::Scheduled => "scheduled",
            Status::Started => "started",
            Status::BackingOff => "backing-off",
            Status::Yielded => "yielded",
            Status::Killed => "killed",
            Status::Cancelled => "cancelled",
            Status::Failed => "failed",
            Status::Succeeded => "succeeded",
        }
    }
}

mod bilrost_encoding {
    use bilrost::encoding::{DistinguishedProxiable, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind, Enumeration};

    use super::Status;

    impl Proxiable for Status {
        type Proxy = u32;

        fn encode_proxy(&self) -> Self::Proxy {
            <Status as Enumeration>::to_number(self)
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = <Status as Enumeration>::try_from_number(proxy).unwrap_or(Status::Unknown);
            Ok(())
        }
    }

    impl DistinguishedProxiable for Status {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonicity::Canonical)
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Fixed)
        to encode proxied type (Status)
        with encoding (bilrost::encoding::Fixed)
        including distinguished
    );
}
