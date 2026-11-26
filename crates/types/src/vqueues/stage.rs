// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Stages in the inbox/vqueue
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    bilrost::Enumeration,
    enum_map::Enum,
    strum::EnumCount,
    strum::FromRepr,
    strum::Display,
    strum::VariantArray,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(u8)]
#[strum(serialize_all = "kebab-case")]
pub enum Stage {
    #[bilrost(0)]
    Unknown = 0,
    /// Holds entries that are waiting for the scheduler to pick and move to `Run`
    #[bilrost(1)]
    Inbox = b'i',
    /// Holds entries that are currently assumed to be running
    #[bilrost(2)]
    Running = b'r',
    /// Holds entries that are suspended and will be woken up upon receiving a signal.
    #[bilrost(3)]
    Suspended = b's',
    /// Holds entries that are paused, either manually or due to errors.
    /// Paused entries can be resumed by moving them back to `Inbox`
    #[bilrost(4)]
    Paused = b'p',
    /// Items that are completed/finished/terminated. This is a terminal stage and
    /// items in this stage are allowed to be deleted/purged/archived either immediately
    /// or delayed.
    #[bilrost(5)]
    Finished = b'f',
}

impl Stage {
    pub const fn serialized_length_fixed() -> usize {
        std::mem::size_of::<Self>()
    }

    /// Every stage but the `Unknown` sentinel, which decoding produces for a
    /// stage this binary doesn't know: the stages a query can name.
    ///
    /// A new stage ships only behind a version barrier with a feature change,
    /// which raises the partition store's minimum version, so a binary never
    /// reads an entry in a stage it doesn't know. The sentinel is a safe default
    /// for decoding, not a state the stored data reaches; for example, queries
    /// read the status indexes, which hold no key for it, as holding every
    /// entry.
    pub const VALUES: [Stage; 5] = [
        Stage::Inbox,
        Stage::Running,
        Stage::Suspended,
        Stage::Paused,
        Stage::Finished,
    ];

    /// Its name, or none for the `Unknown` sentinel, which is no value.
    pub const fn name(self) -> Option<&'static str> {
        match self {
            Stage::Unknown => None,
            stage => Some(stage.as_str()),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Stage::Unknown => "unknown",
            Stage::Inbox => "inbox",
            Stage::Running => "running",
            Stage::Suspended => "suspended",
            Stage::Paused => "paused",
            Stage::Finished => "finished",
        }
    }
}

mod bilrost_encoding {
    use bilrost::encoding::{DistinguishedProxiable, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind, Enumeration};

    use super::Stage;

    impl Proxiable for Stage {
        type Proxy = u32;

        fn encode_proxy(&self) -> Self::Proxy {
            <Stage as Enumeration>::to_number(self)
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = <Stage as Enumeration>::try_from_number(proxy).unwrap_or(Stage::Unknown);
            Ok(())
        }
    }

    impl DistinguishedProxiable for Stage {
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
        to encode proxied type (Stage)
        with encoding (bilrost::encoding::Fixed)
        including distinguished
    );
}
