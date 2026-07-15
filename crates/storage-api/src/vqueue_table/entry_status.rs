// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::RoughTimestamp;
use restate_types::vqueues::{EntryId, EntryKind, Seq, VQueueId, VQueueIdRef};

use super::stats::EntryStatistics;
use super::{EntryKey, EntryMetadata, EntryMetadataRef, Stage};

#[derive(Debug, strum::Display, Clone, Copy, Eq, PartialEq, bilrost::Enumeration)]
#[strum(serialize_all = "kebab-case")]
pub enum Status {
    #[bilrost(0)]
    Unknown,
    #[bilrost(1)]
    New,
    #[bilrost(2)]
    Scheduled,
    /// Invocation has started running with at least one attempt.
    #[bilrost(3)]
    Started,
    /// Invocation has previously started but has been placed back on the inbox stage
    /// due to an attempt error.
    #[bilrost(4)]
    BackingOff,
    /// Invocation has previously started but has been placed back on the inbox stage.
    /// This does not mean that the invocation attempt has failed, it just means that
    /// it has been evicted from the run queue and will be resumed later.
    #[bilrost(5)]
    Yielded,
    ///
    /// -- Terminal states, invocation cannot transition back to any of the previous
    /// statuses
    ///
    #[bilrost(6)]
    Killed,
    #[bilrost(7)]
    Cancelled,
    #[bilrost(8)]
    Failed,
    #[bilrost(9)]
    Succeeded,
}

/// Borrowing version of [`RawStatusHeader`].
///
/// NOTE: keep in-sync with [`RawStatusHeader`]
#[derive(Clone, bilrost::Message)]
pub struct RawStatusHeaderRef<'a> {
    #[bilrost(tag(1))]
    pub qid: VQueueIdRef<'a>,
    #[bilrost(tag(2))]
    pub stage: Stage,
    #[bilrost(tag(3))]
    pub has_lock: bool,
    #[bilrost(tag(4))]
    pub next_run_at: RoughTimestamp,
    #[bilrost(tag(5))]
    pub seq: Seq,
    #[bilrost(tag(6))]
    pub metadata: EntryMetadataRef<'a>,
    // Not borrowed because it's full of numeric values
    #[bilrost(tag(7))]
    pub stats: EntryStatistics,
    #[bilrost(tag(8))]
    pub status: Status,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct RawStatusHeader {
    #[bilrost(tag(1))]
    pub qid: VQueueId,
    #[bilrost(tag(2))]
    pub stage: Stage,
    #[bilrost(tag(3))]
    pub has_lock: bool,
    #[bilrost(tag(4))]
    pub next_run_at: RoughTimestamp,
    #[bilrost(tag(5))]
    pub seq: Seq,
    /// Entry metadata is lightweight metadata and/or resource information that
    /// is copied from entry state to the vqueue's inbox entry on each transition.
    #[bilrost(tag(6))]
    pub metadata: EntryMetadata,
    #[bilrost(tag(7))]
    pub stats: EntryStatistics,
    #[bilrost(tag(8))]
    pub status: Status,
}

/// Owned vqueue entry status header.
#[derive(Debug, Clone)]
pub struct OwnedEntryStatusHeader {
    qid: VQueueId,
    stage: Stage,
    entry_key: EntryKey,
    metadata: EntryMetadata,
    status: Status,
    stats: EntryStatistics,
}

impl OwnedEntryStatusHeader {
    pub fn new(
        qid: VQueueId,
        stage: Stage,
        entry_key: EntryKey,
        metadata: EntryMetadata,
        stats: EntryStatistics,
        status: Status,
    ) -> Self {
        Self {
            qid,
            stage,
            entry_key,
            metadata,
            status,
            stats,
        }
    }
}

impl EntryStatusHeader for OwnedEntryStatusHeader {
    #[inline]
    fn vqueue_id(&self) -> &VQueueId {
        &self.qid
    }

    #[inline]
    fn entry_id(&self) -> &EntryId {
        self.entry_key.entry_id()
    }

    #[inline]
    fn entry_key(&self) -> &EntryKey {
        &self.entry_key
    }

    #[inline]
    fn kind(&self) -> EntryKind {
        self.entry_key.kind()
    }

    #[inline]
    fn stage(&self) -> Stage {
        self.stage
    }

    #[inline]
    fn seq(&self) -> Seq {
        self.entry_key.seq()
    }

    #[inline]
    fn has_lock(&self) -> bool {
        self.entry_key.has_lock()
    }

    #[inline]
    fn next_run_at(&self) -> RoughTimestamp {
        self.entry_key.run_at()
    }

    #[inline]
    fn stats(&self) -> &EntryStatistics {
        &self.stats
    }

    #[inline]
    fn metadata(&self) -> &EntryMetadata {
        &self.metadata
    }

    #[inline]
    fn display_entry_id(&self) -> impl std::fmt::Display + '_ {
        self.entry_id().display(self.qid.partition_key())
    }

    #[inline]
    fn status(&self) -> Status {
        self.status
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

pub trait EntryStatusHeader: std::fmt::Debug {
    fn vqueue_id(&self) -> &VQueueId;
    fn status(&self) -> Status;
    fn entry_id(&self) -> &EntryId;
    fn entry_key(&self) -> &EntryKey;
    fn kind(&self) -> EntryKind;
    fn metadata(&self) -> &EntryMetadata;
    fn stage(&self) -> Stage;
    fn has_lock(&self) -> bool;
    fn next_run_at(&self) -> RoughTimestamp;
    fn seq(&self) -> Seq;
    fn stats(&self) -> &EntryStatistics;
    fn display_entry_id(&self) -> impl std::fmt::Display + '_;
    /// Returns new if this entry has not started yet.
    fn has_started(&self) -> bool {
        self.stats().num_attempts > 0
    }
    /// Returns true if this entry is in the terminal state and cannot transition
    /// out of it.
    fn is_terminal(&self) -> bool {
        if matches!(self.stage(), Stage::Finished) {
            return true;
        }
        false
    }
}

/// For future support for extra state storage for entries.
pub trait LazyEntryStatus: EntryStatusHeader {
    fn header(&self) -> &impl EntryStatusHeader;
    fn into_header(self) -> impl EntryStatusHeader + Send + Sync + 'static;

    fn decode_state_owned<E>(&self) -> Option<E>
    where
        E: EntryStatusExtra + bilrost::OwnedMessage + Send + Sized + 'static;

    fn decode_state_borrowed<'b, E>(&'b self) -> Option<E>
    where
        E: EntryStatusExtra + bilrost::BorrowedMessage<'b> + Sized + Send;
}

/// A marker trait for types that can be used as entry extra state values.
pub trait EntryStatusExtra {}

#[cfg(test)]
mod tests {
    use bilrost::{Message, OwnedMessage};

    use super::*;

    #[test]
    fn fixed_encoding_round_trips_status() {
        #[derive(Debug, PartialEq, bilrost::Message)]
        struct EncodedStatus {
            #[bilrost(tag(1), encoding(fixed))]
            value: Status,
        }

        let value = EncodedStatus {
            value: Status::Succeeded,
        };
        let encoded = value.encode_to_bytes();

        assert_eq!(encoded.as_ref(), &[0x06, 9, 0, 0, 0]);
        assert_eq!(EncodedStatus::decode(encoded).unwrap(), value);
    }
}
