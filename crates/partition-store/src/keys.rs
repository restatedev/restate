// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Allow deprecated KeyKind variants - needed because derive macros (EnumIter, VariantArray)
// generate code that references all variants including deprecated ones.
#![allow(deprecated)]

use std::mem;

use anyhow::anyhow;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use prost::encoding::encoded_len_varint;
use restate_types::ServiceName;
use restate_util_string::ReString;
use rocksdb::MergeOperands;
use strum::EnumIter;
use tracing::{error, trace};

use restate_types::clock::UniqueTimestamp;

/// Every table key needs to have a key kind. This allows to multiplex different keys in the same
/// column family and to evolve a key if necessary.
///
/// # Important
/// There must exist a bijective mapping between the enum variant and its byte representation.
/// See [`KeyKind::as_bytes`] and [`KeyKind::from_bytes`].
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, EnumIter, derive_more::Display, strum::VariantArray,
)]
pub enum KeyKind {
    Deduplication,
    Fsm,
    /// Retired in v1.7 - kept to reserve the `b"ip"` byte encoding.
    #[deprecated(note = "Idempotency was retired in v1.7 and should not be used")]
    Idempotency,
    Inbox,
    /// Retired in 1.6 - kept to reserve the `b"is"` byte encoding.
    #[deprecated(note = "InvocationStatusV1 was retired in 1.6 and should not be used")]
    InvocationStatusV1,
    InvocationStatus,
    Journal,
    JournalV2,
    JournalV2NotificationIdToNotificationIndex,
    JournalV2CompletionIdToCommandIndex,
    JournalEvent,
    Outbox,
    ServiceStatus,
    State,
    /// Scoped variant of State with scope after partition_key.
    /// Supports empty scope for future migration of unscoped entries.
    ScopedState,
    Timers,
    Promise,
    /// Scoped variant of Promise with scope after partition_key.
    /// Supports empty scope for future migration of unscoped entries.
    ScopedPromise,
    // # VQueues --> owned by restate-vqueues
    //
    // todo: split this into empty and non-empty, or add the status in the key prefix
    // for instance, make this VQueueStatus (S | VQueueId)
    // or have empty_vqueues that carry the empty_since in their key prefix. Note that
    // doing so would require that we know the empty_since when we attempt to delete it
    VQueueActive,
    VQueueMeta,
    // Inbox stage management keys
    VQueueInboxStage,
    VQueueRunningStage,
    VQueueSuspendedStage,
    VQueuePausedStage,
    VQueueFinishedStage,
    // Resources' status key
    VQueueEntryStatus,
    // Input payloads stored in vqueues (e.g. state mutations, invocations, etc.)
    VQueueInput,
    // # Locks
    // locks for scoped and unscoped virtual objects and workflows
    Lock,
}

impl KeyKind {
    pub const SERIALIZED_LENGTH: usize = 2;

    pub const fn exclusive_upper_bound(&self) -> [u8; Self::SERIALIZED_LENGTH] {
        let start = self.as_bytes();
        let num = u16::from_be_bytes(*start);
        let num = if num == u16::MAX {
            panic!("key kind to not saturate u16");
        } else {
            num + 1
        };
        num.to_be_bytes()
    }

    /// A once assigned byte representation to a key kind variant must never be changed! Instead,
    /// create a new variant representing a new key.
    ///
    /// # Important
    /// The following invariant must hold:
    /// ```ignore
    /// KeyKind::from_bytes(key_kind.as_bytes()) == key_kind
    /// ```
    pub const fn as_bytes(&self) -> &'static [u8; Self::SERIALIZED_LENGTH] {
        // NOTE: do not use &[0xff, 0xff] as key byte prefix, ever!
        // We should always be able to +1 the those bytes when interpreted as u16
        match self {
            KeyKind::Deduplication => b"de",
            KeyKind::Fsm => b"fs",
            KeyKind::Idempotency => b"ip",
            KeyKind::Inbox => b"ib",
            KeyKind::InvocationStatusV1 => b"is",
            KeyKind::InvocationStatus => b"iS",
            KeyKind::Journal => b"jo",
            KeyKind::JournalV2NotificationIdToNotificationIndex => b"jn",
            KeyKind::JournalV2CompletionIdToCommandIndex => b"jc",
            KeyKind::JournalV2 => b"j2",
            KeyKind::JournalEvent => b"je",
            // ** Locks ** //
            KeyKind::Lock => b"lo",

            KeyKind::Outbox => b"ob",
            KeyKind::ServiceStatus => b"ss",
            KeyKind::State => b"st",
            KeyKind::ScopedState => b"sS",
            KeyKind::Timers => b"ti",
            KeyKind::Promise => b"pr",
            KeyKind::ScopedPromise => b"sP",
            // ** VQueues ** //
            // VQueues own all keys that start with b"q".
            KeyKind::VQueueActive => b"qa",
            KeyKind::VQueueMeta => b"qm",
            // Input payloads stored for vqueue items
            KeyKind::VQueueInput => b"qi",
            // Queue Entry Status
            KeyKind::VQueueEntryStatus => b"qs",
            // Inbox stage management
            KeyKind::VQueueInboxStage => b"qI",
            KeyKind::VQueueRunningStage => b"qR",
            KeyKind::VQueueSuspendedStage => b"qS",
            KeyKind::VQueuePausedStage => b"qP",
            KeyKind::VQueueFinishedStage => b"qF",
        }
    }

    /// A once assigned byte representation to a key kind variant must never be changed! Instead,
    /// create a new variant representing a new key.
    ///
    /// # Important
    /// The following invariant must hold:
    /// ```ignore
    /// KeyKind::from_bytes(key_kind.as_bytes()) == key_kind
    /// ```
    pub const fn from_bytes(bytes: &[u8; Self::SERIALIZED_LENGTH]) -> Option<Self> {
        match bytes {
            b"de" => Some(KeyKind::Deduplication),
            b"fs" => Some(KeyKind::Fsm),
            b"ip" => Some(KeyKind::Idempotency),
            b"ib" => Some(KeyKind::Inbox),
            b"is" => Some(KeyKind::InvocationStatusV1),
            b"iS" => Some(KeyKind::InvocationStatus),
            b"jo" => Some(KeyKind::Journal),
            b"j2" => Some(KeyKind::JournalV2),
            b"je" => Some(KeyKind::JournalEvent),
            b"jn" => Some(KeyKind::JournalV2NotificationIdToNotificationIndex),
            b"jc" => Some(KeyKind::JournalV2CompletionIdToCommandIndex),
            b"lo" => Some(KeyKind::Lock),
            b"ob" => Some(KeyKind::Outbox),
            b"ss" => Some(KeyKind::ServiceStatus),
            b"st" => Some(KeyKind::State),
            b"sS" => Some(KeyKind::ScopedState),
            b"ti" => Some(KeyKind::Timers),
            b"pr" => Some(KeyKind::Promise),
            b"sP" => Some(KeyKind::ScopedPromise),
            // VQueues own all keys that start with b"q"
            b"qa" => Some(KeyKind::VQueueActive),
            b"qi" => Some(KeyKind::VQueueInput),
            b"qm" => Some(KeyKind::VQueueMeta),
            b"qs" => Some(KeyKind::VQueueEntryStatus),
            // VQueue stage management (second char is always uppercase)
            b"qI" => Some(KeyKind::VQueueInboxStage),
            b"qR" => Some(KeyKind::VQueueRunningStage),
            b"qS" => Some(KeyKind::VQueueSuspendedStage),
            b"qP" => Some(KeyKind::VQueuePausedStage),
            b"qF" => Some(KeyKind::VQueueFinishedStage),
            _ => None,
        }
    }

    pub fn serialize<B: BufMut>(&self, buf: &mut B) {
        let bytes = self.as_bytes();
        buf.put_slice(bytes);
    }

    pub fn deserialize<B: Buf>(buf: &mut B) -> Result<Self, StorageError> {
        if buf.remaining() < KeyKind::SERIALIZED_LENGTH {
            return Err(StorageError::DataIntegrityError);
        }

        let mut bytes = [0; KeyKind::SERIALIZED_LENGTH];
        buf.copy_to_slice(&mut bytes);
        Self::from_bytes(&bytes)
            .ok_or_else(|| StorageError::Generic(anyhow::anyhow!("unknown key kind: {:x?}", bytes)))
    }

    // Rocksdb merge operator function (full merge)
    pub fn full_merge(
        key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut kind_buf = key;
        let kind = match KeyKind::deserialize(&mut kind_buf) {
            Ok(kind) => kind,
            Err(e) => {
                error!("Cannot apply merge operator; {e}");
                return None;
            }
        };
        trace!(?kind, "full merge");

        match kind {
            KeyKind::VQueueMeta => vqueue_meta_merge::full_merge(key, existing_val, operands),
            _ => None,
        }
    }

    // Rocksdb merge operator function (partial merge)
    pub fn partial_merge(
        key: &[u8],
        _unused: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut kind_buf = key;
        let kind = match KeyKind::deserialize(&mut kind_buf) {
            Ok(kind) => kind,
            Err(e) => {
                error!("Cannot apply merge operator; {e}");
                return None;
            }
        };
        trace!(?kind, "partial merge");

        match kind {
            KeyKind::VQueueMeta => vqueue_meta_merge::partial_merge(key, operands),
            _ => None,
        }
    }
}

/// Types that can be encoded to a full table key in partition store.
pub trait EncodeTableKey {
    const TABLE: TableKind;
    const KEY_KIND: KeyKind;

    fn serialize_to<B: BufMut>(&self, bytes: &mut B);
    fn serialized_length(&self) -> usize;
}

/// Types that can be decoded to an owned table key.
pub trait DecodeTableKey: Sized + std::fmt::Debug + Send + 'static {
    fn deserialize_from<B: Buf>(bytes: &mut B) -> crate::Result<Self>;
}

/// Types that can be encoded to perform a scan prefix in partition store.
pub trait EncodeTableKeyPrefix {
    const TABLE: TableKind;
    const KEY_KIND: KeyKind;

    fn serialize_to<B: BufMut>(&self, bytes: &mut B);
    fn serialize_key_kind<B: bytes::BufMut>(bytes: &mut B);

    fn serialize(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.serialized_length());
        self.serialize_to(&mut buf);
        buf
    }

    fn serialized_length(&self) -> usize;
}

/// Types that can be encoded to full keys are safe to be used for prefix scans.
impl<T> EncodeTableKeyPrefix for T
where
    T: EncodeTableKey,
{
    const TABLE: TableKind = T::TABLE;
    const KEY_KIND: KeyKind = T::KEY_KIND;

    #[inline]
    fn serialize_key_kind<B: bytes::BufMut>(bytes: &mut B) {
        Self::KEY_KIND.serialize(bytes);
    }

    #[inline]
    fn serialize_to<B: bytes::BufMut>(&self, bytes: &mut B) {
        EncodeTableKey::serialize_to(self, bytes);
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        EncodeTableKey::serialized_length(self)
    }
}

/// The following macro defines an ordered, named key tuple, that is used as a rocksdb key.
///
/// Given the following definition
/// ```ignore
/// define_table_key!(FooBarTable, KeyKind::Foobar, FooBarKey(foo: u32, bar: Bytes));
/// ```
///
/// This macro expands and generates two types. A concrete key type and a prefix builder
/// that can be used in iterators.
/// ```ignore
/// use bytes::{Buf, BufMut, Bytes};
/// use restate_partition_store::TableKind;
/// #[derive(Debug, Eq, PartialEq)]
/// pub struct FooBarKey {
///     pub foo: u32,
///     pub bar: Bytes,
/// }
///
/// #[derive(Default, Debug, Eq, PartialEq)]
/// pub struct FooBarKeyBuilder {
///     pub foo: Option<u32>,
///     pub bar: Option<Bytes>,
/// }
///```
///
macro_rules! define_table_key {

    ($table_kind:expr, $key_kind:path, $key_name:ident ( $($element: ident: $ty: ty),+ $(,)? ) ) => (paste::paste! {
        // key builder by holding references
        #[derive(Default, Debug, Eq, PartialEq, Clone)]
        pub struct [< $key_name BuilderRef >]<'a> { $(pub $element: Option<&'a $ty>),+ }

        // prefix builder impl by references
        #[allow(dead_code)]
        impl<'a> [< $key_name BuilderRef >]<'a> {
            $(pub fn $element(mut self, $element: &'a $ty) -> Self {
                self.$element = Some($element);
                self
            })+

            fn is_complete(&self) -> bool {
                $(
                if self.$element.is_none() {
                    return false;
                }
                )+
                return true;
            }

            /// Converts this prefix builder into a complete key if all the required fields are
            /// set.
            pub fn into_complete(self) -> crate::Result<[< $key_name Ref >] <'a>> {
                if !self.is_complete() {
                    return Err(restate_storage_api::StorageError::DataIntegrityError);
                }
                Ok([< $key_name Ref >] {
                    $($element: self.$element.unwrap(),)+
                })
            }
        }

        impl<'a> crate::keys::EncodeTableKeyPrefix for [<$key_name BuilderRef>]<'a> {
            const TABLE: crate::TableKind = $table_kind;
            const KEY_KIND: $crate::keys::KeyKind = $key_kind;

            #[inline]
            fn serialize_key_kind<B: bytes::BufMut>(bytes: &mut B) {
                $key_kind.serialize(bytes);
            }

            #[inline]
            fn serialize_to<B: bytes::BufMut>(&self, bytes: &mut B) {
                $key_kind.serialize(bytes);
                $(
                    if let Some(v) = &self.$element {
                        $crate::keys::serialize(v, bytes);
                    } else {
                        // Stop at the first None since this is a prefix scan
                        return;
                    }
                )+
            }

            #[inline]
            fn serialized_length(&self) -> usize {
                // we always need space for the key kind
                let mut serialized_length = $crate::keys::KeyKind::SERIALIZED_LENGTH;
                $(
                    if let Some(v) = &self.$element {
                        serialized_length += $crate::keys::KeyEncode::serialized_length(v);
                    } else {
                        // Stop at the first None since this is a prefix scan
                        return serialized_length;
                    }
                )+
                serialized_length
            }

        }

        // main key holder builder
        #[derive(Default, Debug, Eq, PartialEq, Clone)]
        pub struct [< $key_name Builder >] { $(pub $element: Option<$ty>),+ }

        // prefix builder impl
        #[allow(dead_code)]
        impl [< $key_name Builder >] {
            $(pub fn $element(mut self, $element: $ty) -> Self {
                self.$element = Some($element);
                self
            })+

            fn is_complete(&self) -> bool {
                $(
                if self.$element.is_none() {
                    return false;
                }
                )+
                return true;
            }

            /// Converts this prefix builder into a complete key if all the required fields are
            /// set.
            pub fn into_complete(self) -> crate::Result<$key_name> {
                if !self.is_complete() {
                    return Err(restate_storage_api::StorageError::DataIntegrityError);
                }
                Ok($key_name {
                    $($element: self.$element.unwrap(),)+
                })
            }
        }

        #[derive(Debug, Eq, PartialEq, Clone)]
        pub struct [< $key_name Ref >]<'a> { $(pub $element: &'a $ty),+ }

        #[allow(dead_code)]
        impl<'a> [< $key_name Ref >]<'a> {
            pub fn builder<'b>() -> [< $key_name BuilderRef >]<'b> {
                [< $key_name BuilderRef >]::default()
            }

            $(pub fn $element(&self) -> &$ty {
                &self.$element
            })+

            /// Converts this key into a prefix builder.
            pub fn into_builder(self) -> [< $key_name BuilderRef >]<'a> {
                [< $key_name BuilderRef >] {
                    $($element: Some(self.$element),)+
                }
            }
        }

        #[derive(Debug, Eq, PartialEq, Clone)]
        pub struct $key_name { $(pub $element: $ty),+ }

        #[allow(dead_code)]
        impl $key_name {
            pub fn builder() -> [< $key_name Builder >] {
                [< $key_name Builder >]::default()
            }

            $(pub fn $element(&self) -> &$ty {
                &self.$element
            })+

            /// Splits this key into its constituent parts.
            pub fn split(self) -> ($($ty,)+) {
                 ( $(self.$element,)+ )
            }

            /// Converts this key into a prefix builder.
            pub fn into_builder(self) -> [< $key_name Builder >] {
                [< $key_name Builder >] {
                    $($element: Some(self.$element),)+
                }
            }
        }


        impl crate::keys::EncodeTableKeyPrefix for [<$key_name Builder>] {
            const TABLE: crate::TableKind = $table_kind;
            const KEY_KIND: $crate::keys::KeyKind = $key_kind;

            #[inline]
            fn serialize_key_kind<B: bytes::BufMut>(bytes: &mut B) {
                $key_kind.serialize(bytes);
            }

            #[inline]
            fn serialize_to<B: bytes::BufMut>(&self, bytes: &mut B) {
                $key_kind.serialize(bytes);
                $(
                    if let Some(v) = &self.$element {
                        $crate::keys::serialize(v, bytes);
                    } else {
                        // Stop at the first None since this is a prefix scan
                        return;
                    }
                )+
            }

            #[inline]
            fn serialized_length(&self) -> usize {
                // we always need space for the key kind
                let mut serialized_length = $crate::keys::KeyKind::SERIALIZED_LENGTH;
                $(
                    if let Some(v) = &self.$element {
                        serialized_length += $crate::keys::KeyEncode::serialized_length(v);
                    } else {
                        // Stop at the first None since this is a prefix scan
                        return serialized_length;
                    }
                )+
                serialized_length
            }

        }

        // serde
        impl<'a> crate::keys::EncodeTableKey for [< $key_name Ref >]<'a> {
            const TABLE: crate::TableKind = $table_kind;
            const KEY_KIND: $crate::keys::KeyKind = $key_kind;

            #[inline]
            fn serialize_to<B: bytes::BufMut>(&self, bytes: &mut B) {
                $key_kind.serialize(bytes);
                $(
                $crate::keys::serialize(&self.$element, bytes);
                )+
            }

            #[inline]
            fn serialized_length(&self) -> usize {
                // we always need space for the key kind
                let mut serialized_length = $crate::keys::KeyKind::SERIALIZED_LENGTH;
                $(
                    serialized_length += $crate::keys::KeyEncode::serialized_length(&self.$element);
                )+
                serialized_length
            }
        }

        // serde
        impl crate::keys::EncodeTableKey for $key_name {
            const TABLE: crate::TableKind = $table_kind;
            const KEY_KIND: $crate::keys::KeyKind = $key_kind;

            #[inline]
            fn serialize_to<B: bytes::BufMut>(&self, bytes: &mut B) {
                $key_kind.serialize(bytes);
                $(
                $crate::keys::serialize(&self.$element, bytes);
                )+
            }

            #[inline]
            fn serialized_length(&self) -> usize {
                // we always need space for the key kind
                let mut serialized_length = $crate::keys::KeyKind::SERIALIZED_LENGTH;
                $(
                    serialized_length += $crate::keys::KeyEncode::serialized_length(&self.$element);
                )+
                serialized_length
            }
        }

        impl crate::keys::DecodeTableKey for $key_name {
            #[inline]
            fn deserialize_from<B: bytes::Buf>(bytes: &mut B) -> crate::Result<Self> {
                {
                    let key_kind = $crate::keys::KeyKind::deserialize(bytes)?;

                    if key_kind != $key_kind {
                        return Err(restate_storage_api::StorageError::Generic(anyhow::anyhow!("supported key kind '{}' but found key kind '{}'", $key_kind, key_kind)))
                    }
                }

                $(
                    let $element = $crate::keys::deserialize(bytes)?;
                 )+

                Ok(Self {
                    $($element,)+
                })
            }


        }
    })
}

use crate::PaddedPartitionId;
use crate::TableKind;
use crate::vqueue_table::vqueue_meta_merge;
pub(crate) use define_table_key;
use restate_storage_api::StorageError;
use restate_storage_api::deduplication_table::ProducerId;
use restate_storage_api::timer_table::TimerKeyKind;
use restate_types::identifiers::InvocationUuid;
use restate_types::journal_v2::{CompletionId, NotificationId, SignalIndex};

pub(crate) trait KeyEncode {
    fn encode<B: BufMut>(&self, target: &mut B);

    fn serialized_length(&self) -> usize;
}

impl<T: KeyEncode> KeyEncode for &T {
    fn encode<B: BufMut>(&self, target: &mut B) {
        (*self).encode(target);
    }

    fn serialized_length(&self) -> usize {
        (*self).serialized_length()
    }
}

pub(crate) trait KeyDecode: Sized {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self>;
}

impl KeyEncode for Bytes {
    fn encode<B: BufMut>(&self, target: &mut B) {
        write_delimited(self, target);
    }

    fn serialized_length(&self) -> usize {
        self.len()
            + encoded_len_varint(u64::try_from(self.len()).expect("usize should fit into u64"))
    }
}

impl KeyDecode for Bytes {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        read_delimited(source)
    }
}

impl KeyEncode for ByteString {
    fn encode<B: BufMut>(&self, target: &mut B) {
        write_delimited(self, target);
    }

    fn serialized_length(&self) -> usize {
        self.len()
            + encoded_len_varint(u64::try_from(self.len()).expect("usize should fit into u64"))
    }
}

impl KeyDecode for ByteString {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let bs = read_delimited(source)?;

        unsafe { Ok(ByteString::from_bytes_unchecked(bs)) }
    }
}

impl KeyEncode for PaddedPartitionId {
    fn encode<B: BufMut>(&self, target: &mut B) {
        // store u64 in big-endian order to support byte-wise increment operation. See `crate::scan::try_increment`.
        target.put_u64(**self);
    }

    fn serialized_length(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl KeyDecode for PaddedPartitionId {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(PaddedPartitionId::from(source.get_u64()))
    }
}

impl KeyEncode for u128 {
    fn encode<B: BufMut>(&self, target: &mut B) {
        // store u128 in big-endian order to support byte-wise increment operation. See `crate::scan::try_increment`.
        target.put_u128(*self);
    }

    fn serialized_length(&self) -> usize {
        16
    }
}

impl KeyDecode for u128 {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(source.get_u128())
    }
}

impl KeyEncode for UniqueTimestamp {
    fn encode<B: BufMut>(&self, target: &mut B) {
        // store u64 in big-endian order to support byte-wise increment operation. See `crate::scan::try_increment`.
        target.put_u64(self.as_u64());
    }

    fn serialized_length(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl KeyDecode for UniqueTimestamp {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        UniqueTimestamp::try_from(source.get_u64()).map_err(|e| StorageError::Conversion(e.into()))
    }
}

impl<const L: usize> KeyEncode for [u8; L] {
    fn encode<B: BufMut>(&self, target: &mut B) {
        // stores the array as is.
        target.put_slice(self.as_ref());
    }

    fn serialized_length(&self) -> usize {
        L
    }
}

impl<const L: usize> KeyDecode for [u8; L] {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        if source.remaining() < L {
            return Err(StorageError::DataIntegrityError);
        }
        let mut buf = [0u8; L];
        source.copy_to_slice(&mut buf);
        Ok(buf)
    }
}

impl KeyEncode for u64 {
    fn encode<B: BufMut>(&self, target: &mut B) {
        // store u64 in big-endian order to support byte-wise increment operation. See `crate::scan::try_increment`.
        target.put_u64(*self);
    }

    fn serialized_length(&self) -> usize {
        8
    }
}

impl KeyDecode for u64 {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(source.get_u64())
    }
}

impl KeyEncode for u32 {
    fn encode<B: BufMut>(&self, target: &mut B) {
        // store u32 in big-endian order to support byte-wise increment operation. See `crate::scan::try_increment`.
        target.put_u32(*self);
    }

    fn serialized_length(&self) -> usize {
        4
    }
}

impl KeyDecode for u32 {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(source.get_u32())
    }
}

impl KeyEncode for u8 {
    fn encode<B: BufMut>(&self, target: &mut B) {
        // store u8 in big-endian order to support byte-wise increment operation. See `crate::scan::try_increment`.
        target.put_u8(*self);
    }

    fn serialized_length(&self) -> usize {
        1
    }
}

impl KeyDecode for u8 {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(source.get_u8())
    }
}

impl KeyEncode for &[u8] {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put(*self);
    }

    fn serialized_length(&self) -> usize {
        self.len()
    }
}

impl KeyDecode for &[u8] {
    fn decode<B: Buf>(_source: &mut B) -> crate::Result<Self> {
        unimplemented!("could not decode into a slice u8");
    }
}

impl KeyEncode for InvocationUuid {
    fn encode<B: BufMut>(&self, target: &mut B) {
        let slice = self.to_bytes();
        debug_assert_eq!(slice.len(), self.serialized_length());
        target.put_slice(&slice);
    }

    fn serialized_length(&self) -> usize {
        InvocationUuid::RAW_BYTES_LEN
    }
}

impl KeyDecode for InvocationUuid {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        // note: this is a zero-copy when the source is bytes::Bytes.
        if source.remaining() < InvocationUuid::RAW_BYTES_LEN {
            return Err(StorageError::DataIntegrityError);
        }
        let mut buf = [0u8; InvocationUuid::RAW_BYTES_LEN];

        debug_assert!(source.remaining() >= InvocationUuid::RAW_BYTES_LEN);
        source.copy_to_slice(&mut buf);
        Ok(InvocationUuid::from_bytes(buf))
    }
}

impl KeyEncode for ProducerId {
    fn encode<B: BufMut>(&self, target: &mut B) {
        match self {
            ProducerId::Partition(p) => {
                let p = PaddedPartitionId::from(*p);
                target.put_u8(0);
                KeyEncode::encode(&p, target)
            }
            ProducerId::Other(i) => {
                target.put_u8(1);
                KeyEncode::encode(i, target)
            }
            ProducerId::Producer(i) => {
                target.put_u8(2);
                KeyEncode::encode(&u128::from(*i), target)
            }
        }
    }

    fn serialized_length(&self) -> usize {
        1 + match self {
            ProducerId::Partition(p) => KeyEncode::serialized_length(&PaddedPartitionId::from(*p)),
            ProducerId::Other(i) => KeyEncode::serialized_length(i),
            ProducerId::Producer(i) => KeyEncode::serialized_length(&u128::from(*i)),
        }
    }
}

impl KeyDecode for ProducerId {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(match source.get_u8() {
            0 => {
                let padded: PaddedPartitionId = KeyDecode::decode(source)?;
                ProducerId::Partition(padded.into())
            }
            1 => ProducerId::Other(KeyDecode::decode(source)?),
            2 => ProducerId::Producer(u128::decode(source)?.into()),
            i => {
                return Err(StorageError::Generic(anyhow!(
                    "Unexpected wrong discriminator for SequenceNumberSource: {}",
                    i
                )));
            }
        })
    }
}

impl KeyEncode for TimerKeyKind {
    fn encode<B: BufMut>(&self, target: &mut B) {
        assert!(
            self.serialized_length() <= target.remaining_mut(),
            "serialization buffer has not enough space to serialize TimerKind: '{}' bytes required",
            self.serialized_length()
        );
        match self {
            TimerKeyKind::Invoke { invocation_uuid } => {
                target.put_u8(0);
                invocation_uuid.encode(target);
            }
            TimerKeyKind::CompleteJournalEntry {
                invocation_uuid,
                journal_index,
            } => {
                target.put_u8(1);
                invocation_uuid.encode(target);
                journal_index.encode(target);
            }
            TimerKeyKind::CleanInvocationStatus { invocation_uuid } => {
                target.put_u8(2);
                invocation_uuid.encode(target);
            }
            TimerKeyKind::NeoInvoke { invocation_uuid } => {
                target.put_u8(3);
                invocation_uuid.encode(target);
            }
        }
    }

    fn serialized_length(&self) -> usize {
        1 + match self {
            TimerKeyKind::Invoke { invocation_uuid } => {
                KeyEncode::serialized_length(invocation_uuid)
            }
            TimerKeyKind::NeoInvoke { invocation_uuid } => {
                KeyEncode::serialized_length(invocation_uuid)
            }
            TimerKeyKind::CompleteJournalEntry {
                invocation_uuid,
                journal_index,
            } => {
                KeyEncode::serialized_length(invocation_uuid)
                    + KeyEncode::serialized_length(journal_index)
            }
            TimerKeyKind::CleanInvocationStatus { invocation_uuid } => {
                KeyEncode::serialized_length(invocation_uuid)
            }
        }
    }
}

impl KeyDecode for TimerKeyKind {
    fn decode<B: Buf>(source: &mut B) -> crate::partition_store::Result<Self> {
        if source.remaining() < mem::size_of::<u8>() {
            return Err(StorageError::Generic(anyhow!(
                "TimerKind discriminator byte is missing"
            )));
        }

        Ok(match source.get_u8() {
            0 => {
                let invocation_uuid = InvocationUuid::decode(source)?;
                TimerKeyKind::Invoke { invocation_uuid }
            }
            1 => {
                let invocation_uuid = InvocationUuid::decode(source)?;
                let journal_index = u32::decode(source)?;
                TimerKeyKind::CompleteJournalEntry {
                    invocation_uuid,
                    journal_index,
                }
            }
            2 => {
                let invocation_uuid = InvocationUuid::decode(source)?;
                TimerKeyKind::CleanInvocationStatus { invocation_uuid }
            }
            3 => {
                let invocation_uuid = InvocationUuid::decode(source)?;
                TimerKeyKind::NeoInvoke { invocation_uuid }
            }
            i => {
                return Err(StorageError::Generic(anyhow!(
                    "Unknown discriminator for TimerKind: '{}'",
                    i
                )));
            }
        })
    }
}

impl KeyEncode for NotificationId {
    fn encode<B: BufMut>(&self, target: &mut B) {
        assert!(
            self.serialized_length() <= target.remaining_mut(),
            "serialization buffer has not enough space to serialize NotificationId: '{}' bytes required",
            self.serialized_length()
        );
        match self {
            NotificationId::CompletionId(ci) => {
                target.put_u8(0);
                target.put_u32(*ci);
            }
            NotificationId::SignalIndex(si) => {
                target.put_u8(1);
                target.put_u32(*si);
            }
            NotificationId::SignalName(name) => {
                target.put_u8(2);
                target.put(name.clone().into_bytes());
            }
        }
    }

    fn serialized_length(&self) -> usize {
        1 + match self {
            NotificationId::CompletionId(_) => size_of::<CompletionId>(),
            NotificationId::SignalIndex(_) => size_of::<SignalIndex>(),
            NotificationId::SignalName(n) => n.len(),
        }
    }
}

impl KeyDecode for NotificationId {
    fn decode<B: Buf>(source: &mut B) -> crate::partition_store::Result<Self> {
        if source.remaining() < mem::size_of::<u8>() {
            return Err(StorageError::Generic(anyhow!(
                "NotificationId discriminator byte is missing"
            )));
        }

        Ok(match source.get_u8() {
            0 => {
                let completion_index = u32::decode(source)?;
                NotificationId::CompletionId(completion_index)
            }
            1 => {
                let signal_index = u32::decode(source)?;
                NotificationId::SignalIndex(signal_index)
            }
            2 => {
                let signal_name = ByteString::try_from(source.copy_to_bytes(source.remaining()))
                    .map_err(|err| StorageError::Generic(err.into()))?;
                NotificationId::SignalName(signal_name)
            }
            i => {
                return Err(StorageError::Generic(anyhow!(
                    "Unknown discriminator for NotificationId: '{}'",
                    i
                )));
            }
        })
    }
}

macro_rules! impl_string_key_codec {
    ($t:ty) => {
        impl KeyEncode for $t {
            fn encode<B: BufMut>(&self, target: &mut B) {
                write_delimited(self.as_bytes(), target);
            }

            fn serialized_length(&self) -> usize {
                encoded_len_varint(u64::try_from(self.len()).expect("usize fitting into u64"))
                    + self.len()
            }
        }

        impl KeyDecode for $t {
            fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
                let len = prost::encoding::decode_varint(source)
                    .map_err(|error| StorageError::Generic(error.into()))
                    .and_then(|len| {
                        usize::try_from(len).map_err(|err| StorageError::Generic(err.into()))
                    })?;

                let mut string_data = source.take(len);

                let result = if len <= string_data.chunk().len() {
                    // SAFETY: previously serialized as valid UTF-8
                    <$t>::from(unsafe { str::from_utf8_unchecked(&string_data.chunk()[..len]) })
                } else {
                    // Spread across multiple chunks; copy into a contiguous buffer.
                    let string_data = string_data.copy_to_bytes(len);
                    // SAFETY: previously serialized as valid UTF-8
                    <$t>::from(unsafe { str::from_utf8_unchecked(&string_data) })
                };

                string_data.advance(len);
                Ok(result)
            }
        }
    };
}

impl_string_key_codec!(ReString);
impl_string_key_codec!(ServiceName);

#[inline]
fn write_delimited<B: BufMut>(source: impl AsRef<[u8]>, target: &mut B) {
    let source = source.as_ref();
    prost::encoding::encode_varint(source.len() as u64, target);
    target.put(source);
}

#[inline]
fn read_delimited<B: Buf>(source: &mut B) -> crate::Result<Bytes> {
    let len = prost::encoding::decode_varint(source)
        .map_err(|error| StorageError::Generic(error.into()))?;
    // note: this is a zero-copy when the source is bytes::Bytes.
    Ok(source.copy_to_bytes(len as usize))
}

#[inline]
pub(crate) fn serialize<T: KeyEncode, B: BufMut>(what: &T, target: &mut B) {
    what.encode(target);
}

#[inline]
pub(crate) fn deserialize<T: KeyDecode, B: Buf>(source: &mut B) -> crate::Result<T> {
    T::decode(source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use restate_test_util::let_assert;
    use strum::IntoEnumIterator;

    #[test]
    fn write_read_round_trip() {
        let mut buf = BytesMut::new();
        write_delimited("hello", &mut buf);
        write_delimited(" ", &mut buf);
        write_delimited("world", &mut buf);

        let mut got = buf.freeze();
        assert_eq!(read_delimited(&mut got).unwrap(), "hello");
        assert_eq!(read_delimited(&mut got).unwrap(), " ");
        assert_eq!(read_delimited(&mut got).unwrap(), "world");
    }

    fn concat(a: &'static str, b: &'static str) -> Bytes {
        let mut buf = BytesMut::new();
        write_delimited(a, &mut buf);
        write_delimited(b, &mut buf);
        buf.freeze()
    }

    #[test]
    fn write_delim_keeps_lexicographical_sorting() {
        assert!(concat("a", "b") < concat("a", "c"));
        assert!(concat("a", "") < concat("d", ""));
    }

    #[test]
    fn invocation_uuid_roundtrip() {
        let uuid = InvocationUuid::mock_random();

        let mut buf = BytesMut::new();
        uuid.encode(&mut buf);

        let mut got_bytes = buf.freeze();

        assert_eq!(got_bytes.len(), uuid.serialized_length());
        let got = InvocationUuid::decode(&mut got_bytes).expect("deserialization should work");

        assert_eq!(uuid, got);
    }

    define_table_key!(TableKind::Deduplication, KeyKind::Deduplication, DeduplicationTestKey(value: u64));

    #[test]
    fn key_prefix_mismatch() {
        let mut buffer = DeduplicationTestKey { value: 42 }.serialize();
        // overwrite the key prefix
        KeyKind::Fsm.serialize(
            &mut buffer
                .get_mut(0..KeyKind::SERIALIZED_LENGTH)
                .expect("key prefix must be present"),
        );

        let result = DeduplicationTestKey::deserialize_from(&mut buffer);

        let_assert!(Err(StorageError::Generic(err)) = result);
        assert_eq!(
            err.to_string(),
            format!(
                "supported key kind '{}' but found key kind '{}'",
                KeyKind::Deduplication,
                KeyKind::Fsm
            )
        );
    }

    #[test]
    fn unknown_key_prefix() {
        let mut buffer = DeduplicationTestKey { value: 42 }.serialize();
        // overwrite the key prefix with an unknown value
        let unknown_key_prefix = b"ZZ";
        buffer
            .get_mut(0..KeyKind::SERIALIZED_LENGTH)
            .expect("key prefix should be present")
            .put_slice(unknown_key_prefix);

        let result = DeduplicationTestKey::deserialize_from(&mut buffer);

        let_assert!(Err(StorageError::Generic(err)) = result);
        assert_eq!(
            err.to_string(),
            format!("unknown key kind: {unknown_key_prefix:x?}")
        );
    }

    /// Tests that the [`KeyKind`] has a bijective byte representation.
    #[test]
    fn bijective_byte_representation() {
        let key_prefix_iter = KeyKind::iter();
        let mut buffer = BytesMut::with_capacity(2);

        for key_prefix in key_prefix_iter {
            buffer.clear();

            key_prefix.serialize(&mut buffer);
            let deserialized_key_prefix =
                KeyKind::deserialize(&mut buffer).expect("valid byte representation");
            assert_eq!(key_prefix, deserialized_key_prefix);
        }
    }
}
