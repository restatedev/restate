// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Value decoding for partition-store entries.
//!
//! This module provides utilities to decode values stored in the partition-store
//! using the appropriate decoder for each table type.

use bilrost::OwnedMessage;

use restate_partition_store::fsm_table::PartitionStateMachineKey;
use restate_partition_store::keys::{DecodeTableKey, KeyKind};
use restate_partition_store::vqueue_table::{EntryStatusKey, InputPayloadKey, StatusHeaderRaw};
use restate_storage_api::deduplication_table::DedupSequenceNumber;
use restate_storage_api::fsm_table::{PartitionDurability, SequenceNumber};
use restate_storage_api::idempotency_table::IdempotencyMetadata;
use restate_storage_api::inbox_table::InboxEntry;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::journal_table::JournalEntry as JournalEntryV1;
use restate_storage_api::journal_table_v2::StoredEntry;
use restate_storage_api::lock_table::LockState;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::promise_table::Promise;
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::service_status_table::VirtualObjectStatus;
use restate_storage_api::timer_table::Timer;
use restate_storage_api::vqueue_table::EntryValue;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_types::SemanticRestateVersion;
use restate_types::state_mut::ExternalStateMutation;
use restate_types::storage::StorageCodecKind;
use restate_types::vqueues::EntryKind;

/// FSM variable IDs (from partition-store/src/fsm_table/mod.rs)
mod fsm_variable {
    pub const INBOX_SEQ_NUMBER: u64 = 0;
    pub const OUTBOX_SEQ_NUMBER: u64 = 1;
    pub const APPLIED_LSN: u64 = 2;
    pub const RESTATE_VERSION_BARRIER: u64 = 3;
    pub const PARTITION_DURABILITY: u64 = 4;
    pub const STORAGE_VERSION: u64 = 5;
    pub const SERVICES_SCHEMA_METADATA: u64 = 6;
}

/// Result of decoding a value, including codec metadata
#[derive(Debug)]
pub struct DecodedValue {
    /// The codec used (if applicable)
    pub codec: Option<StorageCodecKind>,
    /// Payload size (excluding codec byte)
    pub payload_size: usize,
    /// The decoded content or error
    pub content: DecodedContent,
}

/// The decoded content
#[derive(Debug)]
pub enum DecodedContent {
    /// Successfully decoded value with Debug representation
    Decoded(String),
    /// Raw bytes (e.g., user state) - no codec wrapper
    RawBytes,
    /// Empty value (key-only tables)
    Empty,
    /// Decoding failed with error message
    Error(String),
}

impl std::fmt::Display for DecodedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.content {
            DecodedContent::Decoded(s) => {
                if let Some(codec) = self.codec {
                    write!(f, "[codec={codec}] {s}")
                } else {
                    write!(f, "{s}")
                }
            }
            DecodedContent::RawBytes => {
                write!(f, "<{} bytes of raw data>", self.payload_size)
            }
            DecodedContent::Empty => write!(f, "<empty>"),
            DecodedContent::Error(e) => {
                if let Some(codec) = self.codec {
                    write!(f, "[codec={codec}] <decode error: {e}>")
                } else {
                    write!(f, "<decode error: {e}>")
                }
            }
        }
    }
}

impl DecodedValue {
    fn empty() -> Self {
        Self {
            codec: None,
            payload_size: 0,
            content: DecodedContent::Empty,
        }
    }

    fn raw_bytes(size: usize) -> Self {
        Self {
            codec: None,
            payload_size: size,
            content: DecodedContent::RawBytes,
        }
    }

    fn decoded(codec: Option<StorageCodecKind>, payload_size: usize, value: String) -> Self {
        Self {
            codec,
            payload_size,
            content: DecodedContent::Decoded(value),
        }
    }

    fn error(codec: Option<StorageCodecKind>, payload_size: usize, error: String) -> Self {
        Self {
            codec,
            payload_size,
            content: DecodedContent::Error(error),
        }
    }
}

/// Decode a value based on the key kind and key bytes.
///
/// Different tables use different encoding schemes:
/// - Most tables use protobuf with a StorageCodec wrapper (1-byte codec discriminant + protobuf)
/// - State table stores raw user bytes (no codec)
/// - VQueue tables use bilrost encoding (no codec discriminant)
/// - FSM table uses different types based on the state_id in the key
pub fn decode_value(key_kind: KeyKind, key: &[u8], value: &[u8]) -> DecodedValue {
    if value.is_empty() {
        return DecodedValue::empty();
    }

    match key_kind {
        // Raw bytes - user state, no decoding
        KeyKind::State => DecodedValue::raw_bytes(value.len()),

        // Key-only tables (VQueue active have empty values)
        KeyKind::VQueueActive => {
            if value.is_empty() {
                DecodedValue::empty()
            } else {
                DecodedValue::raw_bytes(value.len())
            }
        }

        // Bilrost-encoded (no StorageCodec wrapper)
        KeyKind::VQueueMeta => decode_bilrost::<VQueueMeta>(value),
        KeyKind::Lock => decode_bilrost::<LockState>(value),
        KeyKind::VQueueEntryStatus => decode_vqueue_entry_status(value, key),
        KeyKind::VQueueInput => decode_vqueue_item(value, key),
        KeyKind::VQueueInboxStage
        | KeyKind::VQueueRunningStage
        | KeyKind::VQueueSuspendedStage
        | KeyKind::VQueuePausedStage
        | KeyKind::VQueueFinishedStage => decode_bilrost::<EntryValue>(value),

        // Protobuf-encoded with StorageCodec
        KeyKind::Deduplication => decode_protobuf::<DedupSequenceNumber>(value),
        KeyKind::Idempotency => decode_protobuf::<IdempotencyMetadata>(value),
        KeyKind::Inbox => decode_protobuf::<InboxEntry>(value),
        #[allow(deprecated)]
        KeyKind::InvocationStatus | KeyKind::InvocationStatusV1 => {
            decode_protobuf::<InvocationStatus>(value)
        }
        KeyKind::Journal => decode_protobuf::<JournalEntryV1>(value),
        KeyKind::JournalV2 => decode_protobuf::<StoredEntry>(value),
        KeyKind::JournalV2NotificationIdToNotificationIndex
        | KeyKind::JournalV2CompletionIdToCommandIndex => {
            // These store JournalEntryIndex (u32)
            decode_with_codec_info(value)
        }
        KeyKind::JournalEvent => {
            // Journal events use protobuf but the type is complex
            decode_with_codec_info(value)
        }
        KeyKind::Outbox => decode_protobuf::<OutboxMessage>(value),
        KeyKind::ServiceStatus => decode_protobuf::<VirtualObjectStatus>(value),
        KeyKind::Timers => decode_protobuf::<Timer>(value),
        KeyKind::Promise => decode_protobuf::<Promise>(value),

        // FSM table - decode based on state_id from key
        KeyKind::Fsm => decode_fsm_value(key, value),
    }
}

/// Decode a bilrost value without a codec prefix
fn decode_bilrost<T>(value: &[u8]) -> DecodedValue
where
    T: OwnedMessage + std::fmt::Debug,
{
    if value.is_empty() {
        return DecodedValue::empty();
    }

    let mut buf = value;
    match T::decode(&mut buf) {
        Ok(v) => DecodedValue::decoded(None, value.len(), format!("{v:?}")),
        Err(e) => DecodedValue::error(None, value.len(), format!("{e}")),
    }
}

fn decode_vqueue_entry_status(value: &[u8], key: &[u8]) -> DecodedValue {
    let key_kind = {
        let mut cursor = key;
        EntryStatusKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| k.id.kind())
    };

    let mut buf = value;
    let header = match StatusHeaderRaw::decode_length_delimited(&mut buf) {
        Ok(header) => header,
        Err(e) => {
            return DecodedValue::error(
                None,
                value.len(),
                format!("failed to decode EntryStatus header: {e}"),
            );
        }
    };

    let trailing_bytes = if buf.is_empty() {
        String::new()
    } else {
        format!(", trailing_bytes={}", buf.len())
    };

    DecodedValue::decoded(
        None,
        value.len(),
        format!("EntryStatus {{ header: {header:?}, kind: {key_kind:?}{trailing_bytes} }}"),
    )
}

fn decode_vqueue_item(value: &[u8], key: &[u8]) -> DecodedValue {
    let key_kind = {
        let mut cursor = key;
        InputPayloadKey::deserialize_from(&mut cursor)
            .ok()
            .map(|k| k.id.kind())
    };

    match key_kind {
        Some(EntryKind::Unknown) => DecodedValue::raw_bytes(value.len()),
        Some(EntryKind::StateMutation) => decode_bilrost::<ExternalStateMutation>(value),
        Some(EntryKind::Invocation) | None => {
            if value.is_empty() {
                DecodedValue::empty()
            } else {
                DecodedValue::decoded(
                    None,
                    value.len(),
                    format!("<bilrost {} bytes>", value.len()),
                )
            }
        }
    }
}

#[allow(dead_code)]
fn decoded_content_to_string(decoded: DecodedValue) -> String {
    match decoded.content {
        DecodedContent::Decoded(s) => s,
        DecodedContent::RawBytes => format!("<{} bytes of raw data>", decoded.payload_size),
        DecodedContent::Empty => "<empty>".to_string(),
        DecodedContent::Error(e) => format!("<decode error: {e}>"),
    }
}

#[allow(dead_code)]
fn take_length_delimited_payload<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let (len, len_varint_size) = decode_varint(input)?;
    let total_needed = len_varint_size.checked_add(len)?;
    if input.len() < total_needed {
        return None;
    }

    let payload_start = len_varint_size;
    let payload_end = payload_start + len;
    let payload = &input[payload_start..payload_end];
    *input = &input[payload_end..];
    Some(payload)
}

#[allow(dead_code)]
fn decode_varint(data: &[u8]) -> Option<(usize, usize)> {
    let mut result: usize = 0;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        if i >= 10 {
            return None;
        }

        result |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }

        shift += 7;
    }

    None
}

/// Decode a protobuf value using PartitionStoreProtobufValue trait
fn decode_protobuf<T>(value: &[u8]) -> DecodedValue
where
    T: PartitionStoreProtobufValue + std::fmt::Debug,
    <<T as PartitionStoreProtobufValue>::ProtobufType as TryInto<T>>::Error: Into<anyhow::Error>,
{
    if value.is_empty() {
        return DecodedValue::empty();
    }

    // Read codec byte
    let codec_byte = value[0];
    let codec = StorageCodecKind::try_from(codec_byte).ok();
    let payload_size = value.len().saturating_sub(1);

    let mut buf = value;
    match T::decode(&mut buf) {
        Ok(v) => DecodedValue::decoded(codec, payload_size, format!("{v:?}")),
        Err(e) => DecodedValue::error(codec, payload_size, format!("{e}")),
    }
}

/// Show codec info without fully decoding
fn decode_with_codec_info(value: &[u8]) -> DecodedValue {
    if value.is_empty() {
        return DecodedValue::empty();
    }

    let codec_byte = value[0];
    let payload_size = value.len().saturating_sub(1);

    match StorageCodecKind::try_from(codec_byte) {
        Ok(codec) => DecodedValue::decoded(
            Some(codec),
            payload_size,
            format!("<{} bytes>", payload_size),
        ),
        Err(_) => DecodedValue::error(
            None,
            value.len(),
            format!("unknown codec byte {codec_byte:#x}"),
        ),
    }
}

/// Decode FSM value using the state_id from the key to determine the value type
fn decode_fsm_value(key: &[u8], value: &[u8]) -> DecodedValue {
    if value.is_empty() {
        return DecodedValue::empty();
    }

    // Extract state_id from the key
    let state_id = {
        let mut cursor = key;
        match PartitionStateMachineKey::deserialize_from(&mut cursor) {
            Ok(k) => k.state_id,
            Err(_) => {
                // Can't parse key, fall back to generic decoding
                return decode_fsm_value_generic(value);
            }
        }
    };

    // Read codec byte
    let codec_byte = value[0];
    let codec = StorageCodecKind::try_from(codec_byte).ok();
    let payload_size = value.len().saturating_sub(1);

    // Decode based on state_id
    match state_id {
        fsm_variable::INBOX_SEQ_NUMBER => {
            decode_fsm_sequence_number(value, codec, payload_size, "InboxSeqNumber")
        }
        fsm_variable::OUTBOX_SEQ_NUMBER => {
            decode_fsm_sequence_number(value, codec, payload_size, "OutboxSeqNumber")
        }
        fsm_variable::APPLIED_LSN => {
            decode_fsm_sequence_number(value, codec, payload_size, "AppliedLsn")
        }
        fsm_variable::STORAGE_VERSION => {
            decode_fsm_sequence_number(value, codec, payload_size, "StorageVersion")
        }
        fsm_variable::RESTATE_VERSION_BARRIER => {
            let mut buf = value;
            match SemanticRestateVersion::decode(&mut buf) {
                Ok(v) => DecodedValue::decoded(codec, payload_size, format!("RestateVersion({v})")),
                Err(e) => DecodedValue::error(codec, payload_size, format!("{e}")),
            }
        }
        fsm_variable::PARTITION_DURABILITY => {
            let mut buf = value;
            match PartitionDurability::decode(&mut buf) {
                Ok(v) => DecodedValue::decoded(codec, payload_size, format!("{v:?}")),
                Err(e) => DecodedValue::error(codec, payload_size, format!("{e}")),
            }
        }
        fsm_variable::SERVICES_SCHEMA_METADATA => {
            // Schema uses FlexbuffersSerde - read length prefix
            let payload = &value[1..];
            if payload.len() < 4 {
                return DecodedValue::error(
                    codec,
                    payload_size,
                    "Schema value too short".to_string(),
                );
            }
            let len = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
            DecodedValue::decoded(codec, payload_size, format!("Schema({len} bytes)"))
        }
        unknown => DecodedValue::decoded(
            codec,
            payload_size,
            format!("<unknown state_id={unknown}, {} bytes>", payload_size),
        ),
    }
}

/// Decode a SequenceNumber FSM value with a descriptive label
fn decode_fsm_sequence_number(
    value: &[u8],
    codec: Option<StorageCodecKind>,
    payload_size: usize,
    label: &str,
) -> DecodedValue {
    let mut buf = value;
    match SequenceNumber::decode(&mut buf) {
        Ok(v) => DecodedValue::decoded(codec, payload_size, format!("{label}({})", v.0)),
        Err(e) => DecodedValue::error(codec, payload_size, format!("{e}")),
    }
}

/// Generic FSM value decoding when we can't parse the key
fn decode_fsm_value_generic(value: &[u8]) -> DecodedValue {
    if value.is_empty() {
        return DecodedValue::empty();
    }

    let codec_byte = value[0];
    let codec = match StorageCodecKind::try_from(codec_byte) {
        Ok(c) => c,
        Err(_) => {
            return DecodedValue::error(
                None,
                value.len(),
                format!("unknown codec byte {codec_byte:#x}"),
            );
        }
    };

    let payload_size = value.len().saturating_sub(1);

    // Try common types
    match codec {
        StorageCodecKind::Protobuf => {
            // Try PartitionDurability (has distinctive fields)
            let mut buf = value;
            if let Ok(v) = PartitionDurability::decode(&mut buf) {
                return DecodedValue::decoded(Some(codec), payload_size, format!("{v:?}"));
            }
            // Try SemanticRestateVersion
            let mut buf = value;
            if let Ok(v) = SemanticRestateVersion::decode(&mut buf) {
                return DecodedValue::decoded(
                    Some(codec),
                    payload_size,
                    format!("RestateVersion({v})"),
                );
            }
            // Try SequenceNumber
            let mut buf = value;
            if let Ok(v) = SequenceNumber::decode(&mut buf) {
                return DecodedValue::decoded(
                    Some(codec),
                    payload_size,
                    format!("SequenceNumber({})", v.0),
                );
            }
            DecodedValue::decoded(
                Some(codec),
                payload_size,
                format!("<{} bytes>", payload_size),
            )
        }
        StorageCodecKind::FlexbuffersSerde => {
            let payload = &value[1..];
            if payload.len() >= 4 {
                let len = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
                DecodedValue::decoded(Some(codec), payload_size, format!("Schema({len} bytes)"))
            } else {
                DecodedValue::error(
                    Some(codec),
                    payload_size,
                    "flexbuffers too short".to_string(),
                )
            }
        }
        _ => DecodedValue::decoded(
            Some(codec),
            payload_size,
            format!("<{} bytes>", payload_size),
        ),
    }
}
