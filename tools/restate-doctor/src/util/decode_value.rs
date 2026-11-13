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

use std::fmt::Write;

use bilrost::OwnedMessage;

use restate_limiter::RuleBook;
use restate_partition_store::PartitionSeal;
use restate_partition_store::blob_table::{BlobHeader, BlobKey};
use restate_partition_store::fsm_table::PartitionStateMachineKey;
use restate_partition_store::keys::{DecodeTableKey, KeyKind};
use restate_partition_store::stats::aggregated::{StageCounts, StageStatusCounts};
use restate_partition_store::stats::{AggregateKeyPrefix, StatKind, StatValueCodec};
use restate_partition_store::vqueue_table::{EntryStatusKey, InputPayloadKey};
use restate_storage_api::blob_table::Header;
use restate_storage_api::deduplication_table::DedupSequenceNumber;
use restate_storage_api::fsm_table::{
    CachedEpochMetadata, FsmPosition, PartitionDurability, SequenceNumber,
};
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
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{EntryValue, RawStatusHeader};
use restate_types::SemanticRestateVersion;
use restate_types::partitions::features::PersistedFeatures;
use restate_types::state_mut::StateMutationInput;
use restate_types::storage::{StorageCodec, StorageCodecKind, StorageDecode};
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
    /// *Since v1.6*
    pub const PARTITION_CONFIG_STATE: u64 = 7;
    /// *Since v1.7.0*
    pub const JC_ORPHAN_CLEANUP_DONE: u64 = 8;
    /// *Since v1.7.0*
    pub const RULE_BOOK: u64 = 9;
    /// *Since v1.7.0*
    pub const STATE_MACHINE_FEATURES: u64 = 10;
    /// *Since v1.7.3*
    pub const SEAL_MARKER: u64 = 11;
    /// *Since v1.7.9*
    pub const STORAGE_FEATURES: u64 = 12;
    /// *Since v1.8.2*
    pub const FSM_POSITION: u64 = 13;
    /// *Since v1.8.2*
    pub const INDEX_REGISTRY: u64 = 14;
    /// *Since v1.8.2*
    pub const WALKS: u64 = 15;
    /// *Since v1.8.2*
    pub const MIGRATION_JOBS: u64 = 16;
    /// *Since v1.8.2*
    pub const MIGRATION_JOB_HISTORY: u64 = 17;
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
    if key_kind == KeyKind::Stats {
        return match AggregateKeyPrefix::decode(key) {
            Ok((_, kind, _)) => decode_stat_value(kind, value),
            Err(err) => DecodedValue::error(None, value.len(), err.to_string()),
        };
    }

    if value.is_empty() {
        return DecodedValue::empty();
    }

    match key_kind {
        // Raw bytes - user state, no decoding
        KeyKind::State | KeyKind::ScopedState => DecodedValue::raw_bytes(value.len()),

        // Preserve nonempty index values as raw data.
        KeyKind::SecondaryIndex => DecodedValue::raw_bytes(value.len()),

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
        KeyKind::Inbox => decode_protobuf::<InboxEntry>(value),
        #[allow(deprecated)]
        KeyKind::Idempotency => decode_with_codec_info(value),
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
        KeyKind::Promise | KeyKind::ScopedPromise => decode_protobuf::<Promise>(value),
        KeyKind::Blob => decode_blob(key, value),

        // FSM table - decode based on state_id from key
        KeyKind::Fsm => decode_fsm_value(key, value),
        KeyKind::Stats => unreachable!("handled before empty-value decoding"),
        #[allow(deprecated)]
        KeyKind::LegacyStats => DecodedValue::decoded(
            None,
            value.len(),
            format!("retired statistics value, {} bytes", value.len()),
        ),
    }
}

fn decode_stat_value(kind: StatKind, value: &[u8]) -> DecodedValue {
    match kind {
        StatKind::AggregatedGauge => match <u64 as StatValueCodec>::deserialize_from(value) {
            Ok(value) => DecodedValue::decoded(None, size_of::<u64>(), format!("Gauge({value})")),
            Err(_) => DecodedValue::error(
                None,
                value.len(),
                format!("expected {} bytes for a gauge value", size_of::<u64>()),
            ),
        },
        StatKind::StageBucketedGauge => decode_stage_counts(value),
        StatKind::StageStatusBucketedGauge => match StageStatusCounts::deserialize_from(value) {
            Ok(value) => DecodedValue::decoded(
                None,
                value.serialized_len(),
                format!("StageStatusCounts({:?})", value.iter().collect::<Vec<_>>()),
            ),
            Err(_) => DecodedValue::error(
                None,
                value.len(),
                "invalid stage/status-bucketed gauge value".to_owned(),
            ),
        },
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
    let header = match RawStatusHeader::decode_length_delimited(&mut buf) {
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
        Some(EntryKind::StateMutation) => decode_bilrost::<StateMutationInput>(value),
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

fn decode_blob(mut key: &[u8], value: &[u8]) -> DecodedValue {
    if value.is_empty() {
        return DecodedValue::empty();
    }

    let created_at = {
        BlobKey::deserialize_from(&mut key)
            .ok()
            .map(|k| k.blob_id.timestamp().to_unix_millis())
    };

    match BlobHeader::decode(value) {
        Ok((header, _body)) => {
            let mut decoded = format!(
                "Blob(Kind: {}, RefCount: {}",
                header.kind(),
                header.ref_count()
            );
            if let Some(ts) = created_at {
                let _ = write!(
                    decoded,
                    ", CreatedAt: {} ({} ms)",
                    ts.into_timestamp(),
                    ts.as_u64()
                );
            }
            decoded.push(')');
            DecodedValue::decoded(None, value.len(), decoded)
        }
        Err(err) => DecodedValue::error(None, value.len(), format!("Blob error: {err}")),
    }
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

    // The seal marker is plain json, it is not wrapped in a StorageCodec envelope.
    if state_id == fsm_variable::SEAL_MARKER {
        return decode_fsm_seal_marker(value);
    }
    if state_id == fsm_variable::STORAGE_FEATURES {
        return decode_fsm_storage_features(value);
    }
    if state_id == fsm_variable::INDEX_REGISTRY {
        return decode_fsm_registry(value);
    }
    if state_id == fsm_variable::WALKS {
        return decode_fsm_walks(value);
    }
    if state_id == fsm_variable::MIGRATION_JOBS {
        return decode_fsm_jobs("MigrationJobs", value);
    }
    if state_id == fsm_variable::MIGRATION_JOB_HISTORY {
        return decode_fsm_jobs("MigrationJobHistory", value);
    }

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
        fsm_variable::PARTITION_CONFIG_STATE => {
            decode_fsm_storage_codec::<CachedEpochMetadata>(value, codec, payload_size)
        }
        fsm_variable::JC_ORPHAN_CLEANUP_DONE => {
            decode_fsm_sequence_number(value, codec, payload_size, "JcOrphanCleanupDone")
        }
        fsm_variable::RULE_BOOK => decode_fsm_storage_codec::<RuleBook>(value, codec, payload_size),
        fsm_variable::STATE_MACHINE_FEATURES => {
            decode_fsm_storage_codec::<PersistedFeatures>(value, codec, payload_size)
        }
        fsm_variable::FSM_POSITION => {
            decode_fsm_storage_codec::<FsmPosition>(value, codec, payload_size)
        }
        unknown => DecodedValue::decoded(
            codec,
            payload_size,
            format!("<unknown state_id={unknown}, {} bytes>", payload_size),
        ),
    }
}

/// Decode the partition seal marker.
///
/// Unlike every other FSM variable, the seal marker is stored as plain json bytes without a
/// [`StorageCodec`] envelope, so there is no codec byte to strip. Values written by a newer
/// version may carry a [`PartitionSeal`] variant we don't know about; in that case we fall back to
/// echoing the raw json instead of reporting a decode error.
fn decode_fsm_seal_marker(value: &[u8]) -> DecodedValue {
    let payload_size = value.len();

    match serde_json::from_slice::<PartitionSeal>(value) {
        Ok(seal) => DecodedValue::decoded(None, payload_size, format!("PartitionSeal({seal:?})")),
        Err(err) => match serde_json::from_slice::<serde_json::Value>(value) {
            Ok(raw) => DecodedValue::decoded(
                None,
                payload_size,
                format!("PartitionSeal(<unrecognized> {raw})"),
            ),
            Err(_) => DecodedValue::error(None, payload_size, format!("{err}")),
        },
    }
}

/// Decode local storage features without interpreting their names.
fn decode_fsm_storage_features(value: &[u8]) -> DecodedValue {
    let payload_size = value.len();
    match serde_json::from_slice::<serde_json::Value>(value) {
        Ok(features) => {
            DecodedValue::decoded(None, payload_size, format!("StorageFeatures({features})"))
        }
        Err(err) => DecodedValue::error(None, payload_size, format!("{err}")),
    }
}

fn decode_stage_counts(value: &[u8]) -> DecodedValue {
    match StageCounts::deserialize_from(value) {
        Ok(value) => DecodedValue::decoded(
            None,
            value.serialized_len(),
            format!("StageCounts({:?})", value.iter().collect::<Vec<_>>()),
        ),
        Err(_) => DecodedValue::error(
            None,
            value.len(),
            "invalid stage-bucketed gauge value".to_owned(),
        ),
    }
}

/// Decodes the index and statistics registry, stored as plain bilrost. A record
/// this binary rejects, because a newer binary wrote it or it breaks an
/// invariant, is still shown from its raw fields, next to the reason: that is
/// when an operator needs to see it most.
fn decode_fsm_registry(value: &[u8]) -> DecodedValue {
    use bilrost::OwnedMessage;
    use restate_partition_store::registry::IndexRegistry;

    let payload_size = value.len();
    let (registry, rejected) = match IndexRegistry::decode(value) {
        Ok(registry) => (registry, None),
        Err(reason) => match <IndexRegistry as OwnedMessage>::decode(value) {
            Ok(registry) => (registry, Some(reason)),
            Err(_) => return DecodedValue::error(None, payload_size, reason.to_string()),
        },
    };
    let entries = registry
        .entries()
        .iter()
        .map(|r| {
            let walk = r
                .walk
                .map(|walk| format!(" walk={walk}"))
                .unwrap_or_default();
            let timeline = if r.timeline == Default::default() {
                String::new()
            } else {
                format!(" {}", r.timeline)
            };
            format!(
                "{} {} {:?} {:?}{walk}{timeline}",
                r.id, r.definition, r.kind, r.state
            )
        })
        .collect::<Vec<_>>();
    DecodedValue::decoded(
        None,
        payload_size,
        format!(
            "IndexRegistry(format={}, [{}]){}",
            registry.format(),
            entries.join("; "),
            rejected
                .map(|reason| format!(" rejected: {reason}"))
                .unwrap_or_default()
        ),
    )
}

/// Decodes the walks, stored as plain bilrost; a rejected record is shown from
/// its raw fields next to the reason, as for the registry.
fn decode_fsm_walks(value: &[u8]) -> DecodedValue {
    use bilrost::OwnedMessage;
    use restate_partition_store::walk::Walks;

    let payload_size = value.len();
    let (walks, rejected) = match Walks::decode(value) {
        Ok(walks) => (walks, None),
        Err(reason) => match <Walks as OwnedMessage>::decode(value) {
            Ok(walks) => (walks, Some(reason)),
            Err(_) => return DecodedValue::error(None, payload_size, reason.to_string()),
        },
    };
    let entries = walks
        .entries()
        .iter()
        .map(|w| {
            format!(
                "id={} job={}/{} end={} cursor={}",
                w.id,
                w.kind.name,
                w.kind.version,
                super::hex_encode(&w.end),
                w.cursor
                    .as_deref()
                    .map_or_else(|| "-".to_owned(), super::hex_encode),
            )
        })
        .collect::<Vec<_>>();
    DecodedValue::decoded(
        None,
        payload_size,
        format!(
            "Walks([{}]){}",
            entries.join("; "),
            rejected
                .map(|reason| format!(" rejected: {reason}"))
                .unwrap_or_default()
        ),
    )
}

/// Decodes a list of migration jobs, the unfinished ones or the history, stored
/// as plain bilrost.
fn decode_fsm_jobs(label: &str, value: &[u8]) -> DecodedValue {
    use restate_partition_store::job::MigrationJobs;

    let payload_size = value.len();
    let jobs = match MigrationJobs::decode(value) {
        Ok(jobs) => jobs,
        Err(err) => return DecodedValue::error(None, payload_size, err.to_string()),
    };
    let entries = jobs
        .entries()
        .iter()
        .map(|j| {
            format!(
                "id={} {:?} {} {:?} walk={:?} created={:?} created_by={:?} started={:?} \
                 finished={:?} passes={} rows_examined={} rows_filled={} slices={} work_us={} \
                 last_slice={:?} last_slice_work_us={:?} failures={} consecutive_failures={} \
                 last_error={:?} last_error_at={:?}",
                j.id(),
                j.kind,
                j.name,
                j.status,
                j.walk,
                j.created_at,
                j.created_by,
                j.started_at,
                j.finished_at,
                j.passes,
                j.rows_examined,
                j.rows_filled,
                j.slices,
                j.work_us,
                j.last_slice_at,
                j.last_slice_work_us,
                j.failures,
                j.consecutive_failures,
                j.last_error,
                j.last_error_at,
            )
        })
        .collect::<Vec<_>>();
    DecodedValue::decoded(
        None,
        payload_size,
        format!("{label}([{}])", entries.join("; ")),
    )
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

/// Decode a `StorageCodec`-wrapped FSM value (codec byte + payload, e.g. bilrost).
fn decode_fsm_storage_codec<T>(
    value: &[u8],
    codec: Option<StorageCodecKind>,
    payload_size: usize,
) -> DecodedValue
where
    T: StorageDecode + std::fmt::Debug,
{
    let mut buf = value;
    match StorageCodec::decode::<T, _>(&mut buf) {
        Ok(v) => DecodedValue::decoded(codec, payload_size, format!("{v:?}")),
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

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use restate_partition_store::PaddedPartitionId;
    use restate_partition_store::fsm_table::PartitionStateMachineKey;
    use restate_partition_store::keys::EncodeTableKeyPrefix;
    use restate_types::identifiers::PartitionId;
    use restate_types::logs::Lsn;
    use restate_types::storage::StorageCodec;
    use zerocopy::IntoBytes;

    use super::*;

    fn fsm_key(state_id: u64) -> BytesMut {
        PartitionStateMachineKey {
            partition_id: PaddedPartitionId::from(restate_types::identifiers::PartitionId::MIN),
            state_id,
        }
        .serialize()
    }

    /// A registry this binary rejects, here one of a newer format, is shown from its
    /// raw fields together with the reason.
    #[test]
    fn rejected_registries_are_shown_with_their_reason() {
        /// The persisted registry's layout, with a format this binary does not support.
        #[derive(bilrost::Message)]
        struct NewerRegistry {
            #[bilrost(1)]
            format: u32,
            #[bilrost(2)]
            next_id: u64,
        }
        let value = bilrost::Message::encode_to_vec(&NewerRegistry {
            format: 2,
            next_id: 64,
        });
        let decoded = decode_value(KeyKind::Fsm, &fsm_key(fsm_variable::INDEX_REGISTRY), &value);
        let DecodedContent::Decoded(text) = &decoded.content else {
            panic!("shown from its raw fields, got: {decoded}");
        };
        assert!(text.contains("IndexRegistry(format=2, [])"), "{text}");
        assert!(
            text.contains("rejected: unsupported registry format 2"),
            "{text}"
        );
    }

    /// The newer FSM variables (>= v1.6) must decode through their dedicated arms
    /// rather than falling through to the `<unknown state_id>` branch.
    #[test]
    fn decodes_new_fsm_variables() {
        // StorageCodec(Bilrost)-wrapped value
        let mut buf = BytesMut::new();
        StorageCodec::encode(&PersistedFeatures::default(), &mut buf).unwrap();
        let decoded = decode_value(
            KeyKind::Fsm,
            &fsm_key(fsm_variable::STATE_MACHINE_FEATURES),
            &buf,
        );
        assert_eq!(decoded.codec, Some(StorageCodecKind::Bilrost));
        assert!(
            matches!(decoded.content, DecodedContent::Decoded(_)),
            "state-machine features should decode, got: {decoded}"
        );

        // Protobuf-wrapped SequenceNumber flag
        let value = {
            use restate_storage_api::protobuf_types::ProtobufStorageWrapper;
            let mut buf = BytesMut::new();
            StorageCodec::encode(
                &ProtobufStorageWrapper(restate_storage_api::protobuf_types::v1::SequenceNumber {
                    sequence_number: 1,
                }),
                &mut buf,
            )
            .unwrap();
            buf
        };
        let decoded = decode_value(
            KeyKind::Fsm,
            &fsm_key(fsm_variable::JC_ORPHAN_CLEANUP_DONE),
            &value,
        );
        assert!(
            matches!(&decoded.content, DecodedContent::Decoded(s) if s.contains("JcOrphanCleanupDone(1)")),
            "jc-orphan-cleanup flag should decode, got: {decoded}"
        );

        // Seal marker: plain json, no StorageCodec envelope
        let value = serde_json::to_vec(&PartitionSeal::AheadOfLog {
            partition_applied_lsn: Lsn::new(10),
            log_tail_lsn: Lsn::new(5),
        })
        .unwrap();
        let decoded = decode_value(KeyKind::Fsm, &fsm_key(fsm_variable::SEAL_MARKER), &value);
        assert_eq!(decoded.codec, None);
        assert!(
            matches!(&decoded.content, DecodedContent::Decoded(s) if s.contains("AheadOfLog")),
            "seal marker should decode, got: {decoded}"
        );

        // A variant written by a newer version falls back to the raw json
        let decoded = decode_value(
            KeyKind::Fsm,
            &fsm_key(fsm_variable::SEAL_MARKER),
            br#"{"type":"SomethingNew"}"#,
        );
        assert!(
            matches!(&decoded.content, DecodedContent::Decoded(s) if s.contains("<unrecognized>")),
            "unknown seal variant should fall back to raw json, got: {decoded}"
        );

        // Storage features: plain json with names that this binary need not recognize.
        let decoded = decode_value(
            KeyKind::Fsm,
            &fsm_key(fsm_variable::STORAGE_FEATURES),
            br#"{"features":{"future-feature":{"min_required_version":"2.0.0"}}}"#,
        );
        assert_eq!(decoded.codec, None);
        assert!(
            matches!(&decoded.content, DecodedContent::Decoded(s) if s.contains("future-feature")),
            "storage features should preserve unknown names, got: {decoded}"
        );

        // FSM position: a StorageCodec-encoded message
        let mut position = FsmPosition::new(Lsn::new(1));
        position.start_byte_offsets(Lsn::new(2));
        position.advance(Lsn::new(3), 30);
        let mut value = BytesMut::new();
        StorageCodec::encode(&position, &mut value).unwrap();
        let decoded = decode_value(KeyKind::Fsm, &fsm_key(fsm_variable::FSM_POSITION), &value);
        assert!(
            matches!(&decoded.content, DecodedContent::Decoded(s) if s.contains("log_byte_offset: 30")),
            "fsm position should decode, got: {decoded}"
        );

        // Plain-bilrost registry, walk and job values have distinct IDs from the position.
        assert_eq!(fsm_variable::FSM_POSITION, 13);
        for (id, value, expected) in [
            (
                14,
                restate_partition_store::registry::IndexRegistry::default().encode(),
                "IndexRegistry(format=1, [])",
            ),
            (
                15,
                restate_partition_store::walk::Walks::default().encode(),
                "Walks([])",
            ),
            (
                16,
                restate_partition_store::job::MigrationJobs::default().encode(),
                "MigrationJobs([])",
            ),
            (
                17,
                restate_partition_store::job::MigrationJobs::default().encode(),
                "MigrationJobHistory([])",
            ),
        ] {
            let decoded = decode_value(KeyKind::Fsm, &fsm_key(id), &value);
            assert_eq!(decoded.codec, None);
            assert_eq!(decoded.to_string(), expected);
        }
    }

    #[test]
    fn decodes_bucketed_gauge_values() {
        use restate_storage_api::vqueue_table::{Stage, Status};

        let mut key = AggregateKeyPrefix::new(PartitionId::from(7), 70)
            .as_bytes()
            .to_vec();
        key.push(StatKind::StageStatusBucketedGauge as u8);
        key.extend_from_slice(b"payload");

        let mut value = vec![Stage::Inbox as u8, Status::New as u8];
        value.extend_from_slice(&42_u64.to_be_bytes());
        let decoded = decode_value(KeyKind::Stats, &key, &value);
        assert_eq!(decoded.codec, None);
        assert_eq!(decoded.payload_size, 2 + size_of::<u64>());
        assert!(
            matches!(decoded.content, DecodedContent::Decoded(value) if value.contains("Inbox") && value.contains("New") && value.contains("42"))
        );

        let decoded = decode_value(KeyKind::Stats, &key, &[0; 7]);
        assert!(
            matches!(decoded.content, DecodedContent::Error(error) if error.contains("invalid stage/status-bucketed"))
        );

        let decoded = decode_value(KeyKind::Stats, &key, &[]);
        assert!(
            matches!(decoded.content, DecodedContent::Decoded(value) if value == "StageStatusCounts([])")
        );

        // A key without its aggregation kind cannot select a value codec.
        let decoded = decode_value(
            KeyKind::Stats,
            &key[..AggregateKeyPrefix::SERIALIZED_LENGTH],
            &value,
        );
        assert!(matches!(decoded.content, DecodedContent::Error(_)));
    }
}
