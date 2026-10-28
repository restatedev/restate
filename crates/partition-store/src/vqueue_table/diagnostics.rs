// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! One-off description of a single vqueue's stored state, used when the scheduler finds the
//! metadata and the stage rows disagreeing. Reads a handful of keys synchronously; it is not
//! meant for hot paths.

use std::fmt::Write as _;

use bilrost::OwnedMessage;
use rocksdb::ReadOptions;

use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{EntryKey, EntryValue, RawStatusHeader, Stage};
use restate_types::vqueues::VQueueId;

use super::entry::EntryStatusKey;
use super::inbox::{
    FinishedKeyRef, InboxKeyRef, PausedKeyRef, RunningKey, RunningKeyRef, SuspendedKeyRef,
};
use super::metadata::{MetaKey, MetaKeyRef};
use crate::PartitionDb;
use crate::keys::{DecodeTableKey, EncodeTableKeyPrefix, KeyDecode, KeyKind};

/// Rows and headers listed per section before the listing is cut short.
const MAX_LISTED: usize = 16;

pub(crate) fn describe_vqueue(db: &PartitionDb, qid: &VQueueId) -> String {
    let mut out = String::new();
    describe_meta(db, qid, &mut out);
    for stage in [
        Stage::Inbox,
        Stage::Running,
        Stage::Suspended,
        Stage::Paused,
        Stage::Finished,
    ] {
        describe_stage(db, qid, stage, &mut out);
    }
    describe_headers(db, qid, &mut out);
    out
}

fn describe_meta(db: &PartitionDb, qid: &VQueueId, out: &mut String) {
    let mut key = [0u8; MetaKey::serialized_length_fixed()];
    MetaKeyRef::builder()
        .qid(qid)
        .serialize_to(&mut key.as_mut());
    let stored = db
        .rocksdb()
        .inner()
        .as_raw_db()
        .get_pinned_cf(db.cf_handle(), key);
    match stored {
        Ok(Some(raw)) => match VQueueMeta::decode(&mut raw.as_ref()) {
            Ok(meta) => {
                let _ = write!(
                    out,
                    "stored meta: num_inbox={} num_running={} num_suspended={} num_paused={} \
                     num_finished={} paused={} {meta:?}",
                    meta.stage_count(Stage::Inbox),
                    meta.stage_count(Stage::Running),
                    meta.stage_count(Stage::Suspended),
                    meta.stage_count(Stage::Paused),
                    meta.stage_count(Stage::Finished),
                    meta.queue_is_paused(),
                );
            }
            Err(err) => {
                let _ = write!(out, "stored meta: undecodable ({err})");
            }
        },
        Ok(None) => out.push_str("stored meta: missing"),
        Err(err) => {
            let _ = write!(out, "stored meta: read failed ({err})");
        }
    }
}

fn stage_prefix(stage: Stage, qid: &VQueueId) -> [u8; RunningKey::by_qid_prefix_len()] {
    let mut prefix = [0u8; RunningKey::by_qid_prefix_len()];
    let buf = &mut prefix.as_mut();
    match stage {
        Stage::Inbox => InboxKeyRef::builder().qid(qid).serialize_to(buf),
        Stage::Running => RunningKeyRef::builder().qid(qid).serialize_to(buf),
        Stage::Suspended => SuspendedKeyRef::builder().qid(qid).serialize_to(buf),
        Stage::Paused => PausedKeyRef::builder().qid(qid).serialize_to(buf),
        Stage::Finished => FinishedKeyRef::builder().qid(qid).serialize_to(buf),
        Stage::Unknown => unreachable!("no on-disk stage"),
    }
    prefix
}

fn bounded_read_options(lower: &[u8]) -> ReadOptions {
    let mut upper = lower.to_vec();
    let bounded = crate::convert_to_upper_bound(&mut upper);
    debug_assert!(bounded);
    let mut opts = ReadOptions::default();
    opts.set_total_order_seek(true);
    opts.set_iterate_lower_bound(lower.to_vec());
    opts.set_iterate_upper_bound(upper);
    opts
}

fn describe_stage(db: &PartitionDb, qid: &VQueueId, stage: Stage, out: &mut String) {
    let prefix = stage_prefix(stage, qid);
    let mut it = db
        .rocksdb()
        .inner()
        .as_raw_db()
        .raw_iterator_cf_opt(db.cf_handle(), bounded_read_options(&prefix));
    it.seek_to_first();
    let mut rows = 0usize;
    let mut listed = String::new();
    while it.valid() {
        if let (Some(key), Some(mut value)) = (it.key(), it.value()) {
            rows += 1;
            if rows <= MAX_LISTED {
                let entry_key =
                    <EntryKey as KeyDecode>::decode(&mut &key[RunningKey::offset_of_entry_key()..]);
                let status = EntryValue::decode(&mut value).map(|v| v.status);
                let _ = write!(listed, " [{entry_key:?} status={status:?}]");
            }
        }
        it.next();
    }
    let _ = write!(
        out,
        "; {}: {rows} rows{listed}",
        stage.to_string().to_lowercase()
    );
    if let Err(err) = it.status() {
        let _ = write!(out, " (iterator error: {err})");
    }
}

fn describe_headers(db: &PartitionDb, qid: &VQueueId, out: &mut String) {
    // Status headers are keyed by partition key and entry id, so list the partition key's
    // headers and keep the ones that point at this vqueue.
    let mut prefix = Vec::with_capacity(KeyKind::SERIALIZED_LENGTH + size_of::<u64>());
    KeyKind::VQueueEntryStatus.serialize(&mut prefix);
    prefix.extend_from_slice(&qid.partition_key().to_be_bytes());
    let mut it = db
        .rocksdb()
        .inner()
        .as_raw_db()
        .raw_iterator_cf_opt(db.cf_handle(), bounded_read_options(&prefix));
    it.seek_to_first();
    let mut headers = 0usize;
    let mut listed = String::new();
    while it.valid() {
        if let (Some(mut key), Some(mut value)) = (it.key(), it.value())
            && let Ok(status_key) = EntryStatusKey::deserialize_from(&mut key)
            && let Ok(header) = RawStatusHeader::decode_length_delimited(&mut value)
            && header.qid == *qid
        {
            headers += 1;
            if headers <= MAX_LISTED {
                let (id,) = status_key.split();
                let _ = write!(
                    listed,
                    " [{id} stage={} status={:?} seq={:?} run_at={:?} lock={}]",
                    header.stage, header.status, header.seq, header.next_run_at, header.has_lock
                );
            }
        }
        it.next();
    }
    let _ = write!(out, "; status headers: {headers}{listed}");
    if let Err(err) = it.status() {
        let _ = write!(out, " (iterator error: {err})");
    }
}
