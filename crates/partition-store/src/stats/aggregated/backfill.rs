// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Startup-only reconstruction from entry status and invocation status, joined in key order.
//! Object groups are completed at each partition-key boundary. Three bounded buffers hold
//! the current objects, service/deployment totals, and sorted output respectively. Spills
//! contain coalesced positive merge operands: even an exceptionally large partition-key
//! group can spill without overwriting earlier contributions or requiring an external sort.
//!
//! The processor must remain stopped throughout the build. Before ingesting any SST we
//! clear an interrupted build and flush the CF, making both the source and its applied LSN
//! durable. Ingestion makes each output file durable before the caller can publish the
//! feature ledger. A crash before that marker restarts the build, including the range clear;
//! a crash after it cannot lose the baseline or replay source writes already counted in it.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, ensure};
use bilrost::{BorrowedMessage, OwnedMessage};
use bytes::{BufMut, BytesMut};
use rocksdb::{IngestExternalFileOptions, Options, ReadOptions, SstFileWriter, WriteBatch};
use smallvec::SmallVec;
use tokio_util::sync::CancellationToken;
use tracing::info;
use zerocopy::IntoBytes;

use restate_memory::ByteCount;
use restate_rocksdb::{Priority, StorageTaskKind};
use restate_storage_api::StorageError;
use restate_storage_api::protobuf_types::v1::lazy::{InvocationStatusV2Lazy, InvocationTargetLazy};
use restate_storage_api::stats::deployment_load::DeploymentLoad;
use restate_storage_api::stats::service_load::ServiceLoad;
use restate_storage_api::stats::virtual_object_load::VirtualObjectLoad;
use restate_storage_api::vqueue_table::RawStatusHeaderRef;
use restate_types::identifiers::{ServiceId, WithPartitionKey};
use restate_types::invocation::ServiceType;
use restate_types::sharding::PartitionId;
use restate_types::storage::StorageCodecKind;
use restate_types::vqueues::{EntryKind, EntryTargetRef, HandlerRef, VQueueId};
use restate_util_time::DurationExt;

use crate::invocation_status_table::InvocationStatusKey;
use crate::keys::{DecodeTableKey, EncodeTableKeyPrefix, KeyKind};
use crate::scan::TableScan;
use crate::stats::{EncodeStatKey, Stat, StatKeyPrefix};
use crate::vqueue_table::{EntryStatusKey, InputPayloadKey};
use crate::{MigrationError, PartitionDb, PartitionStore};

use super::bucketed_gauge::GaugeBucket;
use super::{DeploymentLoadKey, ServiceLoadKey, StageStatus, VirtualObjectLoadKey};

// Per buffer, including keys, bucket allocations and a conservative B-tree node allowance.
// One source record/key can exceed the limit; it is processed and spilled immediately.
const BUFFER_BYTES: usize = 8 * 1024 * 1024;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
type Counts = SmallVec<[(StageStatus, i64); 2]>;

pub(crate) async fn run(
    storage: &PartitionStore,
    cancel: &CancellationToken,
) -> Result<(), MigrationError> {
    let db = storage.partition_db().clone();
    let cancel = cancel.clone();
    db.rocksdb()
        .clone()
        .run_background_read_op(
            "backfill-status-gauges",
            StorageTaskKind::BackgroundIterator,
            Priority::Low,
            move |_| build(&db, &cancel, BUFFER_BYTES),
        )
        .await
        .map_err(|_| StorageError::OperationalError)?
        .map_err(|err| {
            err.downcast::<MigrationError>()
                .unwrap_or_else(|err| StorageError::Generic(err).into())
        })?;
    Ok(())
}

fn check_cancel(cancel: &CancellationToken) -> anyhow::Result<()> {
    ensure!(!cancel.is_cancelled(), MigrationError::MigrationCancelled);
    Ok(())
}

#[derive(Default)]
struct Accumulator {
    entries: BTreeMap<Vec<u8>, Counts>,
    bytes: usize,
}

impl Accumulator {
    fn add(&mut self, key: &[u8], bucket: StageStatus, delta: i64) -> anyhow::Result<()> {
        let counts = if let Some(counts) = self.entries.get_mut(key) {
            counts
        } else {
            // Include inline values and enough per-entry overhead for partially full nodes.
            self.bytes += key.len() + size_of::<(Vec<u8>, Counts)>() + size_of::<[usize; 8]>();
            self.entries.entry(key.to_vec()).or_default()
        };
        let heap_size = |counts: &Counts| {
            if counts.spilled() {
                counts.capacity() * size_of::<(StageStatus, i64)>()
            } else {
                0
            }
        };
        self.bytes -= heap_size(counts);
        if let Some((_, total)) = counts.iter_mut().find(|(existing, _)| *existing == bucket) {
            *total = total
                .checked_add(delta)
                .context("status gauge backfill overflow")?;
        } else {
            counts.push((bucket, delta));
        }
        self.bytes += heap_size(counts);
        Ok(())
    }

    fn drain_into(&mut self, sink: &mut SstSink<'_>) -> anyhow::Result<()> {
        for (key, counts) in std::mem::take(&mut self.entries) {
            check_cancel(sink.cancel)?;
            for (bucket, count) in counts {
                sink.pending.add(&key, bucket, count)?;
            }
            if sink.pending.bytes >= sink.limit {
                sink.spill()?;
            }
        }
        self.bytes = 0;
        Ok(())
    }
}

struct SstSink<'a> {
    db: &'a PartitionDb,
    cancel: &'a CancellationToken,
    directory: tempfile::TempDir,
    options: Options,
    pending: Accumulator,
    limit: usize,
    files: u64,
}

impl<'a> SstSink<'a> {
    fn new(
        db: &'a PartitionDb,
        cancel: &'a CancellationToken,
        limit: usize,
    ) -> anyhow::Result<Self> {
        // A fixed, partition-local parent lets retries clean up files left by a process crash.
        let path = db
            .rocksdb()
            .inner()
            .as_raw_db()
            .path()
            .join(format!("status-gauges-{}.staging", db.partition().id()));
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err).context("cleaning status gauge staging directory"),
        }
        std::fs::create_dir_all(&path)?;
        let directory = tempfile::tempdir_in(path)?;
        let mut options = Options::default();
        options.set_compression_type(rocksdb::DBCompressionType::None);
        options.set_merge_operator(
            "PartitionMerge",
            KeyKind::full_merge,
            KeyKind::partial_merge,
        );
        Ok(Self {
            db,
            cancel,
            directory,
            options,
            pending: Accumulator::default(),
            limit,
            files: 0,
        })
    }

    fn spill(&mut self) -> anyhow::Result<()> {
        if self.pending.entries.is_empty() {
            return Ok(());
        }
        check_cancel(self.cancel)?;
        let path: PathBuf = self.directory.path().join("run.sst");
        let mut writer = SstFileWriter::create(&self.options);
        writer.open(&path)?;
        let mut value = Vec::new();
        for (key, counts) in &self.pending.entries {
            check_cancel(self.cancel)?;
            value.clear();
            for (bucket, count) in counts {
                bucket.encode(&mut value);
                value.put_i64(*count);
            }
            writer.merge(key, &value)?;
        }
        writer.finish()?;
        drop(writer);
        check_cancel(self.cancel)?;
        let mut options = IngestExternalFileOptions::default();
        options.set_move_files(true);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .ingest_external_file_cf_opts(self.db.cf_handle(), &options, vec![path])?;
        self.pending = Accumulator::default();
        self.files += 1;
        Ok(())
    }
}

fn reset(db: &PartitionDb) -> anyhow::Result<()> {
    let mut batch = WriteBatch::default();
    for prefix in [
        StatKeyPrefix::of::<ServiceLoad>(db.partition().id()),
        StatKeyPrefix::of::<DeploymentLoad>(db.partition().id()),
        StatKeyPrefix::of::<VirtualObjectLoad>(db.partition().id()),
    ] {
        let start = prefix.as_bytes();
        let mut end = start.to_vec();
        ensure!(
            crate::convert_to_upper_bound(&mut end),
            "stats prefix overflow"
        );
        batch.delete_range_cf(db.cf_handle(), start, &end);
    }
    let mut options = rocksdb::WriteOptions::default();
    options.disable_wal(true);
    db.rocksdb().inner().write_batch(&batch, &options)?;
    let mut flush = rocksdb::FlushOptions::default();
    flush.set_wait(true);
    db.rocksdb()
        .inner()
        .as_raw_db()
        .flush_cf_opt(db.cf_handle(), &flush)?;
    Ok(())
}

// Decode only the target, skipping the potentially large state map in StateMutationInput.
#[derive(bilrost::Message)]
struct StateMutationTarget {
    #[bilrost(1)]
    service_id: ServiceId,
}

fn target_ref<'a>(target: &'a InvocationTargetLazy<'_>) -> anyhow::Result<EntryTargetRef<'a>> {
    let service = target.service_name();
    let handler = target.handler_name();
    let scope = target.scope();
    Ok(match target.service_ty()? {
        ServiceType::Service => EntryTargetRef::Service {
            service,
            handler,
            scope,
        },
        ServiceType::VirtualObject => EntryTargetRef::VirtualObject {
            service,
            handler: HandlerRef::UserHandler(handler),
            scope,
            key: target.key()?.context("virtual object target has no key")?,
        },
        ServiceType::Workflow => EntryTargetRef::Workflow {
            service,
            handler,
            scope,
            key: target.key()?.context("workflow target has no key")?,
        },
    })
}

fn build(db: &PartitionDb, cancel: &CancellationToken, limit: usize) -> anyhow::Result<u64> {
    check_cancel(cancel)?;
    let started = Instant::now();
    let partition_id = db.partition().id();
    info!(%partition_id, buffer_size = %ByteCount::from(limit), "Backfilling status gauges");
    reset(db).context("resetting status gauges and flushing their source")?;
    let mut sink = SstSink::new(db, cancel, limit)?;
    let mut arena = BytesMut::new();
    let read_options = || {
        let mut options = ReadOptions::default();
        options.fill_cache(false);
        options.set_async_io(true);
        options
    };
    let mut entries = db.scan(
        TableScan::ScanPartitionKeyRange::<EntryStatusKey>(db.partition().key_range)
            .encode(&mut arena),
        read_options(),
    )?;
    let mut invocations = db.scan(
        TableScan::ScanPartitionKeyRange::<InvocationStatusKey>(db.partition().key_range)
            .encode(&mut arena),
        read_options(),
    )?;
    let mut totals = Accumulator::default();
    let mut objects = Accumulator::default();
    let mut previous_partition_key = None;
    let mut source_key = Vec::new();
    let mut stat_key = Vec::new();
    let mut scanned = 0u64;
    let mut last_report = Instant::now();
    while let Some((mut key, mut value)) = entries.item() {
        check_cancel(cancel)?;
        let id = EntryStatusKey::deserialize_from(&mut key)?.id;
        if previous_partition_key != Some(id.partition_key()) {
            objects.drain_into(&mut sink)?;
            previous_partition_key = Some(id.partition_key());
        }
        let header = RawStatusHeaderRef::decode_borrowed_length_delimited(&mut value)?;
        ensure!(
            id.partition_key() == header.qid.partition_key(),
            "entry and queue partition keys disagree"
        );
        let mut record = |target: &EntryTargetRef<'_>| -> anyhow::Result<()> {
            contribute::<ServiceLoad>(
                &mut totals,
                partition_id,
                ServiceLoadKey::borrowed(target.service(), target.handler(), id.kind()),
                &header,
                &mut stat_key,
            )?;
            if let Some(deployment) = header.metadata.deployment {
                contribute::<DeploymentLoad>(
                    &mut totals,
                    partition_id,
                    DeploymentLoadKey::borrowed(deployment, target.service()),
                    &header,
                    &mut stat_key,
                )?;
            }
            if let Some(key) = target.virtual_object_key() {
                contribute::<VirtualObjectLoad>(
                    &mut objects,
                    partition_id,
                    VirtualObjectLoadKey::borrowed(
                        target.service(),
                        key,
                        target.scope(),
                        target.handler(),
                        id.kind(),
                        id.partition_key(),
                    ),
                    &header,
                    &mut stat_key,
                )?;
            }
            Ok(())
        };
        match id.kind() {
            EntryKind::Invocation => {
                let invocation_id = id
                    .to_invocation_id()
                    .context("invalid invocation entry id")?;
                source_key.clear();
                InvocationStatusKey::builder()
                    .partition_key(invocation_id.partition_key())
                    .invocation_uuid(invocation_id.invocation_uuid())
                    .serialize_to(&mut source_key);
                // Invocation entries have the same relative order in both tables. Skip
                // legacy invocations without queue entries without decoding their values.
                while invocations
                    .key()
                    .is_some_and(|key| key < source_key.as_slice())
                {
                    check_cancel(cancel)?;
                    invocations.next();
                }
                invocations.status()?;
                ensure!(
                    invocations.key() == Some(source_key.as_slice()),
                    "missing invocation status for {id}"
                );
                let value = invocations
                    .value()
                    .context("missing invocation status value")?;
                let (&codec, value) = value.split_first().context("empty invocation status")?;
                ensure!(
                    matches!(
                        StorageCodecKind::try_from(codec)?,
                        StorageCodecKind::Protobuf
                    ),
                    "unsupported invocation status codec"
                );
                let mut invocation = InvocationStatusV2Lazy::default();
                invocation.merge(value)?;
                let target = invocation
                    .invocation_target()?
                    .context("missing invocation target")?;
                record(&target_ref(&target)?)?;
                invocations.next();
            }
            EntryKind::StateMutation => {
                let qid = VQueueId::from_raw_bytes(&mut header.qid.as_raw_bytes());
                source_key.clear();
                InputPayloadKey::builder()
                    .qid(qid)
                    .seq(header.seq)
                    .id(id.to_entry_id())
                    .serialize_to(&mut source_key);
                let value = db
                    .rocksdb()
                    .inner()
                    .as_raw_db()
                    .get_pinned_cf_opt(db.cf_handle(), &source_key, &read_options())?
                    .context("missing state mutation input")?;
                let mutation = StateMutationTarget::decode(value.as_ref())?;
                record(&EntryTargetRef::VirtualObject {
                    service: &mutation.service_id.service_name,
                    scope: mutation.service_id.scope.as_ref().map(AsRef::as_ref),
                    key: &mutation.service_id.key,
                    handler: HandlerRef::StateMutation,
                })?;
            }
            EntryKind::Unknown => anyhow::bail!("unknown vqueue entry kind"),
        }
        // Bound even a pathological single partition-key group, or an unusually large
        // service/deployment population. Positive operands compose across all spills.
        if objects.bytes >= limit {
            objects.drain_into(&mut sink)?;
        }
        if totals.bytes >= limit {
            totals.drain_into(&mut sink)?;
        }
        scanned += 1;
        if last_report.elapsed() >= PROGRESS_INTERVAL {
            info!(%partition_id, scanned, files = sink.files, elapsed = %started.elapsed().friendly(), "Backfilling status gauges");
            last_report = Instant::now();
        }
        entries.next();
    }
    entries.status()?;
    invocations.status()?;
    drop((entries, invocations));
    objects.drain_into(&mut sink)?;
    totals.drain_into(&mut sink)?;
    sink.spill()?;
    check_cancel(cancel)?;
    info!(%partition_id, scanned, files = sink.files, elapsed = %started.elapsed().friendly(), "Finished backfilling status gauges");
    Ok(sink.files)
}

fn contribute<S: Stat>(
    accumulator: &mut Accumulator,
    partition_id: PartitionId,
    key: impl EncodeStatKey<S>,
    header: &RawStatusHeaderRef<'_>,
    scratch: &mut Vec<u8>,
) -> anyhow::Result<()> {
    scratch.clear();
    S::encode_key(partition_id, key, scratch);
    accumulator.add(scratch, StageStatus::new(header.stage, header.status), 1)
}

#[cfg(test)]
mod tests;
