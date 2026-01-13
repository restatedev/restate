// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::event_listener::{
    CompactionJobInfo, DBBackgroundErrorReason, DBCompactionReason, DBFlushReason,
    DBWriteStallCondition, EventListener, FlushJobInfo, MemTableInfo, MutableStatus,
    WriteStallInfo,
};
use tracing::{debug, error, trace, warn};

use restate_serde_util::ByteCount;
use restate_time_util::FriendlyDuration;

use crate::DbName;

/// Event listener for logging key RocksDB events and recording metrics.
///
/// The listener is registered with every rocksdb database opened through
/// the [`RocksDbManager`].
pub(crate) struct LoggingEventListener {
    db_name: DbName,
}

impl LoggingEventListener {
    pub fn new(db_name: DbName) -> Self {
        Self { db_name }
    }
}

impl EventListener for LoggingEventListener {
    fn on_flush_begin(&self, i: &FlushJobInfo) {
        let cf_name = i.cf_name().unwrap_or_default();
        let cf_name = String::from_utf8_lossy(&cf_name);
        let trigger = i.flush_reason().as_friendly_reason();
        let smallest_seqno = i.smallest_seqno();
        let largest_seqno = i.largest_seqno();

        let impact = if i.triggered_writes_slowdown() {
            "write-slowdown"
        } else if i.triggered_writes_stop() {
            "write-stop"
        } else {
            "normal"
        };

        trace!(
            db = %self.db_name,
            cf = %cf_name,
            "Flush started. flush-trigger: {trigger}, impact: {impact}, smallest_seqno: {smallest_seqno}, largest_seqno: {largest_seqno}"
        );
    }

    fn on_flush_completed(&self, i: &FlushJobInfo) {
        let cf_name = i.cf_name().unwrap_or_default();
        let cf_name = String::from_utf8_lossy(&cf_name);
        let trigger = i.flush_reason().as_friendly_reason();

        let impact = if i.triggered_writes_slowdown() {
            // If true, then rocksdb is currently slowing-down all writes to prevent
            // creating too many Level 0 files as compaction seems not able to
            // catch up the write request speed.  This indicates that there are
            // too many files in Level 0.
            "write-slowdown"
        } else if i.triggered_writes_stop() {
            // If true, then rocksdb is currently blocking any writes to prevent
            // creating more L0 files.  This indicates that there are too many
            // files in level 0.  Compactions should try to compact L0 files down
            // to lower levels as soon as possible.
            "write-stop"
        } else {
            "normal"
        };

        debug!(
            db = %self.db_name,
            cf = %cf_name,
            "Flush completed. flush-trigger: {trigger}, impact: {impact}"
        );
    }

    fn on_compaction_begin(&self, i: &CompactionJobInfo) {
        let cf_name = i.cf_name().unwrap_or_default();
        let cf_name = String::from_utf8_lossy(&cf_name);
        let trigger = i.compaction_reason().as_friendly_reason();

        let input_files = i.input_file_count();
        let input_level = i.base_input_level();
        let input_bytes = i.total_input_bytes();

        let output_level = i.output_level();

        debug!(
            db = %self.db_name,
            cf = %cf_name,
            "Compaction started. [L{input_level}]->[L{output_level}] compaction-trigger: {trigger}, input_bytes: {}, input_files: {input_files}",
            ByteCount::from(input_bytes),
        );
    }

    fn on_compaction_completed(&self, i: &CompactionJobInfo) {
        let cf_name = i.cf_name().unwrap_or_default();
        let cf_name = String::from_utf8_lossy(&cf_name);
        let trigger = i.compaction_reason().as_friendly_reason();

        let input_files = i.input_file_count();
        let input_level = i.base_input_level();
        let input_bytes = i.total_input_bytes();

        let output_level = i.output_level();
        let output_files = i.output_file_count();

        let input_records = i.input_records();
        let output_records = i.output_records();
        let output_bytes = i.total_output_bytes();

        let corrupt_keys = i.num_corrupt_keys();

        if let Err(err) = i.status() {
            warn!(
                db = %self.db_name,
                cf = %cf_name,
                err = %err,
                "Compaction failed! [L{input_level}]->[L{output_level}] compaction-trigger: {trigger}, \
                    elapsed: {}, corrupt_keys: {corrupt_keys}, \
                    input_bytes: {}, output_bytes: {}, \
                    input_files: {input_files}, output_files: {output_files}, \
                    input_records: {input_records}, output_records: {output_records}",
                FriendlyDuration::from_micros(i.elapsed_micros()),
                ByteCount::from(input_bytes),
                ByteCount::from(output_bytes),
            );
        } else {
            // if compaction completed without making any material impact, it's a trivial
            // compaction and we don't want to bother logging about it on info level.
            let tag = if input_files == output_files || input_records == output_records {
                " [trivial]"
            } else {
                ""
            };

            trace!(
                db = %self.db_name,
                cf = %cf_name,
                "Compaction completed{tag}. [L{input_level}]->[L{output_level}] compaction-trigger: {trigger}, \
                    elapsed: {}, \
                    input_bytes: {}, output_bytes: {}, \
                    input_files: {input_files}, output_files: {output_files}, \
                    input_records: {input_records}, output_records: {output_records}",
                FriendlyDuration::from_micros(i.elapsed_micros()),
                ByteCount::from(input_bytes),
                ByteCount::from(output_bytes),
            );

            let records_compacted = input_records.saturating_sub(output_records);
            let bytes_saved = input_bytes.saturating_sub(output_bytes);
            let files_reduction = input_files.saturating_sub(output_files);
            if bytes_saved > 0 || files_reduction > 0 {
                debug!(
                    db = %self.db_name,
                    cf = %cf_name,
                    "Compaction completed. [L{input_level}]->[L{output_level}] compaction-trigger: {trigger}, \
                        elapsed: {}, \
                        bytes_saved: {}, records_compacted: {records_compacted}, files_reduction: {files_reduction}",
                    FriendlyDuration::from_micros(i.elapsed_micros()),
                    ByteCount::from(bytes_saved),
                );
            }
        }
    }

    fn on_stall_conditions_changed(&self, i: &WriteStallInfo) {
        let cf_name = i.cf_name().unwrap_or_default();
        let cf_name = String::from_utf8_lossy(&cf_name);
        let before = i.prev().as_friendly_reason();
        let after = i.cur().as_friendly_reason();
        warn!(
            db = %self.db_name,
            cf = %cf_name,
            "Stall conditions changed: {before} -> {after}",
        );
    }

    fn on_memtable_sealed(&self, i: &MemTableInfo) {
        let cf_name = i.cf_name().unwrap_or_default();
        let cf_name = String::from_utf8_lossy(&cf_name);
        trace!(
            db = %self.db_name,
            cf = %cf_name,
            "Memtable sealed. deletes: {}, entries: {}, first_seqno: {}, earliest_seqno: {}",
            i.num_deletes(),
            i.num_entries(),
            i.first_seqno(),
            i.earliest_seqno(),
        );
    }

    fn on_background_error(&self, reason: DBBackgroundErrorReason, status: MutableStatus) {
        error!(
            db = %self.db_name,
            reason = ?reason,
            "RocksDB background error: {:?}",
            status.result()
        );
    }
}

// better reason strings for our logging
trait FriendlyReason {
    fn as_friendly_reason(&self) -> &'static str;
}

impl FriendlyReason for DBFlushReason {
    fn as_friendly_reason(&self) -> &'static str {
        match self {
            DBFlushReason::KUnknown => "unknown",
            DBFlushReason::KOthers => "other",
            DBFlushReason::KGetLiveFiles => "get-live-files",
            DBFlushReason::KShutDown => "shutdown",
            DBFlushReason::KExternalFileIngestion => "external-file-ingestion",
            DBFlushReason::KManualCompaction => "manual-compaction",
            DBFlushReason::KWriteBufferManager => "write-buffer-manager",
            DBFlushReason::KWriteBufferFull => "memtable-full",
            DBFlushReason::KTest => "test",
            DBFlushReason::KDeleteFiles => "delete-files",
            DBFlushReason::KAutoCompaction => "auto-compaction",
            DBFlushReason::KManualFlush => "manual-flush",
            DBFlushReason::KErrorRecovery => "error-recovery",
            DBFlushReason::KErrorRecoveryRetryFlush => "error-recovery-retry-flush",
            DBFlushReason::KWalFull => "wal-full",
            DBFlushReason::KCatchUpAfterErrorRecovery => "catch-up-after-error-recovery",
        }
    }
}

impl FriendlyReason for DBCompactionReason {
    fn as_friendly_reason(&self) -> &'static str {
        match self {
            DBCompactionReason::KUnknown => "unknown",
            // Level Compaction
            DBCompactionReason::KLevelL0filesNum => "level-0-files-num",
            DBCompactionReason::KLevelMaxLevelSize => "level-max-level-size",
            // Universal Compaction
            DBCompactionReason::KUniversalSizeAmplification => "universal-size-amplification",
            DBCompactionReason::KUniversalSizeRatio => "universal-size-ratio",
            DBCompactionReason::KUniversalSortedRunNum => "universal-sorted-run-num",
            // Fifo Compaction
            DBCompactionReason::KFifomaxSize => "fifomax-size",
            DBCompactionReason::KFiforeduceNumFiles => "fiforeduce-num-files",
            DBCompactionReason::KFifottl => "fifottl",
            // --
            DBCompactionReason::KManualCompaction => "manual-compaction",
            DBCompactionReason::KFilesMarkedForCompaction => "files-marked-for-compaction",
            DBCompactionReason::KBottommostFiles => "bottommost-files",
            DBCompactionReason::KTtl => "ttl",
            DBCompactionReason::KFlush => "flush",
            DBCompactionReason::KExternalSstIngestion => "external-sst-ingestion",
            DBCompactionReason::KPeriodicCompaction => "periodic-compaction",
            DBCompactionReason::KChangeTemperature => "change-temperature",
            DBCompactionReason::KForcedBlobGc => "forced-blob-gc",
            DBCompactionReason::KRoundRobinTtl => "round-robin-ttl",
            DBCompactionReason::KRefitLevel => "refit-level",
            DBCompactionReason::KNumOfReasons => "num-of-reasons",
        }
    }
}

impl FriendlyReason for DBWriteStallCondition {
    fn as_friendly_reason(&self) -> &'static str {
        match self {
            DBWriteStallCondition::KDelayed => "delayed",
            DBWriteStallCondition::KStopped => "stopped",
            DBWriteStallCondition::KNormal => "normal",
        }
    }
}
