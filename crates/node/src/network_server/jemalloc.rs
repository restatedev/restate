// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared jemalloc utilities for memory statistics and operations.

use std::sync::OnceLock;

use metrics::gauge;
use serde::Serialize;
use tikv_jemalloc_ctl::{epoch, stats};

use crate::metric_definitions::{
    JEMALLOC_ACTIVE, JEMALLOC_ALLOCATED, JEMALLOC_MAPPED, JEMALLOC_METADATA, JEMALLOC_RESIDENT,
    JEMALLOC_RETAINED,
};

/// Pre-computed MIBs for jemalloc statistics to avoid repeated string lookups.
///
/// MIBs (Management Information Base) are pre-computed lookup keys that avoid
/// the overhead of parsing string names on each read.
struct StatsMibs {
    epoch: tikv_jemalloc_ctl::epoch_mib,
    allocated: tikv_jemalloc_ctl::stats::allocated_mib,
    active: tikv_jemalloc_ctl::stats::active_mib,
    metadata: tikv_jemalloc_ctl::stats::metadata_mib,
    mapped: tikv_jemalloc_ctl::stats::mapped_mib,
    retained: tikv_jemalloc_ctl::stats::retained_mib,
    resident: tikv_jemalloc_ctl::stats::resident_mib,
}

impl StatsMibs {
    fn new() -> Result<Self, tikv_jemalloc_ctl::Error> {
        Ok(Self {
            epoch: epoch::mib()?,
            allocated: stats::allocated::mib()?,
            active: stats::active::mib()?,
            metadata: stats::metadata::mib()?,
            mapped: stats::mapped::mib()?,
            retained: stats::retained::mib()?,
            resident: stats::resident::mib()?,
        })
    }
}

/// Global lazily-initialized MIBs for efficient repeated reads.
static STATS_MIBS: OnceLock<Option<StatsMibs>> = OnceLock::new();

fn get_mibs() -> Option<&'static StatsMibs> {
    STATS_MIBS.get_or_init(|| StatsMibs::new().ok()).as_ref()
}

/// jemalloc memory statistics.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct JemallocStats {
    /// Total number of bytes allocated by the application.
    pub allocated: usize,
    /// Total number of bytes in active pages allocated by the application.
    /// This is a multiple of the page size, and greater than or equal to `allocated`.
    pub active: usize,
    /// Total number of bytes dedicated to jemalloc metadata.
    pub metadata: usize,
    /// Total number of bytes in chunks mapped on behalf of the application.
    /// This includes both active and unused chunks.
    pub mapped: usize,
    /// Total number of bytes in virtual memory mappings that were retained
    /// rather than being returned to the operating system.
    pub retained: usize,
    /// Maximum number of bytes in physically resident data pages mapped by the allocator.
    pub resident: usize,
}

impl JemallocStats {
    /// Reads current jemalloc statistics.
    ///
    /// This advances the jemalloc epoch to refresh statistics before reading.
    /// Uses pre-computed MIBs for efficient repeated reads.
    pub fn read() -> Result<Self, tikv_jemalloc_ctl::Error> {
        if let Some(mibs) = get_mibs() {
            mibs.epoch.advance()?;
            Ok(Self {
                allocated: mibs.allocated.read()?,
                active: mibs.active.read()?,
                metadata: mibs.metadata.read()?,
                mapped: mibs.mapped.read()?,
                retained: mibs.retained.read()?,
                resident: mibs.resident.read()?,
            })
        } else {
            // Fallback to string-based API if MIB initialization failed
            epoch::advance()?;
            Self::read_without_epoch_advance()
        }
    }

    /// Reads current jemalloc statistics without advancing the epoch.
    ///
    /// Use this when you've already advanced the epoch or need to read
    /// multiple snapshots without the overhead of epoch advancement.
    pub fn read_without_epoch_advance() -> Result<Self, tikv_jemalloc_ctl::Error> {
        if let Some(mibs) = get_mibs() {
            Ok(Self {
                allocated: mibs.allocated.read()?,
                active: mibs.active.read()?,
                metadata: mibs.metadata.read()?,
                mapped: mibs.mapped.read()?,
                retained: mibs.retained.read()?,
                resident: mibs.resident.read()?,
            })
        } else {
            // Fallback to string-based API if MIB initialization failed
            Ok(Self {
                allocated: stats::allocated::read()?,
                active: stats::active::read()?,
                metadata: stats::metadata::read()?,
                mapped: stats::mapped::read()?,
                retained: stats::retained::read()?,
                resident: stats::resident::read()?,
            })
        }
    }

    /// Submits jemalloc statistics to the metrics recorder.
    pub fn submit_metrics(&self) {
        gauge!(JEMALLOC_ALLOCATED).set(self.allocated as f64);
        gauge!(JEMALLOC_ACTIVE).set(self.active as f64);
        gauge!(JEMALLOC_METADATA).set(self.metadata as f64);
        gauge!(JEMALLOC_MAPPED).set(self.mapped as f64);
        gauge!(JEMALLOC_RETAINED).set(self.retained as f64);
        gauge!(JEMALLOC_RESIDENT).set(self.resident as f64);
    }
}

/// Reads and submits jemalloc statistics to the metrics recorder.
///
/// This is a convenience function that combines reading and submitting.
/// If reading fails, the function silently returns without submitting.
pub fn submit_metrics() {
    if let Ok(stats) = JemallocStats::read() {
        stats.submit_metrics();
    }
}
