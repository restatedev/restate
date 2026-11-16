// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU32, NonZeroUsize};
use std::str::FromStr;

use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use restate_util_bytecount::NonZeroByteCount;
use restate_util_time::FriendlyDuration;

#[serde_as]
#[derive(Debug, Clone, Default, Serialize, Deserialize, derive_builder::Builder, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "RocksDbOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
// NOTE: Prefix with rocksdb_
pub struct RocksDbOptions {
    /// # Disable Direct IO for reads
    ///
    /// Files will be opened in "direct I/O" mode
    /// which means that data r/w from the disk will not be cached or
    /// buffered. The hardware buffer of the devices may however still
    /// be used. Memory mapped files are not impacted by these parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_direct_io_for_reads: Option<bool>,

    /// # Disable Direct IO for flush and compactions
    ///
    /// Use O_DIRECT for writes in background flush and compactions.
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_direct_io_for_flush_and_compactions: Option<bool>,

    /// Disable rocksdb statistics collection
    ///
    /// Default: False (statistics enabled)
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_statistics: Option<bool>,

    /// # RocksDB compaction readahead size in bytes
    ///
    /// If non-zero, we perform bigger reads when doing compaction. If you're
    /// running RocksDB on spinning disks, you should set this to at least 2MB.
    /// That way RocksDB's compaction is doing sequential instead of random reads.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<NonZeroByteCount>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    rocksdb_compaction_readahead_size: Option<NonZeroUsize>,

    /// # RocksDB disable auto compactions
    ///
    /// Disables rocksdb automatic compactions. This can be useful in performance tuning
    /// tests but should not be used in production to avoid write stall.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    rocksdb_disable_auto_compactions: Option<bool>,

    /// # RocksDB periodic compaction seconds
    ///
    /// Sets a period (in seconds) after which data must go through at least one compaction operation.
    ///
    /// Setting to 0 disables periodic compactions.
    ///
    /// Default: unset (automatic)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub(super) rocksdb_periodic_compaction: Option<FriendlyDuration>,

    /// # RocksDB statistics level
    ///
    /// StatsLevel can be used to reduce statistics overhead by skipping certain
    /// types of stats in the stats collection process.
    ///
    /// Default: "except-detailed-timers"
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_statistics_level: Option<StatisticsLevel>,

    /// # RocksDB log level
    ///
    /// Verbosity of the LOG.
    ///
    /// Default: "error"
    rocksdb_log_level: Option<RocksDbLogLevel>,

    /// # RocksDB log keep file num
    ///
    /// Number of info LOG files to keep
    ///
    /// Default: 1
    rocksdb_log_keep_file_num: Option<usize>,

    /// # RocksDB log max file size
    ///
    /// Max size of info LOG file
    ///
    /// Default: 64MB
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    rocksdb_log_max_file_size: Option<NonZeroByteCount>,

    /// # RocksDB block size
    ///
    /// Uncompressed block size
    ///
    /// Default: 64KiB
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<NonZeroByteCount>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<NonZeroByteCount>"))]
    rocksdb_block_size: Option<NonZeroUsize>,

    /// # Disable WAL compression
    ///
    /// When false (the default), the Write-Ahead Log is compressed with Zstd.
    /// Set to true to disable WAL compression. Only applies when WAL is enabled.
    ///
    /// Default: false (WAL compression enabled)
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_wal_compression: Option<bool>,

    /// # SST compression
    ///
    /// Compression of SST files for L0, the levels in between and the bottommost level, in that
    /// order: `none`, `lz4` or `zstd`, as a list or separated by commas. The last value applies to
    /// the remaining levels, so `"lz4"` compresses every level with Lz4 and `"lz4,none"` only L0.
    ///
    /// L0 holds the files written by flushes, which compaction soon rewrites. The bottommost level,
    /// the last one that holds data, holds most of a database's data. Lz4 uses little CPU; Zstd
    /// writes smaller files at a higher CPU cost.
    ///
    /// Default: `"none,lz4,zstd"`
    ///
    /// Takes effect on restart and applies to SST files written from then on: existing files keep
    /// their compression until compaction rewrites them.
    ///
    /// Since v1.8.2
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_compression: Option<RocksDbCompression>,

    // -- Deprecated options (not exposed in the schema)
    /// Deprecated since v1.8.2, to be removed in v1.9.0: `true` turns off compression in L0, like
    /// a first value of `none` in `rocksdb-compression`, unless `rocksdb-compression` is set in the
    /// same section. `false` changes nothing.
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "Option::is_none")]
    rocksdb_disable_l0_l1_compression: Option<bool>,
}

/// Verbosity of the LOG.
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[derive(Debug, Clone, Copy, Hash, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
#[repr(i32)]
pub enum RocksDbLogLevel {
    Debug = 0,
    Info,
    Warn,
    #[default]
    Error,
    Fatal,
    Header,
}

impl RocksDbOptions {
    pub fn apply_common(&mut self, common: &RocksDbOptions) {
        if self.rocksdb_disable_direct_io_for_reads.is_none() {
            self.rocksdb_disable_direct_io_for_reads =
                Some(common.rocksdb_disable_direct_io_for_reads());
        }
        if self
            .rocksdb_disable_direct_io_for_flush_and_compactions
            .is_none()
        {
            self.rocksdb_disable_direct_io_for_flush_and_compactions =
                Some(common.rocksdb_disable_direct_io_for_flush_and_compaction());
        }
        if self.rocksdb_disable_statistics.is_none() {
            self.rocksdb_disable_statistics = Some(common.rocksdb_disable_statistics());
        }
        if self.rocksdb_compaction_readahead_size.is_none() {
            self.rocksdb_compaction_readahead_size =
                Some(common.rocksdb_compaction_readahead_size());
        }
        if self.rocksdb_disable_auto_compactions.is_none() {
            self.rocksdb_disable_auto_compactions = Some(common.rocksdb_disable_auto_compactions());
        }
        if self.rocksdb_periodic_compaction.is_none() {
            self.rocksdb_periodic_compaction = common.rocksdb_periodic_compaction;
        }
        if self.rocksdb_statistics_level.is_none() {
            self.rocksdb_statistics_level = Some(common.rocksdb_statistics_level());
        }
        if self.rocksdb_log_level.is_none() {
            self.rocksdb_log_level = Some(common.rocksdb_log_level());
        }
        if self.rocksdb_log_keep_file_num.is_none() {
            self.rocksdb_log_keep_file_num = Some(common.rocksdb_log_keep_file_num());
        }
        if self.rocksdb_log_max_file_size.is_none() {
            self.rocksdb_log_max_file_size = Some(common.rocksdb_log_max_file_size());
        }
        if self.rocksdb_block_size.is_none() {
            self.rocksdb_block_size = Some(common.rocksdb_block_size());
        }
        if self.rocksdb_disable_wal_compression.is_none() {
            self.rocksdb_disable_wal_compression = Some(common.rocksdb_disable_wal_compression());
        }
        self.fold_deprecated_l0_l1_compression(common.rocksdb_compression());
        if self.rocksdb_compression.is_none() {
            self.rocksdb_compression = Some(common.rocksdb_compression());
        }
    }

    pub fn rocksdb_disable_direct_io_for_reads(&self) -> bool {
        self.rocksdb_disable_direct_io_for_reads.unwrap_or(true)
    }

    pub fn rocksdb_disable_direct_io_for_flush_and_compaction(&self) -> bool {
        self.rocksdb_disable_direct_io_for_flush_and_compactions
            .unwrap_or(true)
    }

    pub fn rocksdb_disable_statistics(&self) -> bool {
        self.rocksdb_disable_statistics.unwrap_or(false)
    }

    pub fn rocksdb_compaction_readahead_size(&self) -> NonZeroUsize {
        self.rocksdb_compaction_readahead_size
            .unwrap_or(NonZeroUsize::new(2_000_000).unwrap())
    }

    pub fn rocksdb_disable_auto_compactions(&self) -> bool {
        self.rocksdb_disable_auto_compactions.unwrap_or(false)
    }

    pub fn rocksdb_periodic_compaction_seconds(&self) -> Option<u64> {
        self.rocksdb_periodic_compaction.map(|v| v.as_secs())
    }

    pub fn rocksdb_statistics_level(&self) -> StatisticsLevel {
        self.rocksdb_statistics_level
            .unwrap_or(StatisticsLevel::ExceptDetailedTimers)
    }

    pub fn rocksdb_log_level(&self) -> RocksDbLogLevel {
        self.rocksdb_log_level.unwrap_or_default()
    }

    pub fn rocksdb_log_keep_file_num(&self) -> usize {
        self.rocksdb_log_keep_file_num.unwrap_or(1)
    }

    pub fn rocksdb_log_max_file_size(&self) -> NonZeroByteCount {
        self.rocksdb_log_max_file_size
            .unwrap_or(NonZeroByteCount::new(
                NonZeroUsize::new(64_000_000).expect("is valid size"),
            ))
    }

    pub fn rocksdb_block_size(&self) -> NonZeroUsize {
        self.rocksdb_block_size
            .unwrap_or(NonZeroUsize::new(64 * 1024).unwrap())
    }

    pub fn rocksdb_disable_wal_compression(&self) -> bool {
        self.rocksdb_disable_wal_compression.unwrap_or(false)
    }

    /// Compression of L0, the levels in between and the bottommost level.
    pub fn rocksdb_compression(&self) -> RocksDbCompression {
        self.rocksdb_compression.unwrap_or_default()
    }

    /// Folds the deprecated `rocksdb-disable-l0-l1-compression` into `rocksdb-compression` and
    /// warns about it. `true` turns off compression in L0 of `inherited`, the compression this
    /// section uses unless it sets `rocksdb-compression`, which then wins. `false` changes nothing.
    pub(super) fn fold_deprecated_l0_l1_compression(&mut self, inherited: RocksDbCompression) {
        if self.rocksdb_disable_l0_l1_compression.take() != Some(true) {
            return;
        }
        // we can't use tracing since config loading happens before tracing is initialized
        if self.rocksdb_compression.is_some() {
            eprintln!(
                "warning: 'rocksdb-disable-l0-l1-compression' is deprecated and will be removed in \
                 v1.9.0. It is ignored because 'rocksdb-compression' is set in the same section; \
                 please remove it."
            );
        } else {
            eprintln!(
                "warning: 'rocksdb-disable-l0-l1-compression' is deprecated and will be removed in \
                 v1.9.0. Please use 'rocksdb-compression' instead, whose first value is the \
                 compression of L0 (default: \"none,lz4,zstd\")."
            );
            self.rocksdb_compression = Some(RocksDbCompression {
                l0: RocksDbCompressionType::None,
                ..inherited
            });
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "RocksbStatistics"))]
#[serde(rename_all = "kebab-case")]
pub enum StatisticsLevel {
    /// Disable all metrics
    DisableAll,
    /// Disable timer stats, and skip histogram stats
    ExceptHistogramOrTimers,
    /// Skip timer stats
    ExceptTimers,
    /// Collect all stats except time inside mutex lock AND time spent on
    /// compression.
    ExceptDetailedTimers,
    /// Collect all stats except the counters requiring to get time inside the
    /// mutex lock.
    ExceptTimeForMutex,
    /// Collect all stats, including measuring duration of mutex operations.
    /// If getting time is expensive on the platform to run, it can
    /// reduce scalability to more threads, especially for writes.
    All,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "RocksbPerfStatisticsLevel"))]
#[serde(rename_all = "kebab-case")]
pub enum PerfStatsLevel {
    /// Disable perf stats
    Disable,
    /// Enables only count stats
    EnableCount,
    /// Count stats and enable time stats except for mutexes
    EnableTimeExceptForMutex,
    /// Other than time, also measure CPU time counters. Still don't measure
    /// time (neither wall time nor CPU time) for mutexes
    EnableTimeAndCPUTimeExceptForMutex,
    /// Enables count and time stats
    EnableTime,
}

/// # SST compression
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub enum RocksDbCompressionType {
    /// No compression.
    None,
    /// LZ4: fast, with a moderate compression ratio.
    Lz4,
    /// Zstandard: slower, with a higher compression ratio.
    Zstd,
}

/// Compression of L0, the levels in between and the bottommost level. Written as one to three
/// values, as a list or separated by commas, the last of which applies to the remaining levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RocksDbCompression {
    /// L0, written by flushes.
    pub l0: RocksDbCompressionType,
    /// The levels between L0 and the bottommost level.
    pub middle: RocksDbCompressionType,
    /// The last level that holds data.
    pub bottommost: RocksDbCompressionType,
}

impl Default for RocksDbCompression {
    fn default() -> Self {
        Self {
            l0: RocksDbCompressionType::None,
            middle: RocksDbCompressionType::Lz4,
            bottommost: RocksDbCompressionType::Zstd,
        }
    }
}

impl RocksDbCompression {
    const EXPECTED: &str = "one to three of `none`, `lz4` and `zstd`, for L0, the levels \
                             in between and the bottommost level";

    fn from_values(values: &[RocksDbCompressionType]) -> Option<Self> {
        match *values {
            [all] => Some(Self {
                l0: all,
                middle: all,
                bottommost: all,
            }),
            [l0, rest] => Some(Self {
                l0,
                middle: rest,
                bottommost: rest,
            }),
            [l0, middle, bottommost] => Some(Self {
                l0,
                middle,
                bottommost,
            }),
            _ => None,
        }
    }
}

impl FromStr for RocksDbCompression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let values = s
            .split(',')
            .map(|value| {
                RocksDbCompressionType::deserialize(value.trim().into_deserializer())
                    .map_err(|err: serde::de::value::Error| err.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_values(&values).ok_or_else(|| format!("expected {}, got `{s}`", Self::EXPECTED))
    }
}

impl Serialize for RocksDbCompression {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        [self.l0, self.middle, self.bottommost].serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RocksDbCompression {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RocksDbCompression;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(
                    f,
                    "{}, as a list or separated by commas",
                    RocksDbCompression::EXPECTED
                )
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                value.parse().map_err(E::custom)
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = Vec::with_capacity(3);
                while let Some(value) = seq.next_element()? {
                    values.push(value);
                }
                RocksDbCompression::from_values(&values)
                    .ok_or_else(|| serde::de::Error::invalid_length(values.len(), &self))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for RocksDbCompression {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "RocksDbCompression".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        let value = generator.subschema_for::<RocksDbCompressionType>();
        schemars::json_schema!({
            "description": "One to three of `none`, `lz4` and `zstd`, for L0, the levels in \
                            between and the bottommost level, as a list or separated by commas. \
                            The last value applies to the remaining levels.",
            "anyOf": [
                {
                    "type": "string",
                    "pattern": r"^\s*(none|lz4|zstd)\s*(,\s*(none|lz4|zstd)\s*){0,2}$",
                },
                {
                    "type": "array",
                    "items": value,
                    "minItems": 1,
                    "maxItems": 3,
                },
            ],
        })
    }
}

/// # RocksDB compaction
///
/// How RocksDB merges a database's files in the background, selected with `style`:
///
/// - `level` (default): leveled compaction. Data moves down levels that each hold about ten times
///   more than the one above. Disk usage stays within about 10% of the live data, but every value
///   is rewritten several times on each level on its way down.
/// - `universal`: universal compaction. Every flush adds a sorted run, and runs of similar size are
///   merged. Newer runs pile up next to the oldest and largest run until they reach
///   `max-size-amplification-percent` of its size, and then every run is merged into one. Values
///   are rewritten far less often, but the database takes more disk space.
///
/// With universal compaction, `rocksdb-compression` applies its bottommost value (Zstd by default)
/// to the oldest sorted run, its L0 value (none by default) to flushed files and to the runs merged
/// back into L0 while older L0 files remain, and its value for the levels in between (Lz4 by
/// default) to the other runs. Lz4 runs take more space, so full merges come sooner.
/// `rocksdb-compression = "none,zstd,zstd"` keeps them smaller, at a higher CPU cost per
/// compaction.
///
/// Switching an existing database from `level` to `universal` needs no migration: every non-empty
/// level becomes one sorted run. Switching back works too, after which compaction moves the data
/// down the levels again. Changes take effect on restart.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "style", rename_all = "kebab-case")]
pub enum RocksDbCompaction {
    /// Leveled compaction.
    #[default]
    Level,
    /// Universal compaction.
    Universal(UniversalCompactionOptions),
}

impl RocksDbCompaction {
    /// Number of files in L0 (leveled compaction) or of sorted runs (universal compaction) that
    /// triggers a compaction. `l0_num_compaction_trigger` applies to leveled compaction only.
    pub fn compaction_trigger(&self, l0_num_compaction_trigger: NonZeroU32) -> NonZeroU32 {
        match self {
            Self::Level => l0_num_compaction_trigger,
            Self::Universal(universal) => universal.sorted_runs_compaction_trigger,
        }
    }
}

/// # Universal compaction options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case", default)]
pub struct UniversalCompactionOptions {
    /// # Sorted runs that trigger a compaction
    ///
    /// Number of sorted runs at which a compaction starts. Every flush adds one. With universal
    /// compaction this is used instead of `rocksdb-l0-num-compaction-trigger`.
    ///
    /// Default: 8
    ///
    /// Since v1.8.2
    pub sorted_runs_compaction_trigger: NonZeroU32,

    /// # Maximum size amplification
    ///
    /// Merge every sorted run into one once the newer runs add up to this percentage of the oldest
    /// run. With 100, a database grows to up to about twice its live data before such a full merge.
    /// The merge writes a new copy of the live data before it deletes the old files, so the
    /// database briefly takes up to about three times its live data. Lower values keep the
    /// database closer to its live data but merge everything more often. RocksDB's own default is
    /// 200.
    ///
    /// Databases that grow at the same pace, such as the partition stores of a node, reach this
    /// point at about the same time, so several full merges can run at once.
    ///
    /// Default: 100
    ///
    /// Since v1.8.2
    pub max_size_amplification_percent: u32,

    /// # Size ratio
    ///
    /// How much larger, in percent, the next older sorted run may be than the runs picked so far
    /// to still join their merge. Higher values merge runs of more different sizes together.
    ///
    /// Default: 1
    ///
    /// Since v1.8.2
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub size_ratio: u32,

    /// # Minimum merge width
    ///
    /// The fewest sorted runs merged together. Values below 2 count as 2.
    ///
    /// Default: 2
    ///
    /// Since v1.8.2
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub min_merge_width: NonZeroU32,

    /// # Maximum merge width
    ///
    /// The most sorted runs merged together. Values below `min-merge-width` count as
    /// `min-merge-width`. Unlimited by default.
    ///
    /// Since v1.8.2
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_merge_width: Option<NonZeroU32>,

    /// # Allow trivial move
    ///
    /// Move files to their new sorted run instead of rewriting them when the runs being merged
    /// don't overlap. This saves rewriting data that is written in key order. Moved files keep
    /// their compression, so with the default `rocksdb-compression`, uncompressed L0 files can
    /// end up in lower levels.
    ///
    /// Default: false
    ///
    /// Since v1.8.2
    #[cfg_attr(feature = "schemars", schemars(skip))]
    pub allow_trivial_move: bool,

    /// # Maximum read amplification
    ///
    /// Upper limit on the number of sorted runs, which reads may have to check. `0` lets RocksDB
    /// pick the limit from the database size, `size-ratio` and the write buffer size. Values below
    /// `sorted-runs-compaction-trigger` count as `sorted-runs-compaction-trigger`. Unset by
    /// default: the limit is `sorted-runs-compaction-trigger`.
    ///
    /// Since v1.8.2
    #[cfg_attr(feature = "schemars", schemars(skip))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_read_amp: Option<u32>,
}

impl Default for UniversalCompactionOptions {
    fn default() -> Self {
        Self {
            sorted_runs_compaction_trigger: NonZeroU32::new(8).unwrap(),
            max_size_amplification_percent: 100,
            size_ratio: 1,
            min_merge_width: NonZeroU32::new(2).unwrap(),
            max_merge_width: None,
            allow_trivial_move: false,
            max_read_amp: None,
        }
    }
}

impl UniversalCompactionOptions {
    /// `min-merge-width`, at least 2.
    pub fn effective_min_merge_width(&self) -> u32 {
        self.min_merge_width.get().max(2)
    }

    /// `max-merge-width`, at least `min-merge-width`, or `None` if unlimited.
    pub fn effective_max_merge_width(&self) -> Option<u32> {
        self.max_merge_width
            .map(|max| max.get().max(self.effective_min_merge_width()))
    }

    /// `max-read-amp`: `0` to auto-tune, otherwise at least the compaction trigger, which RocksDB
    /// requires. `None` if unset.
    pub fn effective_max_read_amp(&self) -> Option<u32> {
        self.max_read_amp.map(|limit| match limit {
            0 => 0,
            limit => limit.max(self.sorted_runs_compaction_trigger.get()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sst_compression_config() {
        use RocksDbCompressionType::{Lz4, None as Off, Zstd};
        let layout = |l0, middle, bottommost| RocksDbCompression {
            l0,
            middle,
            bottommost,
        };
        let parse = |value: &str| {
            toml::from_str::<RocksDbOptions>(&format!("rocksdb-compression = {value}"))
                .map(|options| options.rocksdb_compression())
        };

        assert_eq!(
            RocksDbOptions::default().rocksdb_compression(),
            layout(Off, Lz4, Zstd)
        );
        // The last value applies to the remaining levels, in a string or in a list.
        assert_eq!(parse(r#""lz4""#).unwrap(), layout(Lz4, Lz4, Lz4));
        assert_eq!(parse(r#""lz4, none""#).unwrap(), layout(Lz4, Off, Off));
        assert_eq!(parse(r#"["zstd"]"#).unwrap(), layout(Zstd, Zstd, Zstd));
        assert_eq!(
            parse(r#"["none", "zstd", "lz4"]"#).unwrap(),
            layout(Off, Zstd, Lz4)
        );
        for invalid in [r#""""#, r#""snappy""#, "[]", r#"["lz4", 1]"#] {
            assert!(parse(invalid).is_err(), "{invalid}");
        }
        let too_many = parse(r#""lz4,lz4,lz4,lz4""#).unwrap_err().to_string();
        assert!(too_many.contains("the bottommost level"), "{too_many}");

        // Dumped as a list of all three values, which reads back the same.
        let options: RocksDbOptions =
            toml::from_str(r#"rocksdb-compression = "lz4,none""#).unwrap();
        let dumped = toml::to_string(&options).unwrap();
        assert!(
            dumped.contains(r#"rocksdb-compression = ["lz4", "none", "none"]"#),
            "{dumped}"
        );
        assert_eq!(
            toml::from_str::<RocksDbOptions>(&dumped)
                .unwrap()
                .rocksdb_compression(),
            options.rocksdb_compression()
        );
    }

    /// Each database section inherits `rocksdb-compression` unless it sets it, and the deprecated
    /// `rocksdb-disable-l0-l1-compression = true` turns off L0 compression on top of that.
    #[test]
    fn sst_compression_inheritance_across_sections() -> anyhow::Result<()> {
        use RocksDbCompressionType::{Lz4, None as Off, Zstd};

        use crate::config_loader::ConfigLoaderBuilder;

        let layout = |l0, middle, bottommost| RocksDbCompression {
            l0,
            middle,
            bottommost,
        };
        let default = RocksDbCompression::default();
        let lz4 = layout(Lz4, Lz4, Lz4);
        // config file, expected compression of worker.storage, log-server, metadata-server and
        // bifrost.local
        let cases = [
            ("", [default; 4]),
            (
                "rocksdb-compression = [\"lz4\"]\n\
                 [worker.storage]\nrocksdb-compression = \"none, zstd, zstd\"",
                [layout(Off, Zstd, Zstd), lz4, lz4, lz4],
            ),
            // `true` turns off L0 compression of what the section would use otherwise, and
            // `false` changes nothing.
            (
                "rocksdb-compression = \"lz4\"\n\
                 [worker.storage]\nrocksdb-disable-l0-l1-compression = false\n\
                 [log-server]\nrocksdb-disable-l0-l1-compression = true",
                [lz4, layout(Off, Lz4, Lz4), lz4, lz4],
            ),
            // `rocksdb-compression` set in the same section wins over the deprecated option.
            (
                "rocksdb-disable-l0-l1-compression = true\nrocksdb-compression = \"lz4\"\n\
                 [log-server]\nrocksdb-disable-l0-l1-compression = true\n\
                 rocksdb-compression = \"zstd\"",
                [lz4, layout(Zstd, Zstd, Zstd), lz4, lz4],
            ),
            (
                "rocksdb-disable-l0-l1-compression = true\n\
                 [metadata-server]\nrocksdb-compression = \"lz4,zstd\"",
                [default, default, layout(Lz4, Zstd, Zstd), default],
            ),
        ];

        let dir = restate_test_util::tempdir()?;
        for (i, (file, expected)) in cases.into_iter().enumerate() {
            let path = dir.path().join(format!("config-{i}.toml"));
            std::fs::write(&path, file)?;
            let config = ConfigLoaderBuilder::default()
                .path(Some(path))
                .build()?
                .load_once()?;
            // Each section holds its compression, so that dumps show it.
            let resolved = [
                &config.worker.storage.rocksdb,
                &config.log_server.rocksdb,
                &config.metadata_server.rocksdb,
                &config.bifrost.local.rocksdb,
            ]
            .map(|rocksdb| rocksdb.rocksdb_compression);
            assert_eq!(resolved, expected.map(Some), "config:\n{file}");
        }
        Ok(())
    }

    #[test]
    fn rocksdb_compaction_config() {
        let parse = |toml: &str| toml::from_str::<RocksDbCompaction>(toml);

        assert_eq!(RocksDbCompaction::default(), RocksDbCompaction::Level);
        assert_eq!(
            parse(r#"style = "level""#).unwrap(),
            RocksDbCompaction::Level
        );

        let RocksDbCompaction::Universal(universal) = parse(r#"style = "universal""#).unwrap()
        else {
            panic!("expected universal compaction");
        };
        assert_eq!(universal, UniversalCompactionOptions::default());

        let universal = parse(
            r#"
            style = "universal"
            sorted-runs-compaction-trigger = 4
            max-size-amplification-percent = 50
            max-merge-width = 10
            "#,
        )
        .unwrap();
        let RocksDbCompaction::Universal(options) = &universal else {
            panic!("expected universal compaction");
        };
        assert_eq!(options.max_size_amplification_percent, 50);
        assert_eq!(options.max_merge_width, NonZeroU32::new(10));
        assert_eq!(options.size_ratio, 1);
        let trigger = NonZeroU32::new(2).unwrap();
        assert_eq!(universal.compaction_trigger(trigger).get(), 4);
        assert_eq!(
            RocksDbCompaction::Level.compaction_trigger(trigger),
            trigger
        );
        assert_eq!(
            parse(&toml::to_string(&universal).unwrap()).unwrap(),
            universal
        );

        assert!(parse(r#"style = "fifo""#).is_err());

        // Dumped configurations only show the compaction when it is set.
        let mut storage = crate::config::StorageOptions::default();
        assert!(
            !toml::to_string(&storage)
                .unwrap()
                .contains("rocksdb-compaction")
        );
        storage.rocksdb_compaction = Some(RocksDbCompaction::Level);
        assert!(
            toml::to_string(&storage)
                .unwrap()
                .contains("style = \"level\"")
        );
        let log_server = crate::config::LogServerOptions::default();
        assert!(
            !toml::to_string(&log_server)
                .unwrap()
                .contains("rocksdb-compaction")
        );
        assert_eq!(log_server.rocksdb_compaction(), RocksDbCompaction::Level);
        let RocksDbCompaction::Universal(narrow) =
            parse("style = \"universal\"\nmin-merge-width = 1\nmax-merge-width = 1").unwrap()
        else {
            panic!("expected universal compaction");
        };
        assert_eq!(narrow.effective_min_merge_width(), 2);
        assert_eq!(narrow.effective_max_merge_width(), Some(2));
        assert_eq!(options.effective_max_merge_width(), Some(10));

        assert!(!options.allow_trivial_move);
        assert_eq!(options.effective_max_read_amp(), None);
        let read_amp = |limit: u32| UniversalCompactionOptions {
            max_read_amp: Some(limit),
            ..UniversalCompactionOptions::default()
        };
        assert_eq!(read_amp(0).effective_max_read_amp(), Some(0));
        assert_eq!(read_amp(3).effective_max_read_amp(), Some(8));
        assert_eq!(read_amp(20).effective_max_read_amp(), Some(20));

        // A config file sets the compaction per section, and other sections stay leveled.
        let dir = restate_test_util::tempdir().unwrap();
        let path = dir.path().join("restate.toml");
        std::fs::write(
            &path,
            "[worker.storage.rocksdb-compaction]\nstyle = \"universal\"\n\
             max-size-amplification-percent = 50",
        )
        .unwrap();
        let config = crate::config_loader::ConfigLoaderBuilder::default()
            .path(Some(path))
            .build()
            .unwrap()
            .load_once()
            .unwrap();
        assert_eq!(
            config.worker.storage.rocksdb_compaction(),
            RocksDbCompaction::Universal(UniversalCompactionOptions {
                max_size_amplification_percent: 50,
                ..Default::default()
            })
        );
        assert_eq!(
            config.log_server.rocksdb_compaction(),
            RocksDbCompaction::Level
        );
    }
}
