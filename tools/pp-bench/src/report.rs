// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmark reporting: periodic interval lines, end-of-run summary, and system snapshot helpers.
//! Adapted from logserver-bench/src/report.rs for the partition processor benchmark.

use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use comfy_table::Table;
use hdrhistogram::Histogram;
use rocksdb::statistics::Ticker;

use restate_cli_util::c_println;
use restate_cli_util::ui::console::{Styled, StyledTable};
use restate_cli_util::ui::stylesheet::Style;
use restate_rocksdb::{DbName, RocksDbManager};
use restate_serde_util::ByteCount;
use restate_time_util::FriendlyDuration;

// ---------------------------------------------------------------------------
// Human-friendly formatting helpers
// ---------------------------------------------------------------------------

fn fmt_count(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn fmt_duration_short(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{nanos}ns")
    } else if nanos < 1_000_000 {
        format!("{:.0}us", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.1}ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", nanos as f64 / 1_000_000_000.0)
    }
}

fn friendly(d: Duration) -> FriendlyDuration {
    FriendlyDuration::from(d)
}

fn bytes(b: u64) -> ByteCount {
    ByteCount::<true>::new(b)
}

fn bytes_usize(b: usize) -> ByteCount {
    ByteCount::<true>::new(b as u64)
}

// ---------------------------------------------------------------------------
// CPU usage via getrusage (Unix only)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
pub struct CpuUsage {
    pub user_secs: f64,
    pub system_secs: f64,
}

impl CpuUsage {
    #[cfg(unix)]
    pub fn now() -> Self {
        unsafe {
            let mut usage: libc::rusage = std::mem::zeroed();
            libc::getrusage(libc::RUSAGE_SELF, &mut usage);
            Self {
                user_secs: usage.ru_utime.tv_sec as f64
                    + usage.ru_utime.tv_usec as f64 / 1_000_000.0,
                system_secs: usage.ru_stime.tv_sec as f64
                    + usage.ru_stime.tv_usec as f64 / 1_000_000.0,
            }
        }
    }

    #[cfg(not(unix))]
    pub fn now() -> Self {
        Self::default()
    }

    pub fn delta(&self, prev: &Self) -> Self {
        Self {
            user_secs: self.user_secs - prev.user_secs,
            system_secs: self.system_secs - prev.system_secs,
        }
    }

    pub fn total(&self) -> f64 {
        self.user_secs + self.system_secs
    }

    fn as_friendly(&self) -> (FriendlyDuration, FriendlyDuration) {
        (
            FriendlyDuration::from(Duration::from_secs_f64(self.user_secs)),
            FriendlyDuration::from(Duration::from_secs_f64(self.system_secs)),
        )
    }
}

// ---------------------------------------------------------------------------
// Jemalloc stats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
pub struct JemallocSnapshot {
    pub allocated: usize,
    pub active: usize,
    pub resident: usize,
    pub mapped: usize,
    pub retained: usize,
    pub metadata: usize,
}

impl JemallocSnapshot {
    #[cfg(not(target_env = "msvc"))]
    pub fn read() -> Self {
        use tikv_jemalloc_ctl::{epoch, stats};
        let _ = epoch::advance();
        Self {
            allocated: stats::allocated::read().unwrap_or(0),
            active: stats::active::read().unwrap_or(0),
            resident: stats::resident::read().unwrap_or(0),
            mapped: stats::mapped::read().unwrap_or(0),
            retained: stats::retained::read().unwrap_or(0),
            metadata: stats::metadata::read().unwrap_or(0),
        }
    }

    #[cfg(target_env = "msvc")]
    pub fn read() -> Self {
        Self::default()
    }
}

// ---------------------------------------------------------------------------
// RocksDB ticker snapshot (cumulative counters)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
pub struct RocksDbTickers {
    pub stall_micros: u64,
    pub wal_synced: u64,
    pub wal_bytes: u64,
    pub flush_write_bytes: u64,
    pub compact_read_bytes: u64,
    pub compact_write_bytes: u64,
    pub block_cache_hit: u64,
    pub block_cache_miss: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

impl RocksDbTickers {
    pub fn read(db_name: &str) -> Self {
        let manager = RocksDbManager::get();
        let Some(db) = manager.get_db(DbName::new(db_name)) else {
            return Self::default();
        };
        Self {
            stall_micros: db.get_ticker_count(Ticker::StallMicros),
            wal_synced: db.get_ticker_count(Ticker::WalFileSynced),
            wal_bytes: db.get_ticker_count(Ticker::WalFileBytes),
            flush_write_bytes: db.get_ticker_count(Ticker::FlushWriteBytes),
            compact_read_bytes: db.get_ticker_count(Ticker::CompactReadBytes),
            compact_write_bytes: db.get_ticker_count(Ticker::CompactWriteBytes),
            block_cache_hit: db.get_ticker_count(Ticker::BlockCacheHit),
            block_cache_miss: db.get_ticker_count(Ticker::BlockCacheMiss),
            bytes_written: db.get_ticker_count(Ticker::BytesWritten),
            bytes_read: db.get_ticker_count(Ticker::BytesRead),
        }
    }

    pub fn delta(&self, prev: &Self) -> Self {
        Self {
            stall_micros: self.stall_micros.saturating_sub(prev.stall_micros),
            wal_synced: self.wal_synced.saturating_sub(prev.wal_synced),
            wal_bytes: self.wal_bytes.saturating_sub(prev.wal_bytes),
            flush_write_bytes: self
                .flush_write_bytes
                .saturating_sub(prev.flush_write_bytes),
            compact_read_bytes: self
                .compact_read_bytes
                .saturating_sub(prev.compact_read_bytes),
            compact_write_bytes: self
                .compact_write_bytes
                .saturating_sub(prev.compact_write_bytes),
            block_cache_hit: self.block_cache_hit.saturating_sub(prev.block_cache_hit),
            block_cache_miss: self.block_cache_miss.saturating_sub(prev.block_cache_miss),
            bytes_written: self.bytes_written.saturating_sub(prev.bytes_written),
            bytes_read: self.bytes_read.saturating_sub(prev.bytes_read),
        }
    }
}

// ---------------------------------------------------------------------------
// Combined system snapshot at a point in time
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct SystemSnapshot {
    pub cpu: CpuUsage,
    pub jemalloc: JemallocSnapshot,
    pub rocksdb: RocksDbTickers,
}

impl SystemSnapshot {
    pub fn capture(db_name: &str) -> Self {
        Self {
            cpu: CpuUsage::now(),
            jemalloc: JemallocSnapshot::read(),
            rocksdb: RocksDbTickers::read(db_name),
        }
    }
}

// ---------------------------------------------------------------------------
// Shared counters that benchmarks update, the reporter reads
// ---------------------------------------------------------------------------

/// Benchmark counters. Single-threaded only — uses `Cell` rather than atomics
/// because the apply loop and the reporter task share one thread and only
/// interleave at `.await` points.
pub struct BenchCounters {
    commands: Cell<u64>,
    batches: Cell<u64>,
}

impl BenchCounters {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            commands: Cell::new(0),
            batches: Cell::new(0),
        })
    }

    /// Add `n` to the commands counter.
    pub fn inc_commands(&self, n: u64) {
        self.commands.set(self.commands.get() + n);
    }

    /// Increment the batches counter by 1.
    pub fn inc_batches(&self) {
        self.batches.set(self.batches.get() + 1);
    }

    pub fn commands(&self) -> u64 {
        self.commands.get()
    }

    pub fn batches(&self) -> u64 {
        self.batches.get()
    }
}

// ---------------------------------------------------------------------------
// Interval reporter
// ---------------------------------------------------------------------------

pub struct LatencyCollector {
    tx: tokio::sync::mpsc::UnboundedSender<u64>,
}

impl LatencyCollector {
    pub fn record(&self, nanos: u64) {
        let _ = self.tx.send(nanos);
    }
}

pub struct IntervalReporter {
    interval: FriendlyDuration,
    db_name: String,
    counters: Rc<BenchCounters>,
    rx: tokio::sync::mpsc::UnboundedReceiver<u64>,
}

impl IntervalReporter {
    pub fn new(
        interval: FriendlyDuration,
        db_name: &str,
        counters: Rc<BenchCounters>,
    ) -> (Self, LatencyCollector) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                interval,
                db_name: db_name.to_owned(),
                counters,
                rx,
            },
            LatencyCollector { tx },
        )
    }

    pub async fn run(mut self) -> Histogram<u64> {
        let mut combined = Histogram::<u64>::new(3).unwrap();
        let mut interval_hist = Histogram::<u64>::new(3).unwrap();
        let mut prev_snap = SystemSnapshot::capture(&self.db_name);
        let mut prev_cmds: u64 = 0;
        let start = Instant::now();

        c_println!(
            "{:>8} {:>9} {:>9} {:>8} {:>8} {:>8} {:>5} {:>7} {:>7} {:>7}",
            "ELAPSED",
            "CMDS",
            "CMDS/S",
            "P50",
            "P99",
            "P999",
            "CPU%",
            "STALLS",
            "ALLOC",
            "RSS",
        );

        let mut ticker = tokio::time::interval(*self.interval);
        ticker.tick().await;

        loop {
            ticker.tick().await;
            let wall = self.interval.as_secs_f64();
            let elapsed = start.elapsed();

            interval_hist.reset();
            while let Ok(nanos) = self.rx.try_recv() {
                let _ = interval_hist.record(nanos);
                let _ = combined.record(nanos);
            }

            let cur_cmds = self.counters.commands();
            let delta_cmds = cur_cmds.saturating_sub(prev_cmds);
            prev_cmds = cur_cmds;

            let snap = SystemSnapshot::capture(&self.db_name);
            let cpu_delta = snap.cpu.delta(&prev_snap.cpu);
            let stall_delta = snap
                .rocksdb
                .stall_micros
                .saturating_sub(prev_snap.rocksdb.stall_micros);
            prev_snap = snap;

            let cmds_per_sec = delta_cmds as f64 / wall;
            let cpu_pct = if wall > 0.0 {
                cpu_delta.total() / wall * 100.0
            } else {
                0.0
            };

            let p50 = if interval_hist.is_empty() {
                "-".to_owned()
            } else {
                fmt_duration_short(Duration::from_nanos(
                    interval_hist.value_at_percentile(50.0),
                ))
            };
            let p99 = if interval_hist.is_empty() {
                "-".to_owned()
            } else {
                fmt_duration_short(Duration::from_nanos(
                    interval_hist.value_at_percentile(99.0),
                ))
            };
            let p999 = if interval_hist.is_empty() {
                "-".to_owned()
            } else {
                fmt_duration_short(Duration::from_nanos(
                    interval_hist.value_at_percentile(99.9),
                ))
            };

            let stall_str = if stall_delta == 0 {
                "0".to_owned()
            } else {
                friendly(Duration::from_micros(stall_delta)).to_string()
            };

            c_println!(
                "[{:>5.0}s] {:>9} {:>9.0} {:>8} {:>8} {:>8} {:>4.0}% {:>7} {:>7} {:>7}",
                elapsed.as_secs_f64(),
                fmt_count(delta_cmds),
                cmds_per_sec,
                p50,
                p99,
                p999,
                cpu_pct,
                stall_str,
                bytes_usize(snap.jemalloc.allocated),
                bytes_usize(snap.jemalloc.resident),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// End-of-run summary
// ---------------------------------------------------------------------------

pub struct RunSummary<'a> {
    pub workload: &'a str,
    pub total_commands: u64,
    pub total_batches: u64,
    pub batch_size: usize,
    pub warmup_commands: u64,
    pub wall_time: FriendlyDuration,
    pub latencies: &'a Histogram<u64>,
    pub start_snapshot: &'a SystemSnapshot,
    pub end_snapshot: &'a SystemSnapshot,
    pub db_name: &'a str,
    pub raw_rocksdb_stats: bool,
}

pub fn print_summary(s: &RunSummary<'_>) {
    let wall = s.wall_time.as_secs_f64();

    restate_cli_util::c_title!(">>", "Results");
    {
        let mut table = Table::new_styled();
        table.add_kv_row(
            "Config:",
            format!(
                "workload={}, batch_size={}, warmup={}",
                s.workload, s.batch_size, s.warmup_commands,
            ),
        );
        table.add_kv_row(
            "Total:",
            format!(
                "{} commands ({} batches) in {}",
                fmt_count(s.total_commands),
                fmt_count(s.total_batches),
                s.wall_time,
            ),
        );
        table.add_kv_row(
            "Throughput:",
            format!(
                "{:.0} commands/s | {:.0} batches/s",
                s.total_commands as f64 / wall,
                s.total_batches as f64 / wall,
            ),
        );
        c_println!("{}", table);
    }

    // Latency
    restate_cli_util::c_title!(">>", "Latency (batch commit)");
    if !s.latencies.is_empty() {
        let mut table = Table::new_styled();
        for (label, pct) in [
            ("p50", 50.0),
            ("p90", 90.0),
            ("p99", 99.0),
            ("p99.9", 99.9),
            ("p99.99", 99.99),
            ("max", 100.0),
        ] {
            table.add_kv_row(
                &format!("{label}:"),
                friendly(Duration::from_nanos(s.latencies.value_at_percentile(pct))).to_string(),
            );
        }
        table.add_kv_row("samples:", s.latencies.len().to_string());
        c_println!("{}", table);
    } else {
        c_println!("  {}", Styled(Style::Notice, "(no samples)"));
    }

    // CPU
    {
        let cpu = s.end_snapshot.cpu.delta(&s.start_snapshot.cpu);
        let (user, sys) = cpu.as_friendly();
        restate_cli_util::c_title!(">>", "CPU");
        let mut table = Table::new_styled();
        table.add_kv_row("User:", user.to_string());
        table.add_kv_row("System:", sys.to_string());
        table.add_kv_row("Wall:", s.wall_time.to_string());
        if wall > 0.0 {
            table.add_kv_row("Avg util:", format!("{:.1}%", cpu.total() / wall * 100.0));
        }
        c_println!("{}", table);
    }

    // Memory
    {
        let je = &s.end_snapshot.jemalloc;
        restate_cli_util::c_title!(">>", "Memory (jemalloc)");
        let mut table = Table::new_styled();
        table.add_kv_row("Allocated:", bytes_usize(je.allocated).to_string());
        table.add_kv_row("Active:", bytes_usize(je.active).to_string());
        table.add_kv_row("Resident:", bytes_usize(je.resident).to_string());
        table.add_kv_row("Mapped:", bytes_usize(je.mapped).to_string());
        table.add_kv_row("Retained:", bytes_usize(je.retained).to_string());
        c_println!("{}", table);
    }

    // RocksDB
    {
        let rocks = s.end_snapshot.rocksdb.delta(&s.start_snapshot.rocksdb);
        restate_cli_util::c_title!(">>", "RocksDB");
        let mut table = Table::new_styled();
        table.add_kv_row(
            "Write stall:",
            friendly(Duration::from_micros(rocks.stall_micros)).to_string(),
        );
        table.add_kv_row(
            "WAL:",
            format!(
                "{} syncs, {} written",
                fmt_count(rocks.wal_synced),
                bytes(rocks.wal_bytes),
            ),
        );
        table.add_kv_row(
            "Flush:",
            format!("{} written", bytes(rocks.flush_write_bytes)),
        );
        table.add_kv_row(
            "Compaction:",
            format!(
                "read={} written={}",
                bytes(rocks.compact_read_bytes),
                bytes(rocks.compact_write_bytes),
            ),
        );

        let cache_total = rocks.block_cache_hit + rocks.block_cache_miss;
        if cache_total > 0 {
            table.add_kv_row(
                "Block cache:",
                format!(
                    "{:.1}% hit ({} / {})",
                    rocks.block_cache_hit as f64 / cache_total as f64 * 100.0,
                    fmt_count(rocks.block_cache_hit),
                    fmt_count(cache_total),
                ),
            );
        } else {
            table.add_kv_row("Block cache:", "(no accesses)");
        }
        c_println!("{}", table);
    }

    // Raw stats dump
    if s.raw_rocksdb_stats {
        let manager = RocksDbManager::get();
        if let Some(db) = manager.get_db(DbName::new(s.db_name))
            && let Some(stats_str) = db.get_statistics_str()
        {
            c_println!();
            c_println!("--- Raw RocksDB Statistics ---");
            c_println!("{stats_str}");
        }
    }

    c_println!();
}

// ---------------------------------------------------------------------------
// JSON output for CI
// ---------------------------------------------------------------------------

pub fn print_json_summary(s: &RunSummary<'_>) {
    let wall = s.wall_time.as_secs_f64();
    let cpu = s.end_snapshot.cpu.delta(&s.start_snapshot.cpu);
    let je = &s.end_snapshot.jemalloc;

    let json = serde_json::json!({
        "workload": s.workload,
        "batch_size": s.batch_size,
        "total_commands": s.total_commands,
        "total_batches": s.total_batches,
        "warmup_commands": s.warmup_commands,
        "wall_secs": wall,
        "commands_per_sec": s.total_commands as f64 / wall,
        "batches_per_sec": s.total_batches as f64 / wall,
        "latency_ns": {
            "p50": s.latencies.value_at_percentile(50.0),
            "p90": s.latencies.value_at_percentile(90.0),
            "p99": s.latencies.value_at_percentile(99.0),
            "p999": s.latencies.value_at_percentile(99.9),
            "p9999": s.latencies.value_at_percentile(99.99),
            "max": s.latencies.max(),
        },
        "cpu": {
            "user_secs": cpu.user_secs,
            "system_secs": cpu.system_secs,
            "avg_util_pct": if wall > 0.0 { cpu.total() / wall * 100.0 } else { 0.0 },
        },
        "jemalloc": {
            "allocated_bytes": je.allocated,
            "resident_bytes": je.resident,
        },
    });

    println!("{}", serde_json::to_string_pretty(&json).unwrap());
}
