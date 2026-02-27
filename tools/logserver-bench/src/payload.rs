// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Payload generation: in-memory pool or pre-generated file for deterministic, zero-overhead runs.
//!
//! # Payload file format
//!
//! A payload file stores pre-materialized record batches so that benchmark runs
//! don't spend CPU on RNG during the measured phase and repeated runs use
//! identical data.
//!
//! ```text
//! ┌─────────────────── Header (fixed size) ──────────────────┐
//! │ magic: [u8; 8]       "RSBENCH\0"                         │
//! │ version: u32          1                                   │
//! │ payload_size: u32     body size per record (bytes)        │
//! │ key_style: u8         0=None, 1=Single, 2=Pair, 3=Range  │
//! │ batch_size: u32       records per batch                   │
//! │ num_batches: u32      total batches in file               │
//! │ seed: u64             RNG seed used to generate           │
//! │ _reserved: [u8; 7]    padding / future use                │
//! └──────────────────────────────────────────────────────────┘
//! ┌─────────────────── Body ─────────────────────────────────┐
//! │ For each batch (num_batches times):                       │
//! │   For each record (batch_size times):                     │
//! │     keys:  encoded keys (size depends on key_style)       │
//! │     body:  [u8; payload_size]                             │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! Keys encoding per style:
//! - None:  0 bytes
//! - Single: 8 bytes (u64 LE)
//! - Pair:   16 bytes (2 × u64 LE)
//! - Range:  16 bytes (lo u64 LE, hi u64 LE)

use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use restate_cli_util::{c_println, c_success};
use restate_serde_util::ByteCount;
use restate_types::logs::{Keys, Record};
use restate_types::storage::PolyBytes;
use restate_types::time::NanosSinceEpoch;

// ---------------------------------------------------------------------------
// CLI types
// ---------------------------------------------------------------------------

/// Controls how record keys are generated.
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum KeyStyleArg {
    /// No keys (Keys::None) — record matches all readers.
    #[default]
    None,
    /// Single u64 key per record.
    Single,
    /// Pair of u64 keys per record.
    Pair,
    /// Inclusive range of u64 keys per record.
    Range,
}

/// Specification for generating payloads with controlled characteristics.
#[derive(Debug, Clone, clap::Args)]
pub struct PayloadSpec {
    /// Payload body size (e.g. "512", "4KB", "1MiB").
    #[arg(long, default_value = "512")]
    pub payload_size: ByteCount,

    /// Entropy of the payload body (0.0 = all zeros / maximally compressible,
    /// 1.0 = random bytes / incompressible).
    #[arg(long, default_value = "1.0")]
    pub entropy: f64,

    /// Key style attached to each record.
    #[arg(long, default_value = "none")]
    pub key_style: KeyStyleArg,

    /// Seed for deterministic payload generation.
    /// If omitted, a random seed is used and printed at startup for reproducibility.
    #[arg(long)]
    pub seed: Option<u64>,
}

/// Options for the `generate-payload` subcommand.
#[derive(Debug, Clone, clap::Parser)]
pub struct GeneratePayloadOpts {
    /// Output file path.
    #[arg(long, short)]
    pub output: PathBuf,

    /// Number of records per batch (should match --records-per-batch on the benchmark).
    #[arg(long, default_value = "10")]
    pub records_per_batch: usize,

    /// Number of batches to generate.
    #[arg(long, default_value = "1024")]
    pub num_batches: usize,

    #[clap(flatten)]
    pub payload: PayloadSpec,
}

// ---------------------------------------------------------------------------
// PayloadPool — unified in-memory pool from either RNG or file
// ---------------------------------------------------------------------------

/// Pre-generated pool of record batches that benchmark tasks cycle through.
pub struct PayloadPool {
    records: Vec<Arc<[Record]>>,
    batch_size: usize,
    index: usize,
}

impl PayloadPool {
    /// Create a new pool by generating batches in-memory from the RNG.
    pub fn new(spec: &PayloadSpec, batch_size: usize, pool_batches: usize) -> Self {
        let seed = spec.seed.unwrap_or_else(|| rand::rng().random());
        let mut rng = StdRng::seed_from_u64(seed);

        let records: Vec<Arc<[Record]>> = (0..pool_batches)
            .map(|_| {
                let batch: Vec<Record> = (0..batch_size)
                    .map(|_| generate_record(&mut rng, spec))
                    .collect();
                batch.into()
            })
            .collect();

        Self {
            records,
            batch_size,
            index: 0,
        }
    }

    /// Load a pool from a previously generated payload file.
    ///
    /// The `expected_batch_size` is validated against the file header. Pass `None`
    /// to accept whatever batch size the file was generated with.
    pub fn from_file(path: &Path, expected_batch_size: Option<usize>) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);

        let header = FileHeader::read_from(&mut reader)?;
        if let Some(expected) = expected_batch_size {
            anyhow::ensure!(
                header.batch_size as usize == expected,
                "Payload file batch_size ({}) does not match --records-per-batch ({expected})",
                header.batch_size,
            );
        }

        let batch_size = header.batch_size as usize;
        let payload_size = header.payload_size as usize;
        let key_bytes = header.key_style.byte_size();

        let mut records = Vec::with_capacity(header.num_batches as usize);
        for _ in 0..header.num_batches {
            let mut batch = Vec::with_capacity(batch_size);
            for _ in 0..batch_size {
                let keys = read_keys(&mut reader, header.key_style)?;
                let mut body = vec![0u8; payload_size];
                reader.read_exact(&mut body)?;
                batch.push(Record::from_parts(
                    NanosSinceEpoch::now(),
                    keys,
                    PolyBytes::Bytes(Bytes::from(body)),
                ));
            }
            records.push(Arc::from(batch));
        }

        // Verify we consumed exactly the expected amount of data
        let expected_body_bytes = header.num_batches as u64
            * batch_size as u64
            * (key_bytes as u64 + payload_size as u64);
        let actual_file_size = std::fs::metadata(path)?.len();
        let expected_total = FileHeader::SIZE as u64 + expected_body_bytes;
        anyhow::ensure!(
            actual_file_size == expected_total,
            "Payload file size mismatch: expected {expected_total} bytes, got {actual_file_size}",
        );

        Ok(Self {
            records,
            batch_size,
            index: 0,
        })
    }

    /// Get the next batch of records, cycling through the pool.
    pub fn next_batch(&mut self) -> Arc<[Record]> {
        let batch = self.records[self.index].clone();
        self.index = (self.index + 1) % self.records.len();
        batch
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

// ---------------------------------------------------------------------------
// Payload file generation (the `generate-payload` subcommand)
// ---------------------------------------------------------------------------

/// Generate a payload file from the given options. Does not require TaskCenter.
pub fn generate_payload_file(opts: &GeneratePayloadOpts) -> anyhow::Result<()> {
    let batch_size = opts.records_per_batch.max(1);
    let seed = opts.payload.seed.unwrap_or_else(|| rand::rng().random());
    let mut rng = StdRng::seed_from_u64(seed);
    let key_style: FileKeyStyle = opts.payload.key_style.into();

    let header = FileHeader {
        payload_size: opts.payload.payload_size.as_usize() as u32,
        key_style,
        batch_size: batch_size as u32,
        num_batches: opts.num_batches as u32,
        seed,
    };

    let file = std::fs::File::create(&opts.output)?;
    let mut writer = BufWriter::new(file);

    header.write_to(&mut writer)?;

    let payload_size = opts.payload.payload_size.as_usize();
    let entropy = opts.payload.entropy;
    let total_records = opts.num_batches * batch_size;

    for _ in 0..opts.num_batches {
        for _ in 0..batch_size {
            let keys = generate_keys(&mut rng, opts.payload.key_style);
            write_keys(&mut writer, &keys)?;
            let body = generate_body(&mut rng, payload_size, entropy);
            writer.write_all(&body)?;
        }
    }
    writer.flush()?;

    let file_size = std::fs::metadata(&opts.output)?.len();
    c_success!(
        "Generated {} records ({} batches × {}) to {}",
        total_records,
        opts.num_batches,
        batch_size,
        opts.output.display(),
    );
    c_println!(
        "  payload={}, key_style={:?}, seed={seed}, file_size={}",
        opts.payload.payload_size,
        opts.payload.key_style,
        ByteCount::<true>::new(file_size),
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Record generation helpers
// ---------------------------------------------------------------------------

fn generate_record(rng: &mut StdRng, spec: &PayloadSpec) -> Record {
    let body = generate_body(rng, spec.payload_size.as_usize(), spec.entropy);
    let keys = generate_keys(rng, spec.key_style);
    Record::from_parts(NanosSinceEpoch::now(), keys, PolyBytes::Bytes(body))
}

fn generate_body(rng: &mut StdRng, size: usize, entropy: f64) -> Bytes {
    let entropy = entropy.clamp(0.0, 1.0);
    let mut buf = vec![0u8; size];
    let random_bytes = (size as f64 * entropy).ceil() as usize;
    if random_bytes > 0 {
        rng.fill_bytes(&mut buf[..random_bytes.min(size)]);
    }
    Bytes::from(buf)
}

fn generate_keys(rng: &mut StdRng, style: KeyStyleArg) -> Keys {
    match style {
        KeyStyleArg::None => Keys::None,
        KeyStyleArg::Single => Keys::Single(rng.random()),
        KeyStyleArg::Pair => Keys::Pair(rng.random(), rng.random()),
        KeyStyleArg::Range => {
            let a: u64 = rng.random();
            let b: u64 = rng.random();
            Keys::RangeInclusive(a.min(b)..=a.max(b))
        }
    }
}

// ---------------------------------------------------------------------------
// Payload file format
// ---------------------------------------------------------------------------

const MAGIC: [u8; 8] = *b"RSBENCH\0";
const VERSION: u32 = 1;

/// Key style tag stored in the file header (1 byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum FileKeyStyle {
    None = 0,
    Single = 1,
    Pair = 2,
    Range = 3,
}

impl FileKeyStyle {
    fn from_u8(v: u8) -> anyhow::Result<Self> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Single),
            2 => Ok(Self::Pair),
            3 => Ok(Self::Range),
            _ => anyhow::bail!("Unknown key style tag: {v}"),
        }
    }

    /// Number of bytes needed to encode keys of this style.
    fn byte_size(self) -> usize {
        match self {
            Self::None => 0,
            Self::Single => 8,
            Self::Pair | Self::Range => 16,
        }
    }
}

impl From<KeyStyleArg> for FileKeyStyle {
    fn from(k: KeyStyleArg) -> Self {
        match k {
            KeyStyleArg::None => Self::None,
            KeyStyleArg::Single => Self::Single,
            KeyStyleArg::Pair => Self::Pair,
            KeyStyleArg::Range => Self::Range,
        }
    }
}

/// Fixed-size file header (40 bytes).
struct FileHeader {
    payload_size: u32,
    key_style: FileKeyStyle,
    batch_size: u32,
    num_batches: u32,
    seed: u64,
}

impl FileHeader {
    /// Total header size in bytes.
    const SIZE: usize = 8 + 4 + 4 + 1 + 4 + 4 + 8 + 7; // = 40

    fn write_to<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&MAGIC)?;
        w.write_all(&VERSION.to_le_bytes())?;
        w.write_all(&self.payload_size.to_le_bytes())?;
        w.write_all(&[self.key_style as u8])?;
        w.write_all(&self.batch_size.to_le_bytes())?;
        w.write_all(&self.num_batches.to_le_bytes())?;
        w.write_all(&self.seed.to_le_bytes())?;
        w.write_all(&[0u8; 7])?; // reserved
        Ok(())
    }

    fn read_from<R: Read>(r: &mut R) -> anyhow::Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        anyhow::ensure!(magic == MAGIC, "Not a valid payload file (bad magic)");

        let mut buf4 = [0u8; 4];
        r.read_exact(&mut buf4)?;
        let version = u32::from_le_bytes(buf4);
        anyhow::ensure!(
            version == VERSION,
            "Unsupported payload file version: {version}"
        );

        r.read_exact(&mut buf4)?;
        let payload_size = u32::from_le_bytes(buf4);

        let mut buf1 = [0u8; 1];
        r.read_exact(&mut buf1)?;
        let key_style = FileKeyStyle::from_u8(buf1[0])?;

        r.read_exact(&mut buf4)?;
        let batch_size = u32::from_le_bytes(buf4);

        r.read_exact(&mut buf4)?;
        let num_batches = u32::from_le_bytes(buf4);

        let mut buf8 = [0u8; 8];
        r.read_exact(&mut buf8)?;
        let seed = u64::from_le_bytes(buf8);

        let mut reserved = [0u8; 7];
        r.read_exact(&mut reserved)?;

        Ok(Self {
            payload_size,
            key_style,
            batch_size,
            num_batches,
            seed,
        })
    }
}

fn write_keys<W: Write>(w: &mut W, keys: &Keys) -> std::io::Result<()> {
    match keys {
        Keys::None => {}
        Keys::Single(k) => w.write_all(&k.to_le_bytes())?,
        Keys::Pair(a, b) => {
            w.write_all(&a.to_le_bytes())?;
            w.write_all(&b.to_le_bytes())?;
        }
        Keys::RangeInclusive(r) => {
            w.write_all(&r.start().to_le_bytes())?;
            w.write_all(&r.end().to_le_bytes())?;
        }
    }
    Ok(())
}

fn read_keys<R: Read>(r: &mut R, style: FileKeyStyle) -> anyhow::Result<Keys> {
    let mut buf8 = [0u8; 8];
    match style {
        FileKeyStyle::None => Ok(Keys::None),
        FileKeyStyle::Single => {
            r.read_exact(&mut buf8)?;
            Ok(Keys::Single(u64::from_le_bytes(buf8)))
        }
        FileKeyStyle::Pair => {
            r.read_exact(&mut buf8)?;
            let a = u64::from_le_bytes(buf8);
            r.read_exact(&mut buf8)?;
            let b = u64::from_le_bytes(buf8);
            Ok(Keys::Pair(a, b))
        }
        FileKeyStyle::Range => {
            r.read_exact(&mut buf8)?;
            let lo = u64::from_le_bytes(buf8);
            r.read_exact(&mut buf8)?;
            let hi = u64::from_le_bytes(buf8);
            Ok(Keys::RangeInclusive(lo..=hi))
        }
    }
}
