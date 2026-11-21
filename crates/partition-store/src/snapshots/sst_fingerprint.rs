// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cheap fingerprints for RocksDB SST files.
//!
//! Incremental snapshots store every SST of a partition in a shared `ssts/` prefix under a key
//! derived from the file's fingerprint, so the fingerprint must be identical for identical files
//! and cheap enough to compute for every SST of every snapshot. Hashing whole files does not
//! scale, so for block-based tables written with `format_version` 6 we only hash the tail:
//!
//! ```text
//! xxh3-128, seed 1 (Algorithm::TailV6): file_size as u64 LE || file bytes[properties_block_offset..EOF]
//! ```
//!
//! RocksDB writes the properties block after the data, filter and index blocks, so the suffix holds
//! only the properties block, the metaindex block and the footer, about 2.5 KiB per SST. It is a
//! roll-up of values that identify the file rather than a hash of its contents:
//! * the properties block holds `db_id`, `db_session_id` and `orig_file_number`, from which RocksDB
//!   derives its SST unique id, guaranteed unique among files written by one process and
//!   probabilistically unique across hosts (`include/rocksdb/unique_id.h`), plus entry counts,
//!   sizes and sequence number bounds;
//! * the footer holds `base_context_checksum`, a random value drawn per file, and a checksum of
//!   the footer itself; the metaindex and properties blocks carry their own block checksums.
//!
//! SSTs are immutable once written, so a file's identity determines its contents. Export hard
//! links SSTs and import keeps their bytes, so a file keeps its fingerprint across a restore.
//!
//! Layout facts (RocksDB `table/format.h`, `table/format.cc` and
//! `table/block_based/block_based_table_builder.cc`):
//! * the footer occupies the last 53 bytes (`Footer::kNewVersionsEncodedLength`); for
//!   `format_version` >= 1 the format version is at `[41..45]` and the magic number at
//!   `[45..53]`. For `format_version` 6, `[13..17]` holds the metaindex block size.
//! * the metaindex block is written uncompressed immediately before the footer, followed by a
//!   5-byte block trailer whose first byte is the compression type.
//! * the metaindex maps block names to block handles: `rocksdb.properties` points at the
//!   properties block, encoded as varint64 offset and varint64 size.
//! * a block is a sequence of prefix-compressed entries followed by the restart offsets and the
//!   restart count (all u32 LE).
//!
//! Every other file - older or newer format versions, plain/cuckoo tables, truncated or
//! otherwise malformed files - is fingerprinted by hashing the whole file. That fallback is
//! correct but reads the file once, so `restate.partition_store.snapshots.fingerprint.fallback.total`
//! tracks how often it happens. Note that Restate writes `format_version` 6 tables
//! (`restate_rocksdb::configuration::create_default_block_options`), which is also the highest
//! version this module understands; a RocksDB upgrade that starts writing version 7 would move
//! every SST to the fallback path.
//!
//! The fingerprint does not cover the data, filter or index blocks. A file modified in place
//! without touching its tail would keep its fingerprint. RocksDB does not do that to live SSTs:
//! the one in-place rewrite it has, assigning a global sequence number on ingestion, patches the
//! properties block and so changes the fingerprint.

use std::fmt;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use tokio::io::AsyncReadExt;
use tracing::debug;
use xxhash_rust::xxh3::Xxh3;

use crate::metric_definitions::SNAPSHOT_FINGERPRINT_FALLBACK;

/// Length of the block-based table footer (`Footer::kNewVersionsEncodedLength`).
const FOOTER_LEN: u64 = 53;
/// Length of the trailer that follows every block, including the metaindex block.
const BLOCK_TRAILER_LEN: u64 = 5;
/// Magic number of block-based table files.
const BLOCK_BASED_MAGIC: u64 = 0x88e2_41b7_85f4_cff7;
/// Format version of the only tables whose tail layout this module understands.
const SUPPORTED_FORMAT_VERSION: u32 = 6;
/// Compression type byte in a block trailer for uncompressed blocks.
const NO_COMPRESSION: u8 = 0;
/// Metaindex entry pointing at the properties block.
const PROPERTIES_BLOCK_NAME: &[u8] = b"rocksdb.properties";
/// Fingerprint algorithms, used as the xxh3 seed so that each hashes in its own domain: keys
/// from different algorithms cannot collide by construction, and a new algorithm (for example
/// for a future table format) takes the next id. Never reuse or renumber an id.
#[derive(Clone, Copy)]
#[repr(u64)]
enum Algorithm {
    /// File size and the tail from the properties block, for block-based `format_version` 6.
    TailV6 = 1,
    /// The whole file, for every other file.
    WholeFile = 2,
}

impl Algorithm {
    fn hasher(self) -> Xxh3 {
        Xxh3::with_seed(self as u64)
    }
}

/// Bounds the memory and read size used when hashing.
const HASH_CHUNK_SIZE: usize = 256 * 1024;

/// Fingerprint of an SST file, rendered as 32 lowercase hex characters.
///
/// XXH3 is a non-cryptographic hash. We rely on a 128-bit digest's collision space being large
/// enough that random RocksDB SSTs do not collide in practice; the input is trusted data produced
/// by this process (not attacker-controlled), so chosen-prefix collision resistance is not
/// required. If snapshot inputs ever become reachable from untrusted sources, this hash must be
/// replaced with a cryptographic one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SstFingerprint(u128);

impl fmt::Display for SstFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

/// Computes the [`SstFingerprint`] of the SST file at `path`.
///
/// Falls back to hashing the whole file when the file does not have the block-based
/// `format_version` 6 layout this module understands. Only I/O errors are returned; a file that
/// merely fails to parse never fails the fingerprinting.
pub(crate) async fn sst_fingerprint(path: &Path) -> io::Result<SstFingerprint> {
    let blocking_path = path.to_path_buf();
    let parsed = tokio::task::spawn_blocking(move || fingerprint_rocksdb_layout(&blocking_path))
        .await
        .map_err(io::Error::other)?;

    match parsed {
        Ok(fingerprint) => Ok(SstFingerprint(fingerprint)),
        Err(ParseError::Io(err)) => Err(err),
        Err(err @ ParseError::Malformed(_)) => {
            metrics::counter!(SNAPSHOT_FINGERPRINT_FALLBACK).increment(1);
            debug!(
                path = %path.display(),
                reason = %err,
                "Falling back to hashing the whole SST file"
            );
            hash_whole_file(path).await
        }
    }
}

#[derive(Debug)]
enum ParseError {
    Io(io::Error),
    /// The file does not have the block-based format_version 6 tail layout.
    Malformed(&'static str),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Malformed(reason) => f.write_str(reason),
        }
    }
}

impl From<io::Error> for ParseError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// xxh3-128 over the file size and the file's tail from the properties block onwards.
fn fingerprint_rocksdb_layout(path: &Path) -> Result<u128, ParseError> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let properties_offset = properties_block_offset(&file, file_size)?;

    let mut hasher = Algorithm::TailV6.hasher();
    hasher.update(&file_size.to_le_bytes());
    hash_range(&file, properties_offset, file_size, &mut hasher)?;
    Ok(hasher.digest128())
}

/// Returns the offset of the properties block, read from the metaindex through the footer.
fn properties_block_offset(file: &File, file_size: u64) -> Result<u64, ParseError> {
    let (metaindex_offset, metaindex_size) = read_metaindex_handle(file, file_size)?;

    let metaindex_len = usize::try_from(metaindex_size)
        .map_err(|_| ParseError::Malformed("metaindex block does not fit in memory"))?;
    let mut metaindex = vec![0u8; metaindex_len];
    read_at(file, metaindex_offset, &mut metaindex)?;

    // The metaindex is always written uncompressed; its trailer holds the compression type in the
    // first byte, followed by the block checksum.
    let mut trailer = [0u8; BLOCK_TRAILER_LEN as usize];
    read_at(file, metaindex_offset + metaindex_size, &mut trailer)?;
    if trailer[0] != NO_COMPRESSION {
        return Err(ParseError::Malformed("metaindex block is compressed"));
    }

    let value = find_block_entry(&metaindex, PROPERTIES_BLOCK_NAME)?.ok_or(
        ParseError::Malformed("metaindex has no rocksdb.properties entry"),
    )?;
    let (properties_offset, properties_size) = decode_block_handle(value)?;

    // The properties block is written after the data, filter and index blocks, right before the
    // metaindex block.
    let properties_end = properties_offset
        .checked_add(properties_size)
        .ok_or(ParseError::Malformed("properties block handle overflows"))?;
    if properties_offset >= metaindex_offset || properties_end > metaindex_offset {
        return Err(ParseError::Malformed(
            "properties block handle points outside the table body",
        ));
    }

    Ok(properties_offset)
}

/// Reads the footer and returns the offset and size of the metaindex block.
fn read_metaindex_handle(file: &File, file_size: u64) -> Result<(u64, u64), ParseError> {
    if file_size < FOOTER_LEN + BLOCK_TRAILER_LEN {
        return Err(ParseError::Malformed(
            "file is smaller than a block-based table footer",
        ));
    }

    let mut footer = [0u8; FOOTER_LEN as usize];
    read_at(file, file_size - FOOTER_LEN, &mut footer)?;

    if u64_le(&footer[45..53]) != BLOCK_BASED_MAGIC {
        return Err(ParseError::Malformed("not a block-based table"));
    }
    if u32_le(&footer[41..45]) != SUPPORTED_FORMAT_VERSION {
        return Err(ParseError::Malformed(
            "unsupported block-based table format version",
        ));
    }

    // The metaindex block sits immediately before the footer, followed by its trailer.
    let metaindex_end = file_size - FOOTER_LEN - BLOCK_TRAILER_LEN;
    let metaindex_size = u64::from(u32_le(&footer[13..17]));
    if metaindex_size > metaindex_end {
        return Err(ParseError::Malformed(
            "metaindex block size extends past the start of the file",
        ));
    }

    Ok((metaindex_end - metaindex_size, metaindex_size))
}

/// Looks `name` up in a RocksDB block and returns its value.
///
/// Every offset is bounds checked against the block, so malformed input yields an error instead
/// of a panic.
fn find_block_entry<'a>(block: &'a [u8], name: &[u8]) -> Result<Option<&'a [u8]>, ParseError> {
    let Some(restart_count_at) = block.len().checked_sub(4) else {
        return Err(ParseError::Malformed(
            "block is shorter than its restart count",
        ));
    };
    let num_restarts = u32_le(&block[restart_count_at..]) as usize;
    let restarts_len = num_restarts
        .checked_mul(4)
        .and_then(|len| len.checked_add(4))
        .ok_or(ParseError::Malformed("block restart count overflows"))?;
    let data_end = block
        .len()
        .checked_sub(restarts_len)
        .ok_or(ParseError::Malformed(
            "block restart array extends past the block",
        ))?;

    let mut pos = 0;
    let mut last_key: Vec<u8> = Vec::new();
    while pos < data_end {
        let shared = decode_varint(block, &mut pos, data_end)? as usize;
        let non_shared = decode_varint(block, &mut pos, data_end)? as usize;
        let value_len = decode_varint(block, &mut pos, data_end)? as usize;

        let key_end = pos
            .checked_add(non_shared)
            .ok_or(ParseError::Malformed("block entry key overflows"))?;
        let value_end = key_end
            .checked_add(value_len)
            .ok_or(ParseError::Malformed("block entry value overflows"))?;
        if shared > last_key.len() || value_end > data_end {
            return Err(ParseError::Malformed("block entry extends past the block"));
        }

        let mut key = Vec::with_capacity(shared + non_shared);
        key.extend_from_slice(&last_key[..shared]);
        key.extend_from_slice(&block[pos..key_end]);
        if key == name {
            return Ok(Some(&block[key_end..value_end]));
        }

        last_key = key;
        pos = value_end;
    }

    Ok(None)
}

/// Decodes varint64 offset and size of a block handle.
fn decode_block_handle(value: &[u8]) -> Result<(u64, u64), ParseError> {
    let mut pos = 0;
    let offset = decode_varint(value, &mut pos, value.len())?;
    let size = decode_varint(value, &mut pos, value.len())?;
    Ok((offset, size))
}

/// Decodes a varint64 that must end at or before `end`.
fn decode_varint(bytes: &[u8], pos: &mut usize, end: usize) -> Result<u64, ParseError> {
    let mut result = 0u64;
    for shift in (0..64).step_by(7) {
        if *pos >= end {
            return Err(ParseError::Malformed("truncated varint in block"));
        }
        let byte = bytes[*pos];
        *pos += 1;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }

    Err(ParseError::Malformed("varint in block is too long"))
}

/// Feeds `file[start..end]` into `hasher` in bounded chunks.
fn hash_range(file: &File, start: u64, end: u64, hasher: &mut Xxh3) -> Result<(), ParseError> {
    let range_len = end.checked_sub(start).ok_or(ParseError::Malformed(
        "hash range starts past the file size",
    ))?;
    let range_len =
        usize::try_from(range_len).map_err(|_| ParseError::Malformed("hash range is too large"))?;

    let mut buf = vec![0u8; range_len.min(HASH_CHUNK_SIZE)];
    let mut pos = start;
    while pos < end {
        let want = ((end - pos) as usize).min(buf.len());
        read_at(file, pos, &mut buf[..want])?;
        hasher.update(&buf[..want]);
        pos += want as u64;
    }

    Ok(())
}

/// Reads exactly `buf.len()` bytes at `offset`, treating a short read as a parse failure.
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> Result<(), ParseError> {
    match file.read_exact_at(buf, offset) {
        Ok(()) => Ok(()),
        // The file shrank while we were reading it; retry the whole thing via the fallback.
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Err(ParseError::Malformed(
            "file is shorter than its layout claims",
        )),
        Err(err) => Err(ParseError::Io(err)),
    }
}

/// xxh3-128 over the whole file, used for files without the block-based format_version 6 tail.
async fn hash_whole_file(path: &Path) -> io::Result<SstFingerprint> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Algorithm::WholeFile.hasher();
    let mut buf = vec![0u8; HASH_CHUNK_SIZE];

    loop {
        let len = file.read(&mut buf).await?;
        if len == 0 {
            break;
        }
        hasher.update(&buf[..len]);
    }

    Ok(SstFingerprint(hasher.digest128()))
}

fn u32_le(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[..4].try_into().expect("four bytes"))
}

fn u64_le(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().expect("eight bytes"))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;

    use xxhash_rust::xxh3::Xxh3;

    use restate_rocksdb::configuration::create_default_block_options;
    use restate_types::config::RocksDbOptions;

    use super::*;

    /// Writes an SST with the given block-based table options.
    fn write_sst(
        path: &Path,
        block_options: &rocksdb::BlockBasedOptions,
        entries: &[(&[u8], &[u8])],
    ) -> anyhow::Result<()> {
        let mut options = rocksdb::Options::default();
        options.set_block_based_table_factory(block_options);

        let mut writer = rocksdb::SstFileWriter::create(&options);
        writer.open(path)?;
        for (key, value) in entries {
            writer.put(key, value)?;
        }
        writer.finish()?;
        Ok(())
    }

    #[tokio::test]
    async fn fingerprint_follows_the_rocksdb_layout() -> anyhow::Result<()> {
        let dir = restate_test_util::tempdir()?;
        let original = dir.path().join("original.sst");
        let copied = dir.path().join("copied.sst");
        let linked = dir.path().join("linked.sst");
        let other = dir.path().join("other.sst");

        // Restate's own table options, so that changing the format version they write fails here
        // instead of silently sending every SST to the whole-file fallback.
        let block_options = create_default_block_options(&RocksDbOptions::default(), None);
        write_sst(&original, &block_options, &[(b"a", b"1"), (b"b", b"2")])?;
        write_sst(&other, &block_options, &[(b"c", b"3"), (b"d", b"4")])?;
        std::fs::copy(&original, &copied)?;
        std::fs::hard_link(&original, &linked)?;

        // The parse-only path must succeed, i.e. the file must not need the fallback.
        let fingerprint = fingerprint_rocksdb_layout(&original).expect("v6 table to be parsed");
        assert_eq!(fingerprint, sst_fingerprint(&original).await?.0);
        assert_eq!(
            fingerprint,
            fingerprint_rocksdb_layout(&copied).expect("copied v6 table to be parsed")
        );
        assert_eq!(
            fingerprint,
            fingerprint_rocksdb_layout(&linked).expect("hard linked v6 table to be parsed")
        );
        assert_ne!(
            fingerprint,
            fingerprint_rocksdb_layout(&other).expect("other v6 table to be parsed")
        );

        // ... and it must be xxh3-128(size_le || bytes[properties_block_offset..EOF]).
        let properties_offset = {
            let file = File::open(&original)?;
            let file_size = file.metadata()?.len();
            properties_block_offset(&file, file_size).expect("properties block offset")
        };
        let bytes = std::fs::read(&original)?;
        let mut hasher = Xxh3::with_seed(1);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes[usize::try_from(properties_offset)?..]);
        assert_eq!(fingerprint, hasher.digest128());

        // The key must stay 32 lowercase hex characters.
        let key = SstFingerprint(fingerprint).to_string();
        assert_eq!(key.len(), 32);
        assert!(
            key.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );

        Ok(())
    }

    #[tokio::test]
    async fn fingerprint_falls_back_to_hashing_the_whole_file() -> anyhow::Result<()> {
        let dir = restate_test_util::tempdir()?;
        let older_format = dir.path().join("older_format.sst");
        let garbage = dir.path().join("garbage.sst");

        let mut block_options = rocksdb::BlockBasedOptions::default();
        block_options.set_format_version(5);
        write_sst(&older_format, &block_options, &[(b"a", b"1")])?;
        std::fs::write(&garbage, b"not an sst")?;

        for path in [&older_format, &garbage] {
            assert!(matches!(
                fingerprint_rocksdb_layout(path),
                Err(ParseError::Malformed(_))
            ));

            let bytes = std::fs::read(path)?;
            let mut hasher = Xxh3::with_seed(2);
            hasher.update(&bytes);
            assert_eq!(sst_fingerprint(path).await?.0, hasher.digest128());
        }

        Ok(())
    }
}
