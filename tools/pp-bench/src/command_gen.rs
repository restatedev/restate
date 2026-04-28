// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Command generation and file I/O for the pp-bench tool.
//!
//! # Command file format
//!
//! ```text
//! +------------------ Header (fixed, 64 bytes) ------------------+
//! | magic: [u8; 8]       "PPBENCH\0"                             |
//! | version: u32          1                                       |
//! | workload_source: u8   0=PatchState, 1=Invoke, 2=Extracted     |
//! | num_commands: u64     total commands in file                   |
//! | seed: u64             RNG seed (0 for extracted)               |
//! | start_lsn: u64        first LSN in file (0 for generated)     |
//! | end_lsn: u64          last LSN in file (0 for generated)      |
//! | partition_id: u16     partition ID (0 for generated)           |
//! | flags: u16            bit 0: has_real_lsns                     |
//! | _reserved: [u8; 5]    padding / future use                    |
//! +---------------------------------------------------------------+
//! +------------------ Body (per record) --------------------------+
//! | [if has_real_lsns] lsn: u64 (LE)  original LSN from Bifrost   |
//! | len: u32 (LE)       byte length of encoded Envelope            |
//! | data: [u8; len]     StorageCodec-encoded Envelope              |
//! +---------------------------------------------------------------+
//! ```

use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use bytes::{Bytes, BytesMut};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

use restate_cli_util::{c_println, c_success};
use restate_serde_util::ByteCount;
use restate_types::identifiers::{
    InvocationId, PartitionId, PartitionProcessorRpcRequestId, ServiceId, WithPartitionKey,
};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocation, Source, VirtualObjectHandlerType,
};
use restate_types::logs::Lsn;
use restate_types::state_mut::ExternalStateMutation;
use restate_types::storage::StorageCodec;
use restate_wal_protocol::{Command, Destination, Envelope, Header};

use crate::{GenerateOpts, InspectOpts, WorkloadSpec, WorkloadType};

// ---------------------------------------------------------------------------
// File format constants
// ---------------------------------------------------------------------------

const MAGIC: [u8; 8] = *b"PPBENCH\0";
const FORMAT_VERSION: u32 = 1;

/// Workload source tag stored in the file header.
pub const WORKLOAD_SOURCE_PATCH_STATE: u8 = 0;
pub const WORKLOAD_SOURCE_INVOKE: u8 = 1;
pub const WORKLOAD_SOURCE_EXTRACTED: u8 = 2;

/// Flag bits in the header `flags` field.
pub const FLAG_HAS_REAL_LSNS: u16 = 0x0001;

pub struct FileHeader {
    pub workload_source: u8,
    pub num_commands: u64,
    pub seed: u64,
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub partition_id: u16,
    pub flags: u16,
}

impl FileHeader {
    #[allow(dead_code)]
    const SIZE: usize = 64;

    pub fn has_real_lsns(&self) -> bool {
        self.flags & FLAG_HAS_REAL_LSNS != 0
    }

    pub fn write_to<W: Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&MAGIC)?;
        w.write_all(&FORMAT_VERSION.to_le_bytes())?;
        w.write_all(&[self.workload_source])?;
        w.write_all(&self.num_commands.to_le_bytes())?;
        w.write_all(&self.seed.to_le_bytes())?;
        w.write_all(&self.start_lsn.to_le_bytes())?;
        w.write_all(&self.end_lsn.to_le_bytes())?;
        w.write_all(&self.partition_id.to_le_bytes())?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&[0u8; 5])?; // reserved
        Ok(())
    }

    pub fn read_from<R: Read>(r: &mut R) -> anyhow::Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        anyhow::ensure!(
            magic == MAGIC,
            "Not a valid pp-bench command file (bad magic)"
        );

        let mut buf4 = [0u8; 4];
        r.read_exact(&mut buf4)?;
        let version = u32::from_le_bytes(buf4);
        anyhow::ensure!(
            version == FORMAT_VERSION,
            "Unsupported command file version: {version}"
        );

        let mut buf1 = [0u8; 1];
        r.read_exact(&mut buf1)?;
        let workload_source = buf1[0];

        let mut buf8 = [0u8; 8];
        r.read_exact(&mut buf8)?;
        let num_commands = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let seed = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let start_lsn = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let end_lsn = u64::from_le_bytes(buf8);

        let mut buf2 = [0u8; 2];
        r.read_exact(&mut buf2)?;
        let partition_id = u16::from_le_bytes(buf2);

        r.read_exact(&mut buf2)?;
        let flags = u16::from_le_bytes(buf2);

        let mut reserved = [0u8; 5];
        r.read_exact(&mut reserved)?;

        Ok(Self {
            workload_source,
            num_commands,
            seed,
            start_lsn,
            end_lsn,
            partition_id,
            flags,
        })
    }
}

// ---------------------------------------------------------------------------
// Loaded workload result
// ---------------------------------------------------------------------------

/// Result of loading commands from a file — carries optional real LSNs
/// and metadata from the file header.
pub struct LoadedWorkload {
    pub commands: Vec<Command>,
    pub lsns: Option<Vec<Lsn>>,
    pub start_lsn: Option<Lsn>,
    pub partition_id: Option<PartitionId>,
    pub workload_source: u8,
}

// ---------------------------------------------------------------------------
// Command generation
// ---------------------------------------------------------------------------

fn generate_one(rng: &mut StdRng, spec: &WorkloadSpec) -> Command {
    match spec.workload {
        WorkloadType::PatchState => generate_patch_state(rng, spec),
        WorkloadType::Invoke => generate_invoke(rng, spec),
    }
}

fn generate_patch_state(rng: &mut StdRng, spec: &WorkloadSpec) -> Command {
    let key_idx = rng.random_range(0..spec.num_keys);
    let service_id = ServiceId::new(None, "bench.PatchTarget", format!("key-{key_idx}"));

    let value_size = spec.value_size.as_usize();
    let mut state = HashMap::with_capacity(spec.state_entries as usize);
    for i in 0..spec.state_entries {
        let mut value = vec![0u8; value_size];
        rng.fill_bytes(&mut value);
        state.insert(Bytes::from(format!("state-{i}")), Bytes::from(value));
    }

    Command::PatchState(ExternalStateMutation {
        service_id,
        version: None,
        state,
    })
}

fn generate_invoke(rng: &mut StdRng, spec: &WorkloadSpec) -> Command {
    let key_idx = rng.random_range(0..spec.num_keys);
    let target = InvocationTarget::virtual_object(
        "bench.InvokeTarget",
        format!("key-{key_idx}"),
        "handler",
        VirtualObjectHandlerType::Exclusive,
    );
    let invocation_id = InvocationId::generate(&target, None);
    let mut invocation = ServiceInvocation::initialize(
        invocation_id,
        target,
        Source::Ingress(PartitionProcessorRpcRequestId::new()),
    );
    let arg_size: usize = rng.random_range(64..512);
    let mut arg_buf = vec![0u8; arg_size];
    rng.fill_bytes(&mut arg_buf);
    invocation.argument = Bytes::from(arg_buf);

    Command::Invoke(Box::new(invocation))
}

// ---------------------------------------------------------------------------
// Wrapping commands in Envelopes for serialization
// ---------------------------------------------------------------------------

fn wrap_in_envelope(command: Command) -> Envelope {
    let partition_key = match &command {
        Command::PatchState(mutation) => mutation.service_id.partition_key(),
        Command::Invoke(invoke) => invoke.partition_key(),
        _ => 0,
    };
    Envelope::new(
        Header {
            source: restate_wal_protocol::Source::Ingress {},
            dest: Destination::Processor {
                partition_key,
                dedup: None,
            },
        },
        command,
    )
}

// ---------------------------------------------------------------------------
// Encoding helpers (shared between generate and extract)
// ---------------------------------------------------------------------------

/// Encode and write a single envelope record, optionally with an LSN prefix.
pub fn write_record<W: Write>(
    writer: &mut W,
    envelope: &Envelope,
    lsn: Option<Lsn>,
    scratch: &mut BytesMut,
) -> anyhow::Result<()> {
    let encoded = StorageCodec::encode_and_split(envelope, scratch)
        .map_err(|e| anyhow::anyhow!("Failed to encode envelope: {e}"))?;
    if let Some(lsn) = lsn {
        writer.write_all(&u64::from(lsn).to_le_bytes())?;
    }
    writer.write_all(&(encoded.len() as u32).to_le_bytes())?;
    writer.write_all(&encoded)?;
    Ok(())
}

/// Read a single record, optionally with an LSN prefix.
fn read_record<R: Read>(
    reader: &mut R,
    has_real_lsns: bool,
) -> anyhow::Result<(Option<Lsn>, Envelope)> {
    let lsn = if has_real_lsns {
        let mut buf8 = [0u8; 8];
        reader.read_exact(&mut buf8)?;
        Some(Lsn::from(u64::from_le_bytes(buf8)))
    } else {
        None
    };

    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut data = vec![0u8; len];
    reader.read_exact(&mut data)?;

    let mut cursor = &data[..];
    let envelope: Envelope = StorageCodec::decode(&mut cursor)
        .map_err(|e| anyhow::anyhow!("Failed to decode envelope: {e}"))?;

    Ok((lsn, envelope))
}

// ---------------------------------------------------------------------------
// File writing (generate subcommand)
// ---------------------------------------------------------------------------

/// Generate a command file from the given options. Does not require TaskCenter.
pub fn generate_command_file(opts: &GenerateOpts) -> anyhow::Result<()> {
    let header = FileHeader {
        workload_source: match opts.spec.workload {
            WorkloadType::PatchState => WORKLOAD_SOURCE_PATCH_STATE,
            WorkloadType::Invoke => WORKLOAD_SOURCE_INVOKE,
        },
        num_commands: opts.num_commands,
        seed: opts.spec.seed,
        start_lsn: 0,
        end_lsn: 0,
        partition_id: 0,
        flags: 0,
    };

    let file = std::fs::File::create(&opts.output)?;
    let mut writer = BufWriter::new(file);
    header.write_to(&mut writer)?;

    let mut rng = StdRng::seed_from_u64(opts.spec.seed);
    let mut scratch = BytesMut::with_capacity(4096);
    for _ in 0..opts.num_commands {
        let cmd = generate_one(&mut rng, &opts.spec);
        let envelope = wrap_in_envelope(cmd);
        write_record(&mut writer, &envelope, None, &mut scratch)?;
    }
    writer.flush()?;

    let file_size = std::fs::metadata(&opts.output)?.len();
    c_success!(
        "Generated {} commands to {}",
        opts.num_commands,
        opts.output.display(),
    );
    c_println!(
        "  workload={:?}, seed={}, file_size={}",
        opts.spec.workload,
        opts.spec.seed,
        ByteCount::<true>::new(file_size),
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// File reading (used by CommandPool and workload)
// ---------------------------------------------------------------------------

/// Load commands from a pre-generated or extracted file.
pub fn load_commands_from_file(path: &Path) -> anyhow::Result<LoadedWorkload> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let header = FileHeader::read_from(&mut reader)?;
    let has_real_lsns = header.has_real_lsns();
    let num_commands = header.num_commands as usize;

    let mut commands = Vec::with_capacity(num_commands);
    let mut lsns = if has_real_lsns {
        Some(Vec::with_capacity(num_commands))
    } else {
        None
    };

    for _ in 0..num_commands {
        let (lsn, envelope) = read_record(&mut reader, has_real_lsns)?;
        commands.push(envelope.command);
        if let (Some(lsn_vec), Some(lsn)) = (&mut lsns, lsn) {
            lsn_vec.push(lsn);
        }
    }

    let start_lsn = if header.start_lsn > 0 {
        Some(Lsn::from(header.start_lsn))
    } else {
        None
    };
    let partition_id = if header.workload_source == WORKLOAD_SOURCE_EXTRACTED {
        Some(PartitionId::from(header.partition_id))
    } else {
        None
    };

    Ok(LoadedWorkload {
        commands,
        lsns,
        start_lsn,
        partition_id,
        workload_source: header.workload_source,
    })
}

/// Generate commands in-memory (no file I/O).
pub fn generate_commands_inline(spec: &WorkloadSpec, count: u64) -> Vec<Command> {
    let mut rng = StdRng::seed_from_u64(spec.seed);
    (0..count).map(|_| generate_one(&mut rng, spec)).collect()
}

// ---------------------------------------------------------------------------
// Extracted command file writing (used by extract subcommand)
// ---------------------------------------------------------------------------

/// Writer for extracted command files. Streams records one at a time, then
/// seeks back to update the header with the final count and LSN range.
pub struct ExtractedFileWriter {
    writer: BufWriter<std::fs::File>,
    scratch: BytesMut,
    num_commands: u64,
    first_lsn: Option<u64>,
    last_lsn: u64,
    partition_id: u16,
}

impl ExtractedFileWriter {
    pub fn create(path: &Path, partition_id: u16) -> anyhow::Result<Self> {
        let file = std::fs::File::create(path)?;
        let mut writer = BufWriter::new(file);

        // Write placeholder header — will be updated at finish()
        let header = FileHeader {
            workload_source: WORKLOAD_SOURCE_EXTRACTED,
            num_commands: 0,
            seed: 0,
            start_lsn: 0,
            end_lsn: 0,
            partition_id,
            flags: FLAG_HAS_REAL_LSNS,
        };
        header.write_to(&mut writer)?;

        Ok(Self {
            writer,
            scratch: BytesMut::with_capacity(4096),
            num_commands: 0,
            first_lsn: None,
            last_lsn: 0,
            partition_id,
        })
    }

    pub fn write_envelope(&mut self, lsn: Lsn, envelope: &Envelope) -> anyhow::Result<()> {
        write_record(&mut self.writer, envelope, Some(lsn), &mut self.scratch)?;
        let lsn_val = u64::from(lsn);
        if self.first_lsn.is_none() {
            self.first_lsn = Some(lsn_val);
        }
        self.last_lsn = lsn_val;
        self.num_commands += 1;
        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<u64> {
        self.writer.flush()?;

        // Seek back to beginning and rewrite the header with final counts
        let inner = self.writer.into_inner().map_err(|e| e.into_error())?;
        let mut file = inner;
        file.seek(SeekFrom::Start(0))?;

        let header = FileHeader {
            workload_source: WORKLOAD_SOURCE_EXTRACTED,
            num_commands: self.num_commands,
            seed: 0,
            start_lsn: self.first_lsn.unwrap_or(0),
            end_lsn: self.last_lsn,
            partition_id: self.partition_id,
            flags: FLAG_HAS_REAL_LSNS,
        };
        header.write_to(&mut file)?;
        file.flush()?;

        Ok(self.num_commands)
    }
}

// ---------------------------------------------------------------------------
// File inspection (inspect subcommand)
// ---------------------------------------------------------------------------

fn workload_source_name(tag: u8) -> &'static str {
    match tag {
        WORKLOAD_SOURCE_PATCH_STATE => "PatchState (generated)",
        WORKLOAD_SOURCE_INVOKE => "Invoke (generated)",
        WORKLOAD_SOURCE_EXTRACTED => "Extracted",
        _ => "Unknown",
    }
}

fn describe_command(cmd: &Command) -> String {
    match cmd {
        Command::PatchState(m) => {
            format!(
                "PatchState {{ service={}/{}, keys={} }}",
                m.service_id.service_name,
                m.service_id.key,
                m.state.len(),
            )
        }
        Command::Invoke(inv) => {
            format!(
                "Invoke {{ target={}, arg_len={} }}",
                inv.invocation_target,
                inv.argument.len(),
            )
        }
        other => other.name().to_string(),
    }
}

/// Inspect a command file: print header metadata and a sample of commands.
pub fn inspect_command_file(opts: &InspectOpts) -> anyhow::Result<()> {
    let file = std::fs::File::open(&opts.file)?;
    let file_size = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    let header = FileHeader::read_from(&mut reader)?;
    let has_real_lsns = header.has_real_lsns();

    c_println!("File:       {}", opts.file.display());
    c_println!("Size:       {}", ByteCount::<true>::new(file_size));
    c_println!("Format:     v{FORMAT_VERSION}");
    c_println!(
        "Source:     {}",
        workload_source_name(header.workload_source)
    );
    c_println!("Commands:   {}", header.num_commands);
    if header.seed > 0 {
        c_println!("Seed:       {}", header.seed);
    }
    if has_real_lsns {
        c_println!("LSN range:  {} .. {}", header.start_lsn, header.end_lsn);
        c_println!("Partition:  {}", header.partition_id);
    }

    if opts.num == 0 {
        return Ok(());
    }

    let to_show = (opts.num as u64).min(header.num_commands) as usize;
    c_println!();
    c_println!("First {} command(s):", to_show);

    let mut total_encoded_bytes: u64 = 0;
    let mut min_encoded_len: u32 = u32::MAX;
    let mut max_encoded_len: u32 = 0;

    for i in 0..header.num_commands as usize {
        // Read optional LSN
        let lsn = if has_real_lsns {
            let mut buf8 = [0u8; 8];
            reader.read_exact(&mut buf8)?;
            Some(u64::from_le_bytes(buf8))
        } else {
            None
        };

        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf);
        total_encoded_bytes += len as u64;
        min_encoded_len = min_encoded_len.min(len);
        max_encoded_len = max_encoded_len.max(len);

        let mut data = vec![0u8; len as usize];
        reader.read_exact(&mut data)?;

        if i < to_show {
            let mut cursor = &data[..];
            let envelope: Envelope = StorageCodec::decode(&mut cursor)
                .map_err(|e| anyhow::anyhow!("Failed to decode command {i}: {e}"))?;
            let lsn_prefix = lsn.map(|l| format!("lsn={l} ")).unwrap_or_default();
            c_println!(
                "  [{i}] {lsn_prefix}{} ({}B)",
                describe_command(&envelope.command),
                len
            );
        }
    }

    if header.num_commands as usize > to_show {
        c_println!("  ... ({} more)", header.num_commands as usize - to_show);
    }

    c_println!();
    if let Some(avg) = total_encoded_bytes.checked_div(header.num_commands) {
        c_println!(
            "Encoded size: total={}, avg={}, min={}, max={}",
            ByteCount::<true>::new(total_encoded_bytes),
            ByteCount::<true>::new(avg),
            ByteCount::<true>::new(min_encoded_len as u64),
            ByteCount::<true>::new(max_encoded_len as u64),
        );
    }

    Ok(())
}
