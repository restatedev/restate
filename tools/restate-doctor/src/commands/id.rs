// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! ID decoding commands for analyzing Restate resource identifiers

use std::str::FromStr;

use cling::prelude::*;
use ulid::Ulid;

use restate_types::identifiers::{
    AwakeableIdentifier, DeploymentId, ExternalSignalIdentifier, InvocationId, SnapshotId,
    SubscriptionId, TimestampAwareId, WithPartitionKey,
};
use restate_types::time::MillisSinceEpoch;

use crate::output::{StyledTable, c_println, c_title, comfy_table::Table};
use crate::util::colorize_id::{colorize_id, id_color_legend};

/// ID analysis commands
#[derive(Run, Subcommand, Clone)]
pub enum IdCommand {
    /// Decode a Restate resource ID and display its components
    Decode(Decode),
}

/// Decode a resource ID
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_decode")]
pub struct Decode {
    /// The ID to decode (e.g., inv_1fBuS9KMPNrt0zQBGdilypucn1EeT7AO5j)
    pub id: String,
}

pub async fn run_decode(cmd: &Decode) -> anyhow::Result<()> {
    let input = cmd.id.trim();

    // Try to detect the resource type from the prefix
    if let Some(prefix) = input.split('_').next() {
        match prefix {
            "inv" => return decode_invocation_id(input),
            "dp" => return decode_deployment_id(input),
            "sub" => return decode_subscription_id(input),
            "prom" => return decode_awakeable_id(input),
            "sign" => return decode_signal_id(input),
            "snap" => return decode_snapshot_id(input),
            _ => {}
        }
    }

    // No recognized prefix - try as raw ULID
    decode_raw_ulid(input)
}

/// Print the common header table with resource type, colorized input, and legend
fn print_header(resource_type: &str, input: &str) {
    let mut table = Table::new_styled();
    table.add_kv_row("Resource Type:", resource_type);
    table.add_kv_row("Input:", colorize_id(input));
    table.add_kv_row("ID colors:", id_color_legend());
    c_println!("{table}");
}

/// Print the raw bytes section for any byte-serializable ID
fn print_raw_bytes(bytes: &[u8]) {
    c_title!("📦", "Raw Bytes");
    let mut table = Table::new_styled();
    table.add_kv_row("Total Length:", format!("{} bytes", bytes.len()));
    table.add_kv_row("Hex:", hex_encode(bytes));
    c_println!("{table}");
}

/// Try to print ULID timestamp info if the value looks like a valid ULID timestamp.
/// Returns true if a timestamp was printed.
fn print_ulid_timestamp_if_valid(uuid_u128: u128) -> bool {
    if let Some(ts) = try_extract_ulid_timestamp(uuid_u128) {
        c_title!("🕐", "ULID Timestamp (if random invocation)");
        let mut table = Table::new_styled();
        table.add_kv_row("Timestamp:", format_timestamp(ts));
        c_println!("{table}");
        true
    } else {
        false
    }
}

/// Decode a ULID-based ID that implements TimestampAwareId (Deployment, Subscription, Snapshot)
fn decode_timestamp_id<T>(input: &str, resource_type: &str) -> anyhow::Result<()>
where
    T: FromStr + TimestampAwareId + IdWithBytes,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let id = T::from_str(input)?;

    print_header(resource_type, input);

    c_title!("🔍", "Decoded Components");
    let mut components = Table::new_styled();
    components.add_kv_row("Created At:", format_timestamp(id.timestamp()));
    c_println!("{components}");

    print_raw_bytes(&id.to_bytes());

    Ok(())
}

trait IdWithBytes {
    fn to_bytes(&self) -> [u8; 16];
}

impl IdWithBytes for DeploymentId {
    fn to_bytes(&self) -> [u8; 16] {
        DeploymentId::to_bytes(self)
    }
}

impl IdWithBytes for SubscriptionId {
    fn to_bytes(&self) -> [u8; 16] {
        SubscriptionId::to_bytes(self)
    }
}

impl IdWithBytes for SnapshotId {
    fn to_bytes(&self) -> [u8; 16] {
        SnapshotId::to_bytes(self)
    }
}

fn decode_invocation_id(input: &str) -> anyhow::Result<()> {
    let id = InvocationId::from_str(input)?;

    print_header("Invocation", input);

    c_title!("🔍", "Decoded Components");
    let mut components = Table::new_styled();
    components.add_kv_row("Partition Key:", id.partition_key());
    components.add_kv_row("Invocation UUID:", id.invocation_uuid());
    c_println!("{components}");

    // The InvocationUuid could be a ULID (random invocation) or a hash (deterministic)
    let uuid_u128: u128 = id.invocation_uuid().into();
    if !print_ulid_timestamp_if_valid(uuid_u128) {
        c_println!("Note: UUID appears to be a hash (deterministic invocation), not a ULID");
    }

    print_raw_bytes(&id.to_bytes());

    Ok(())
}

fn decode_deployment_id(input: &str) -> anyhow::Result<()> {
    decode_timestamp_id::<DeploymentId>(input, "Deployment")
}

fn decode_subscription_id(input: &str) -> anyhow::Result<()> {
    decode_timestamp_id::<SubscriptionId>(input, "Subscription")
}

fn decode_snapshot_id(input: &str) -> anyhow::Result<()> {
    decode_timestamp_id::<SnapshotId>(input, "Snapshot")
}

/// Decode IDs that contain an embedded InvocationId (Awakeable, Signal)
fn decode_invocation_based_id<T, F, E>(
    input: &str,
    resource_type: &str,
    extra_fields: F,
) -> anyhow::Result<()>
where
    T: FromStr<Err = E>,
    E: std::error::Error + Send + Sync + 'static,
    F: FnOnce(T, &mut Table) -> InvocationId,
{
    let id = T::from_str(input)?;

    print_header(resource_type, input);

    c_title!("🔍", "Decoded Components");
    let mut components = Table::new_styled();
    let invocation_id = extra_fields(id, &mut components);
    c_println!("{components}");

    // Try to extract timestamp from invocation UUID
    let uuid_u128: u128 = invocation_id.invocation_uuid().into();
    print_ulid_timestamp_if_valid(uuid_u128);

    Ok(())
}

fn decode_awakeable_id(input: &str) -> anyhow::Result<()> {
    decode_invocation_based_id::<AwakeableIdentifier, _, _>(
        input,
        "Awakeable (Promise)",
        |id, table| {
            let (invocation_id, entry_index) = id.into_inner();
            table.add_kv_row("Invocation ID:", invocation_id);
            table.add_kv_row("  Partition Key:", invocation_id.partition_key());
            table.add_kv_row("  UUID:", invocation_id.invocation_uuid());
            table.add_kv_row("Entry Index:", entry_index);
            invocation_id
        },
    )
}

fn decode_signal_id(input: &str) -> anyhow::Result<()> {
    decode_invocation_based_id::<ExternalSignalIdentifier, _, _>(input, "Signal", |id, table| {
        let (invocation_id, signal_id) = id.into_inner();
        table.add_kv_row("Invocation ID:", invocation_id);
        table.add_kv_row("  Partition Key:", invocation_id.partition_key());
        table.add_kv_row("  UUID:", invocation_id.invocation_uuid());
        table.add_kv_row("Signal ID:", format!("{signal_id:?}"));
        invocation_id
    })
}

fn decode_raw_ulid(input: &str) -> anyhow::Result<()> {
    let id = Ulid::from_string(input).map_err(|_| {
        anyhow::anyhow!(
            "Unrecognized ID format: '{input}'\n\n\
             Expected formats:\n  \
               - inv_1...  (Invocation ID)\n  \
               - dp_1...   (Deployment ID)\n  \
               - sub_1...  (Subscription ID)\n  \
               - prom_1... (Awakeable/Promise ID)\n  \
               - sign_1... (Signal ID)\n  \
               - snap_1... (Snapshot ID)\n  \
               - Raw ULID (26 character Crockford base32)"
        )
    })?;

    print_header("Raw ULID (unknown resource type)", input);

    c_title!("🔍", "Decoded Components");
    let mut components = Table::new_styled();
    components.add_kv_row(
        "Timestamp:",
        format_timestamp(MillisSinceEpoch::new(id.timestamp_ms())),
    );
    components.add_kv_row("Random:", id.random());
    c_println!("{components}");

    c_title!("📦", "Raw Bytes");
    let bytes = id.to_bytes();
    let mut raw = Table::new_styled();
    raw.add_kv_row("Total Length:", format!("{} bytes", bytes.len()));
    raw.add_kv_row("Hex:", hex_encode(&bytes));
    raw.add_kv_row("u128:", u128::from(id));
    c_println!("{raw}");

    Ok(())
}

/// Try to extract a ULID timestamp from a u128 value.
/// Returns None if the timestamp seems unreasonable (likely a hash, not a ULID).
fn try_extract_ulid_timestamp(value: u128) -> Option<MillisSinceEpoch> {
    // ULID structure: 48 bits timestamp + 80 bits random
    let timestamp_ms = (value >> 80) as u64;

    // Sanity check: timestamp should be reasonable (between year 2000 and 2100)
    const MIN_REASONABLE_TS: u64 = 946_684_800_000; // Year 2000
    const MAX_REASONABLE_TS: u64 = 4_102_444_800_000; // Year 2100

    if (MIN_REASONABLE_TS..=MAX_REASONABLE_TS).contains(&timestamp_ms) {
        Some(MillisSinceEpoch::new(timestamp_ms))
    } else {
        None
    }
}

/// Format a MillisSinceEpoch timestamp as a human-readable string using jiff
fn format_timestamp(ts: MillisSinceEpoch) -> String {
    format!("{} ({} ms)", ts.into_timestamp(), ts.as_u64())
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
