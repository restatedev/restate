// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The cluster's status saved to a file (`status --dump`), to look at later or elsewhere
//! (`--file`): a snapshot, and more samples of what changes by the second so that rates can be
//! computed.
//!
//! The file keeps what the cluster returned rather than the views built from it, so a newer
//! restatectl shows an older file with its own checks. It's JSON compressed with zstd: the
//! metadata as serde writes it, the nodes' gRPC responses as base64-encoded protobuf, which any
//! version decodes, and the SQL tables as rows.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Utc};
use prost::Message;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::time::MissedTickBehavior;

use restate_core::protobuf::node_ctl_svc::IdentResponse;
use restate_metadata_server_grpc::grpc::StatusResponse;
use restate_types::PlainNodeId;
use restate_types::logs::metadata::Logs;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::protobuf::cluster::ClusterState;

use super::{ClusterView, Sample, Snapshot, sql};
use crate::connection::{ConnectionInfo, ConnectionInfoError};

/// How many samples a dump takes, a second apart.
pub const SAMPLES: usize = 5;
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// The version of the file's format; it changes only when older versions can't read a file.
const FORMAT: u32 = 1;
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

pub struct Dump {
    /// the snapshot, with the first sample
    snapshot: Snapshot,
    /// the samples taken after it, oldest first
    later: Vec<Sample>,
}

impl Dump {
    /// Reads the cluster's status and the tables a dump keeps, then samples what changes by the
    /// second until there are `SAMPLES`.
    pub async fn collect(connection: &ConnectionInfo) -> Result<Self, ConnectionInfoError> {
        let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tick.tick().await;
        let snapshot = Snapshot::fetch(connection, sql::DUMPED).await?;
        let mut later = Vec::with_capacity(SAMPLES - 1);
        for _ in 1..SAMPLES {
            tick.tick().await;
            later.push(Sample::fetch(connection).await.0);
        }
        Ok(Self { snapshot, later })
    }

    pub fn cluster_name(&self) -> &str {
        self.snapshot.nodes_config.cluster_name()
    }

    pub fn captured_at(&self) -> DateTime<Utc> {
        self.snapshot.sample.captured_at
    }

    /// A view of the cluster per sample, oldest first.
    pub fn views(&self) -> Vec<ClusterView> {
        std::iter::once(self.snapshot.clone())
            .chain(self.later.iter().map(|sample| Snapshot {
                sample: sample.clone(),
                ..self.snapshot.clone()
            }))
            .map(ClusterView::build)
            .collect()
    }

    /// Writes the dump to `path`, and returns its size.
    pub fn write(&self, path: &Path) -> anyhow::Result<u64> {
        let json = serde_json::to_vec(&File::from(self))?;
        let compressed = zstd::encode_all(json.as_slice(), 0)?;
        std::fs::write(path, &compressed)
            .with_context(|| format!("can't write {}", path.display()))?;
        Ok(compressed.len() as u64)
    }

    pub fn read(path: &Path) -> anyhow::Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("can't read {}", path.display()))?;
        // also take the JSON decompressed, e.g. to look at it
        let json = if bytes.starts_with(&ZSTD_MAGIC) {
            zstd::decode_all(bytes.as_slice())
                .with_context(|| format!("{} is damaged", path.display()))?
        } else {
            bytes
        };
        let not_a_dump = || {
            format!(
                "{} isn't a file written by `restatectl status --dump`",
                path.display()
            )
        };
        let header: Header = serde_json::from_slice(&json).with_context(not_a_dump)?;
        if header.format > FORMAT {
            bail!(
                "{} was written by restatectl {}, which is newer than this one; update restatectl to \
                 read it",
                path.display(),
                header.written_by
            );
        }
        let file: File = serde_json::from_slice(&json).with_context(not_a_dump)?;
        Ok(file.into())
    }
}

/// The views of the samples in the dump at `path`, oldest first.
pub fn load(path: &Path) -> anyhow::Result<Vec<ClusterView>> {
    let mut views = Dump::read(path)?.views();
    for view in &mut views {
        view.file = Some(path.display().to_string());
    }
    Ok(views)
}

/// What every version of the format starts with.
#[derive(Deserialize)]
struct Header {
    format: u32,
    written_by: String,
}

#[derive(Serialize, Deserialize)]
struct File {
    format: u32,
    /// the restatectl version that wrote the file
    written_by: String,
    nodes_config: NodesConfiguration,
    partition_table: Option<PartitionTable>,
    logs: Option<Logs>,
    idents: BTreeMap<PlainNodeId, Proto<IdentResponse>>,
    metadata_servers: BTreeMap<PlainNodeId, Result<Proto<StatusResponse>, String>>,
    tables: sql::Tables,
    nodes_probed: bool,
    dialed: BTreeMap<PlainNodeId, String>,
    warnings: Vec<String>,
    /// oldest first
    samples: Vec<SampleFile>,
}

#[derive(Serialize, Deserialize)]
struct SampleFile {
    captured_at: DateTime<Utc>,
    cluster_state: Option<Proto<ClusterState>>,
    tables: sql::Tables,
    warnings: Vec<String>,
}

impl From<&Sample> for SampleFile {
    fn from(sample: &Sample) -> Self {
        Self {
            captured_at: sample.captured_at,
            cluster_state: sample.cluster_state.clone().map(Proto),
            tables: sample.tables.clone(),
            warnings: sample.warnings.clone(),
        }
    }
}

impl From<SampleFile> for Sample {
    fn from(sample: SampleFile) -> Self {
        Self {
            captured_at: sample.captured_at,
            cluster_state: sample.cluster_state.map(|Proto(state)| state),
            tables: sample.tables,
            warnings: sample.warnings,
        }
    }
}

impl From<&Dump> for File {
    fn from(dump: &Dump) -> Self {
        let snapshot = &dump.snapshot;
        Self {
            format: FORMAT,
            written_by: env!("CARGO_PKG_VERSION").to_owned(),
            nodes_config: snapshot.nodes_config.clone(),
            partition_table: snapshot.partition_table.clone(),
            logs: snapshot.logs.clone(),
            idents: snapshot
                .idents
                .iter()
                .map(|(id, ident)| (*id, Proto(ident.clone())))
                .collect(),
            metadata_servers: snapshot
                .metadata_servers
                .iter()
                .map(|(id, status)| (*id, status.clone().map(Proto)))
                .collect(),
            tables: snapshot.tables.clone(),
            nodes_probed: snapshot.nodes_probed,
            dialed: snapshot
                .dialed
                .iter()
                .map(|(id, address)| (*id, address.clone()))
                .collect(),
            warnings: snapshot.warnings.clone(),
            samples: std::iter::once(&snapshot.sample)
                .chain(&dump.later)
                .map(SampleFile::from)
                .collect(),
        }
    }
}

impl From<File> for Dump {
    fn from(file: File) -> Self {
        let mut samples = file.samples.into_iter().map(Sample::from);
        // a file has at least the snapshot's sample; without, the views lack the cluster state
        let first = samples.next().unwrap_or_else(|| Sample {
            captured_at: DateTime::UNIX_EPOCH,
            cluster_state: None,
            tables: sql::Tables::new(),
            warnings: vec!["the file has no sample of the cluster state".to_owned()],
        });
        Self {
            snapshot: Snapshot {
                nodes_config: file.nodes_config,
                partition_table: file.partition_table,
                logs: file.logs,
                idents: file
                    .idents
                    .into_iter()
                    .map(|(id, Proto(ident))| (id, ident))
                    .collect(),
                metadata_servers: file
                    .metadata_servers
                    .into_iter()
                    .map(|(id, status)| (id, status.map(|Proto(status)| status)))
                    .collect(),
                tables: file.tables,
                nodes_probed: file.nodes_probed,
                dialed: file.dialed.into_iter().collect(),
                warnings: file.warnings,
                sample: first,
            },
            later: samples.collect(),
        }
    }
}

/// A protobuf message, kept as its base64-encoded bytes.
struct Proto<T>(T);

impl<T: Message> Serialize for Proto<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(self.0.encode_to_vec()))
    }
}

impl<'de, T: Message + Default> Deserialize<'de> for Proto<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        let bytes = STANDARD.decode(encoded).map_err(D::Error::custom)?;
        T::decode(bytes.as_slice())
            .map(Proto)
            .map_err(D::Error::custom)
    }
}

/// A dump of `large_cluster` whose logs take 50 records a second, of 100 bytes each, which
/// every reader reads.
#[cfg(test)]
pub(crate) fn dump_of_large_cluster() -> Dump {
    let snapshot = super::tests::large_cluster_snapshot();
    let later = (1..SAMPLES)
        .map(|i| {
            let mut sample = snapshot.sample.clone();
            sample.captured_at += chrono::Duration::seconds(i as i64);
            let workers = sample.tables.get_mut("loglet_workers").unwrap();
            for row in workers.as_mut().unwrap() {
                let tail = row["known_global_tail"].as_u64().unwrap_or_default();
                row["known_global_tail"] = (tail + 50 * i as u64).into();
            }
            // and every reader reads them: 100 bytes each
            let readers = sample.tables.get_mut("bifrost_read_streams").unwrap();
            for row in readers.as_mut().unwrap() {
                for (column, per_record) in [("records_read", 1), ("bytes_read", 100)] {
                    let read = row[column].as_u64().unwrap_or_default();
                    row[column] = (read + 50 * per_record * i as u64).into();
                }
            }
            sample
        })
        .collect();
    Dump { snapshot, later }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A dump reads back as it was written, compressed or not, with a view per sample; files of a
    /// newer format, or that aren't dumps, are refused.
    #[test]
    fn dumps_read_back_as_written() {
        let dump = dump_of_large_cluster();
        let dir = restate_test_util::tempdir().unwrap();
        let json = |views: Vec<ClusterView>| serde_json::to_value(views).unwrap();
        let written = json(dump.views());
        assert_eq!(written.as_array().unwrap().len(), SAMPLES);

        let path = dir.path().join("status.json.zst");
        dump.write(&path).unwrap();
        assert_eq!(json(Dump::read(&path).unwrap().views()), written);
        let views = load(&path).unwrap();
        assert_eq!(views[0].file.as_deref(), path.to_str());
        // the log tails moved between the samples
        assert!(views[4].logs[0].tail_lsn > views[0].logs[0].tail_lsn);

        let plain = dir.path().join("status.json");
        let mut file = serde_json::to_value(File::from(&dump)).unwrap();
        std::fs::write(&plain, file.to_string()).unwrap();
        assert_eq!(json(Dump::read(&plain).unwrap().views()), written);

        file["format"] = (FORMAT + 1).into();
        std::fs::write(&plain, file.to_string()).unwrap();
        let err = Dump::read(&plain).err().unwrap().to_string();
        assert!(err.contains("newer than this one"), "{err}");
        std::fs::write(&plain, "{}").unwrap();
        let err = Dump::read(&plain).err().unwrap().to_string();
        assert!(err.contains("isn't a file written by"), "{err}");
    }
}
