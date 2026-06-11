// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! High-volume exactly-once stress test for the ingestion client.
//!
//! Ingests a large number of records (default 5M, override via `INGEST_STRESS_RECORDS`) whose
//! bodies each carry a per-partition monotonic sequence number and an increment. A mock partition
//! leader randomly injects `NotLeaderWithEpoch` (bumping the partition's leader epoch), which forces
//! the client to carry over and replay all in-flight batches against the new epoch. The mock server
//! deduplicates using a per-partition high-water-mark — exactly as a real partition processor does —
//! so that even an "apply-then-reject" (the dangerous case where a record was applied but the ack
//! never reached the client) is counted once. After the run, the sum of increments issued by the
//! client must equal the sum accumulated server-side.
//!
//! Example run command
//!
//! `INGEST_STRESS_RECORDS=5000000 cargo nextest run -p restate-ingestion-client --run-ignored all ingest_exactly_once_under_leadership_churn`

use std::num::NonZeroUsize;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{StreamExt, stream::FuturesUnordered};
use rand::{Rng, SeedableRng, rngs::SmallRng};
use test_log::test;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use restate_core::{
    TaskCenter, TaskCenterFutureExt, TestCoreEnvBuilder,
    network::{BackPressureMode, FailingConnector, ServiceMessage, ServiceStream},
    partitions::PartitionRouting,
};
use restate_types::{
    GenerationalNodeId, Version,
    identifiers::{LeaderEpoch, PartitionId, PartitionKey},
    logs::{HasRecordKeys, Keys},
    net::{
        ingest::{IngestRecord, ReceivedIngestRequest, ResponseStatus},
        partition_processor::PartitionLeaderService,
    },
    partitions::{
        PartitionTable,
        state::{LeadershipState, PartitionReplicaSetStates},
    },
    storage::{
        StorageCodec, StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode,
        StorageEncodeError,
    },
};

use crate::{IngestionClient, SessionOptions};

/// Number of partitions the load fans out across.
const NUM_PARTITIONS: u16 = 4;
/// Memory budget (bytes) handed to the ingestion client.
const MEMORY_BUDGET: usize = 10 * 1024 * 1024;
/// Batch size (bytes); deliberately small to force many batches and exercise pipelining.
const BATCH_SIZE: usize = 8 * 1024;
/// One-in-N chance that a batch triggers a simulated leadership change. Kept low so the run makes
/// progress and terminates.
const FAILURE_ODDS: u32 = 200;

/// Message body: a per-partition monotonic sequence number plus the increment to apply. Encoded as
/// three little-endian `u64`s — a trivial, dependency-free `StorageEncode`/`StorageDecode`.
#[derive(Debug, Clone)]
struct CounterMsg {
    partition_key: u64,
    seq: u64,
    increment: u64,
}

impl StorageEncode for CounterMsg {
    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::LengthPrefixedRawBytes
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        buf.put_u64_le(self.partition_key);
        buf.put_u64_le(self.seq);
        buf.put_u64_le(self.increment);
        Ok(())
    }
}

impl StorageDecode for CounterMsg {
    fn decode<B: Buf>(buf: &mut B, _kind: StorageCodecKind) -> Result<Self, StorageDecodeError> {
        if buf.remaining() < 3 * size_of::<u64>() {
            return Err(StorageDecodeError::DecodeValue(
                format!(
                    "insufficient data: expected {} bytes, got {}",
                    3 * size_of::<u64>(),
                    buf.remaining()
                )
                .into(),
            ));
        }
        Ok(Self {
            partition_key: buf.get_u64_le(),
            seq: buf.get_u64_le(),
            increment: buf.get_u64_le(),
        })
    }
}

impl HasRecordKeys for CounterMsg {
    fn record_keys(&self) -> Keys {
        Keys::Single(self.partition_key)
    }
}

/// Builds the test environment: an incoming-only core env with `NUM_PARTITIONS` equally-sized
/// partitions, all led by the local node at the initial epoch, plus an ingestion client for
/// [`CounterMsg`] bodies. Returns the server-side service stream, the client, a routing key per
/// partition, the shared replica-set states (so the mock server can announce epoch bumps), and the
/// local node id.
async fn init() -> (
    ServiceStream<PartitionLeaderService>,
    IngestionClient<FailingConnector, CounterMsg>,
    Vec<PartitionKey>,
    PartitionReplicaSetStates,
    GenerationalNodeId,
) {
    let partition_table =
        PartitionTable::with_equally_sized_partitions(Version::MIN, NUM_PARTITIONS);
    // Resolve a routing key per partition straight from the table we built (any key inside a
    // partition's range routes to it), avoiding any reliance on global metadata propagation.
    let keys: Vec<PartitionKey> = (0..NUM_PARTITIONS)
        .map(|p| {
            partition_table
                .get(&PartitionId::from(p))
                .expect("partition exists")
                .key_range
                .start()
        })
        .collect();

    let mut builder = TestCoreEnvBuilder::with_incoming_only_connector()
        .add_mock_nodes_config()
        .set_partition_table(partition_table);

    let states = PartitionReplicaSetStates::default();
    for p in 0..NUM_PARTITIONS {
        states.note_observed_leader(
            PartitionId::from(p),
            LeadershipState {
                current_leader: builder.my_node_id,
                current_leader_epoch: LeaderEpoch::INITIAL,
            },
        );
    }

    let svc = builder
        .router_builder
        .register_service::<PartitionLeaderService>(BackPressureMode::PushBack);
    let incoming = svc.start();

    let my_node_id = builder.my_node_id;
    let env = builder.build().await;
    let client = IngestionClient::new(
        env.networking,
        env.metadata.updateable_partition_table(),
        PartitionRouting::new(states.clone(), TaskCenter::current()),
        NonZeroUsize::new(MEMORY_BUDGET).unwrap(),
        SessionOptions::builder()
            .batch_size(NonZeroUsize::new(BATCH_SIZE).unwrap())
            .build()
            .unwrap(),
    );

    (incoming, client, keys, states, my_node_id)
}

/// Applies a batch's records to the server-side total, deduplicating via the per-partition
/// high-water-mark. Records whose sequence number is not strictly greater than the mark are
/// duplicates produced by a replay and are skipped.
fn apply(hwm: &mut u64, total: &mut u64, records: Vec<IngestRecord>) {
    for record in records {
        let mut buf: Bytes = record.record;
        let msg: CounterMsg =
            StorageCodec::decode(&mut buf).expect("record body decodes as CounterMsg");
        if msg.seq > *hwm {
            *total += msg.increment;
            *hwm = msg.seq;
        }
    }
}

/// Drives the service stream, applying records and randomly injecting leadership changes. Returns
/// `(per_partition_totals, leadership_bumps)` when cancelled.
async fn mock_server(
    mut stream: ServiceStream<PartitionLeaderService>,
    states: PartitionReplicaSetStates,
    my_node_id: GenerationalNodeId,
    seed: u64,
    cancel: CancellationToken,
) -> (Vec<u64>, u64) {
    let n = usize::from(NUM_PARTITIONS);
    let mut epoch = vec![LeaderEpoch::INITIAL; n];
    let mut hwm = vec![0u64; n];
    let mut totals = vec![0u64; n];
    let mut bumps = 0u64;
    let mut rng = SmallRng::seed_from_u64(seed);

    loop {
        let msg = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            msg = stream.next() => match msg {
                Some(msg) => msg,
                None => break,
            },
        };

        let ServiceMessage::Rpc(rpc) = msg else {
            continue;
        };

        let partition = rpc
            .sort_code()
            .expect("ingest request carries a partition sort code");
        let idx = partition as usize;
        let partition_id = PartitionId::from(partition as u16);
        let typed = rpc.into_typed::<ReceivedIngestRequest>();
        let (reply, body) = typed.split();

        let current = LeadershipState {
            current_leader: my_node_id,
            current_leader_epoch: epoch[idx],
        };

        // Atomically reject stale in-flight batches sent against a superseded epoch.
        if body.target_leader_epoch != Some(epoch[idx]) {
            reply.send(
                ResponseStatus::NotLeaderWithEpoch {
                    of: partition_id,
                    last_seen_leadership_state: current,
                }
                .into(),
            );
            continue;
        }

        if rng.random_ratio(1, FAILURE_ODDS) {
            // Half the injections apply the batch before rejecting (the "apply-then-reject" case
            // the dedup high-water-mark must absorb on replay); the other half reject cleanly.
            if rng.random_bool(0.5) {
                apply(&mut hwm[idx], &mut totals[idx], body.records);
            }
            epoch[idx] = epoch[idx].next();
            bumps += 1;
            let new_state = LeadershipState {
                current_leader: my_node_id,
                current_leader_epoch: epoch[idx],
            };
            states.note_observed_leader(partition_id, new_state);
            reply.send(
                ResponseStatus::NotLeaderWithEpoch {
                    of: partition_id,
                    last_seen_leadership_state: new_state,
                }
                .into(),
            );
        } else {
            apply(&mut hwm[idx], &mut totals[idx], body.records);
            reply.send(ResponseStatus::Ack.into());
        }
    }

    (totals, bumps)
}

/// Ingests `count` records to partition `idx` (via `key`) with monotonic sequence numbers, keeping a
/// bounded number of commits in flight. Returns `(idx, sum_of_increments)` it issued.
async fn produce(
    mut client: IngestionClient<FailingConnector, CounterMsg>,
    idx: usize,
    key: PartitionKey,
    count: u64,
) -> (usize, u64) {
    let mut local_total = 0u64;
    let mut pending = FuturesUnordered::new();

    for seq in 1..=count {
        let increment = (seq % 7) + 1;
        local_total += increment;

        let mut ingest_fut = client.ingest(
            key,
            CounterMsg {
                partition_key: key,
                seq,
                increment,
            },
        );

        loop {
            tokio::select! {
                commit = &mut ingest_fut => {
                    let commit = commit.expect("ingest accepts the record");
                    pending.push(commit);
                    break;
                }
                Some(result) = pending.next() => {
                    result.expect("record commits");
                }
            }
        }
    }

    while let Some(result) = pending.next().await {
        result.expect("record commits");
    }

    (idx, local_total)
}

/// Number of records assigned to partition `p`, distributing any remainder over the first few
/// partitions.
fn count_for(total: u64, p: u16) -> u64 {
    let base = total / u64::from(NUM_PARTITIONS);
    let extra = u64::from(p < (total % u64::from(NUM_PARTITIONS)) as u16);
    base + extra
}

#[ignore = "high-volume stress test; run explicitly with --ignored"]
#[test(restate_core::test(flavor = "multi_thread", worker_threads = 4))]
async fn ingest_exactly_once_under_leadership_churn() {
    let total: u64 = std::env::var("INGEST_STRESS_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000);

    // Seed the fault-injection RNG from the environment, or pick one at random. Printing it lets a
    // failing run be reproduced deterministically via `INGEST_STRESS_SEED`.
    let seed: u64 = std::env::var("INGEST_STRESS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(rand::random);
    println!("INGEST_STRESS_SEED={seed} (set this env var to reproduce this run)");

    let (stream, client, keys, states, my_node_id) = init().await;

    let cancel = CancellationToken::new();
    let server =
        tokio::spawn(mock_server(stream, states, my_node_id, seed, cancel.clone()).in_current_tc());

    let mut producers = JoinSet::new();
    for p in 0..NUM_PARTITIONS {
        let idx = usize::from(p);
        let client = client.clone();
        let key = keys[idx];
        let count = count_for(total, p);
        producers.spawn(async move { produce(client, idx, key, count).await }.in_current_tc());
    }

    let mut client_totals = vec![0u64; usize::from(NUM_PARTITIONS)];
    while let Some(result) = producers.join_next().await {
        let (idx, local_total) = result.expect("producer task joins");
        client_totals[idx] = local_total;
    }

    // Every record has committed (acked exactly once), so the server has processed everything.
    cancel.cancel();
    let (server_totals, bumps) = server.await.expect("server task joins");
    client.close();

    // Each partition keeps its own total: the server-side accumulation must match the client's
    // issued increments partition-by-partition, not just in aggregate.
    assert_eq!(
        client_totals, server_totals,
        "per-partition server totals must equal the increments issued by the client"
    );
    assert!(
        bumps > 0,
        "expected the run to exercise at least one leadership change"
    );
}
