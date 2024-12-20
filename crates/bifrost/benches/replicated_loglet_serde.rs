// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pprof::criterion::{Output, PProfProfiler};
use pprof::flamegraph::Options;
use prost::Message as _;
use rand::distributions::Alphanumeric;
use rand::{random, Rng};

use restate_bifrost::InputRecord;
use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber, ProducerId};
use restate_types::identifiers::{InvocationId, LeaderEpoch, PartitionProcessorRpcRequestId};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocation, ServiceInvocationSpanContext,
};
use restate_types::logs::{LogId, LogletId, Record};
use restate_types::net::codec::{serialize_message, MessageBodyExt, WireDecode};
use restate_types::net::replicated_loglet::{Append, CommonRequestHeader};
use restate_types::protobuf::node::Message;
use restate_types::time::MillisSinceEpoch;
use restate_types::GenerationalNodeId;
use restate_wal_protocol::{Command, Destination, Envelope};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub fn flamegraph_options<'a>() -> Options<'a> {
    #[allow(unused_mut)]
    let mut options = Options::default();
    if cfg!(target_os = "macos") {
        // Ignore different thread origins to merge traces. This seems not needed on Linux.
        options.base = vec!["__pthread_joiner_wake".to_string(), "_main".to_string()];
    }
    options
}

fn rand_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn generate_envelope() -> Arc<Envelope> {
    let source_partition_id = random::<u16>().into();
    let partition_key = random();
    let leader_epoch = LeaderEpoch::from(random::<u64>());
    let node_id = GenerationalNodeId::new(random(), random());
    let idempotency_key: ByteString = rand_string(15).into();

    let request_id = PartitionProcessorRpcRequestId::new();
    let inv_source = restate_types::invocation::Source::Ingress(request_id);
    let handler: ByteString = format!("aFunction_{}", rand_string(1)).into();

    let header = restate_wal_protocol::Header {
        source: restate_wal_protocol::Source::Processor {
            partition_id: source_partition_id,
            partition_key: Some(partition_key),
            leader_epoch,
            node_id: node_id.as_plain(),
            generational_node_id: Some(node_id),
        },
        dest: Destination::Processor {
            partition_key,
            dedup: Some(DedupInformation {
                producer_id: ProducerId::self_producer(),
                sequence_number: restate_storage_api::deduplication_table::DedupSequenceNumber::Esn(
                    EpochSequenceNumber {
                        leader_epoch: LeaderEpoch::from(random::<u64>()),
                        sequence_number: random(),
                    },
                ),
            }),
        },
    };
    let command = Command::Invoke(ServiceInvocation {
        invocation_id: InvocationId::generate(
            &InvocationTarget::service("MyWonderfulService", handler.clone()),
            Some(&idempotency_key),
        ),
        invocation_target: InvocationTarget::Service {
            name: "AnotherService".into(),
            handler,
        },
        argument: "DataSent".to_string().into(),
        source: inv_source,
        span_context: ServiceInvocationSpanContext::default(),
        headers: vec![restate_types::invocation::Header::new(
            "content-type",
            "application/json",
        )],
        execution_time: Some(MillisSinceEpoch::after(Duration::from_secs(10))),
        completion_retention_duration: Some(Duration::from_secs(10)),
        idempotency_key: Some(idempotency_key),
        response_sink: Some(
            restate_types::invocation::ServiceInvocationResponseSink::Ingress { request_id },
        ),
        submit_notification_sink: Some(
            restate_types::invocation::SubmitNotificationSink::Ingress { request_id },
        ),
    });

    Envelope::new(header, command).into()
}

fn serialize_append_message(payloads: Arc<[Record]>) -> anyhow::Result<Message> {
    let append_message = Append {
        header: CommonRequestHeader {
            log_id: LogId::from(12u16),
            segment_index: 2.into(),
            loglet_id: LogletId::new(12u16.into(), 4.into()),
        },
        payloads,
    };

    let body = serialize_message(
        append_message,
        restate_types::net::ProtocolVersion::Flexbuffers,
    )
    .unwrap();

    let message = Message {
        header: Some(restate_types::protobuf::node::Header {
            my_nodes_config_version: Some(restate_types::protobuf::common::Version { value: 5 }),
            my_logs_version: None,
            my_schema_version: None,
            my_partition_table_version: None,
            msg_id: random(),
            in_response_to: None,
            span_context: None,
        }),
        body: Some(body),
    };
    Ok(message)
}

fn deserialize_append_message(serialized_message: Bytes) -> anyhow::Result<Append> {
    let protocol_version = restate_types::net::ProtocolVersion::Flexbuffers;
    let msg = Message::decode(serialized_message)?;
    let body = msg.body.unwrap();
    // we ignore non-deserializable messages (serde errors, or control signals in drain)
    let mut msg_body = body.try_as_binary_body(restate_types::net::ProtocolVersion::Flexbuffers)?;
    Ok(Append::decode(&mut msg_body.payload, protocol_version)?)
}

fn replicated_loglet_append_serde(c: &mut Criterion) {
    let mut group = c.benchmark_group("replicated-loglet-serde");
    let batch: Vec<Record> = vec![generate_envelope(); 10]
        .into_iter()
        .map(|r| InputRecord::from(r).into_record())
        .collect();

    let payloads: Arc<[Record]> = batch.into();

    let mut buf = BytesMut::new();
    let message = serialize_append_message(payloads.clone()).unwrap();
    message.encode(&mut buf).unwrap();
    let serialized = buf.freeze();

    group
        .sample_size(40)
        .measurement_time(Duration::from_secs(20))
        .bench_function("serialize", |bencher| {
            bencher.iter(|| {
                let mut buf = BytesMut::new();
                let message = black_box(serialize_append_message(payloads.clone()).unwrap());
                black_box(message.encode(&mut buf)).unwrap();
            });
        })
        .bench_function("deserialize", |bencher| {
            bencher.iter(|| {
                black_box(deserialize_append_message(serialized.clone())).unwrap();
            });
        });
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(997, Output::Flamegraph(Some(flamegraph_options()))));
    targets = replicated_loglet_append_serde
);
criterion_main!(benches);
