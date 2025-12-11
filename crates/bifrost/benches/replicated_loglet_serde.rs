// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use bytes::{Bytes, BytesMut};
use bytestring::ByteString;
use criterion::{Criterion, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};
use pprof::flamegraph::Options;
use prost::Message as _;
use rand::distr::Alphanumeric;
use rand::{Rng, RngCore, random};

use restate_bifrost::InputRecord;
use restate_core::network::protobuf::network::message::Body;
use restate_core::network::protobuf::network::{Datagram, Message, datagram};
use restate_invoker_api::{Effect, EffectKind};
use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber, ProducerId};
use restate_types::identifiers::{InvocationId, LeaderEpoch, PartitionProcessorRpcRequestId};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocation, ServiceInvocationSpanContext,
};
use restate_types::journal_v2::CommandType;
use restate_types::journal_v2::raw::{RawCommand, RawEntry};
use restate_types::logs::{LogId, LogletId, LogletOffset, Record, SequenceNumber};
use restate_types::net::codec::{WireDecode, WireEncode};
use restate_types::net::log_server::{LogServerRequestHeader, Store, StoreFlags};
use restate_types::net::replicated_loglet::{Append, CommonRequestHeader};
use restate_types::net::{RpcRequest, Service};
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, RestateVersion};
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
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn invoke_cmd() -> Command {
    let idempotency_key: ByteString = rand_string(15).into();
    let request_id = PartitionProcessorRpcRequestId::new();
    let inv_source = restate_types::invocation::Source::Ingress(request_id);
    let handler: ByteString = format!("aFunction_{}", rand_string(10)).into();

    Command::Invoke(Box::new(ServiceInvocation {
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
        completion_retention_duration: Duration::from_secs(10),
        journal_retention_duration: Default::default(),
        idempotency_key: Some(idempotency_key),
        response_sink: Some(
            restate_types::invocation::ServiceInvocationResponseSink::Ingress { request_id },
        ),
        submit_notification_sink: Some(
            restate_types::invocation::SubmitNotificationSink::Ingress { request_id },
        ),
        restate_version: RestateVersion::current(),
    }))
}

fn invoker_effect_cmd() -> Command {
    let idempotency_key: ByteString = rand_string(15).into();
    let handler: ByteString = format!("aFunction_{}", rand_string(10)).into();

    let mut data = [0u8; 128];
    rand::rng().fill_bytes(&mut data);

    Command::InvokerEffect(Box::new(Effect {
        invocation_id: InvocationId::generate(
            &InvocationTarget::service("MyWonderfulService", handler.clone()),
            Some(&idempotency_key),
        ),
        kind: EffectKind::journal_entry(
            RawEntry::Command(RawCommand::new(
                CommandType::SetState,
                Bytes::copy_from_slice(&data),
            )),
            Some(random()),
        ),
    }))
}

pub fn generate_envelope<G>(generator: G) -> Arc<Envelope>
where
    G: FnOnce() -> Command,
{
    let partition_key = random();
    let leader_epoch = LeaderEpoch::from(random::<u64>());

    let header = restate_wal_protocol::Header {
        source: restate_wal_protocol::Source::Processor {
            partition_id: None,
            partition_key: Some(partition_key),
            leader_epoch,
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
    let command = generator();

    Envelope::new(header, command).into()
}

fn serialize_store_message(payloads: Arc<[Record]>) -> anyhow::Result<Message> {
    let store_message = Store {
        header: LogServerRequestHeader {
            loglet_id: LogletId::new(12u16.into(), 4.into()),
            known_global_tail: LogletOffset::new(55),
        },
        payloads: payloads.into(),
        timeout_at: Some(MillisSinceEpoch::now()),
        flags: StoreFlags::empty(),
        first_offset: LogletOffset::new(56),
        sequencer: GenerationalNodeId::new(1, 1),
        known_archived: LogletOffset::INVALID,
    };

    let body = Body::Datagram(Datagram {
        datagram: Some(
            restate_core::network::protobuf::network::RpcCall {
                payload: store_message.encode_to_bytes(restate_types::net::ProtocolVersion::V2)?,
                id: 88,
                service: <Store as RpcRequest>::Service::TAG.into(),
                msg_type: Store::TYPE.into(),
                sort_code: Some(12u64),
            }
            .into(),
        ),
    });

    let message = Message {
        header: Some(restate_core::network::protobuf::network::Header {
            my_nodes_config_version: 5,
            ..Default::default()
        }),
        body: Some(body),
    };
    Ok(message)
}

fn serialize_append_message(payloads: Arc<[Record]>) -> anyhow::Result<Message> {
    let append_message = Append {
        header: CommonRequestHeader {
            log_id: LogId::from(12u16),
            segment_index: 2.into(),
            loglet_id: LogletId::new(12u16.into(), 4.into()),
        },
        payloads: payloads.into(),
    };

    let body = Body::Datagram(Datagram {
        datagram: Some(
            restate_core::network::protobuf::network::RpcCall {
                payload: append_message.encode_to_bytes(restate_types::net::ProtocolVersion::V2)?,
                id: 88,
                service: <Append as RpcRequest>::Service::TAG.into(),
                msg_type: Append::TYPE.into(),
                sort_code: Some(12u64),
            }
            .into(),
        ),
    });

    let message = Message {
        header: Some(restate_core::network::protobuf::network::Header {
            my_nodes_config_version: 5,
            ..Default::default()
        }),
        body: Some(body),
    };
    Ok(message)
}

fn deserialize_append_message(serialized_message: Bytes) -> anyhow::Result<Append> {
    // let protocol_version = restate_types::net::ProtocolVersion::V1;
    // let msg = Message::decode(serialized_message)?;
    // let body = msg.body.unwrap();
    // we ignore non-deserializable messages (serde errors, or control signals in drain)
    // let msg_body = body.try_as_binary_body(restate_types::net::ProtocolVersion::V1)?;
    // Ok(Append::decode(msg_body.payload, protocol_version))
    //
    // disabled during fabric v2 refactoring, feel free to implement it if you need this bench
    let message = Message::decode(serialized_message)?;

    let Body::Datagram(Datagram {
        datagram: Some(datagram::Datagram::RpcCall(rpc_call)),
    }) = message.body.context("missing message body")?
    else {
        bail!("expecting a Datagram(RpcCall)");
    };

    Ok(Append::decode(
        rpc_call.payload,
        restate_types::net::ProtocolVersion::V2,
    ))
}

fn replicated_loglet_append_invoke_cmd_serde(c: &mut Criterion) {
    let mut group = c.benchmark_group("replicated-loglet-serde.invoke-cmd");
    let batch: Vec<Record> = vec![generate_envelope(invoke_cmd); 10]
        .into_iter()
        .map(|r| InputRecord::from(r).into_record())
        .collect();

    let payloads: Arc<[Record]> = batch.into();

    let mut buf = BytesMut::new();
    let message = serialize_append_message(payloads.clone()).unwrap();
    message.encode(&mut buf).unwrap();
    let serialized = buf.freeze();

    group
        .sample_size(50)
        .measurement_time(Duration::from_secs(5))
        .bench_function("serialize-append", |bencher| {
            bencher.iter(|| {
                let mut buf = BytesMut::new();
                let message = black_box(serialize_append_message(payloads.clone()).unwrap());
                black_box(message.encode(&mut buf)).unwrap();
            });
        })
        .bench_function("deserialize-append", |bencher| {
            bencher.iter(|| {
                black_box(deserialize_append_message(serialized.clone())).unwrap();
            });
        })
        .bench_function("serialize-store", |bencher| {
            bencher.iter(|| {
                let mut buf = BytesMut::new();
                let message = black_box(serialize_store_message(payloads.clone()).unwrap());
                black_box(message.encode(&mut buf)).unwrap();
            });
        });
    group.finish();
}

fn replicated_loglet_append_invoker_effect_cmd_serde(c: &mut Criterion) {
    let mut group = c.benchmark_group("replicated-loglet-serde.invoker-effect");
    let batch: Vec<Record> = vec![generate_envelope(invoker_effect_cmd); 10]
        .into_iter()
        .map(|r| InputRecord::from(r).into_record())
        .collect();

    let payloads: Arc<[Record]> = batch.into();

    let mut buf = BytesMut::new();
    let message = serialize_append_message(payloads.clone()).unwrap();
    message.encode(&mut buf).unwrap();
    let serialized = buf.freeze();

    group
        .sample_size(50)
        .measurement_time(Duration::from_secs(5))
        .bench_function("serialize-append", |bencher| {
            bencher.iter(|| {
                let mut buf = BytesMut::new();
                let message = black_box(serialize_append_message(payloads.clone()).unwrap());
                black_box(message.encode(&mut buf)).unwrap();
            });
        })
        .bench_function("deserialize-append", |bencher| {
            bencher.iter(|| {
                black_box(deserialize_append_message(serialized.clone())).unwrap();
            });
        })
        .bench_function("serialize-store", |bencher| {
            bencher.iter(|| {
                let mut buf = BytesMut::new();
                let message = black_box(serialize_store_message(payloads.clone()).unwrap());
                black_box(message.encode(&mut buf)).unwrap();
            });
        });
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(997, Output::Flamegraph(Some(flamegraph_options()))));
    targets = replicated_loglet_append_invoke_cmd_serde, replicated_loglet_append_invoker_effect_cmd_serde
);
criterion_main!(benches);
