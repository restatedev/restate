//! This module has a test to run manually that can be tested against the
//! [Counter example](https://github.com/restatedev/sdk-java/blob/main/examples/src/main/java/dev/restate/sdk/examples/BlockingCounter.java) in sdk-java.

mod mocks;

use bytes::Bytes;
use common::types::ServiceInvocationId;
use hyper::Uri;
use invoker::{
    DeliveryOptions, EndpointMetadata, Invoker, Kind, OutputEffect, ProtocolType,
    UnboundedInvokerInputSender,
};
use journal::raw::{RawEntryCodec, RawEntryHeader};
use journal::{
    Completion, CompletionResult, Entry, EntryResult, GetStateEntry, GetStateValue,
    OutputStreamEntry,
};
use mocks::{InMemoryJournalStorage, SimulatorAction};
use prost::Message;
use service_protocol::codec::ProtobufRawEntryCodec;
use std::collections::HashMap;
use test_utils::{assert, assert_eq, let_assert, test};
use uuid::Uuid;

type PartitionProcessorSimulator =
    mocks::PartitionProcessorSimulator<UnboundedInvokerInputSender, ProtobufRawEntryCodec>;

#[derive(Clone, PartialEq, Eq, Message)]
pub struct CounterAddRequest {
    #[prost(string, tag = "1")]
    pub counter_name: String,
    #[prost(int64, tag = "2")]
    pub value: i64,
}
#[derive(Clone, PartialEq, Eq, Message)]
pub struct CounterUpdateResult {
    #[prost(int64, tag = "1")]
    pub old_value: i64,
    #[prost(int64, tag = "2")]
    pub new_value: i64,
}

fn register_counter_test_steps(partition_processor_simulator: &mut PartitionProcessorSimulator) {
    partition_processor_simulator.append_handler_step(|out| {
        let_assert!(
            OutputEffect {
                service_invocation_id,
                kind: Kind::JournalEntry {
                    entry_index: 1,
                    entry
                }
            } = out
        );
        assert!(let RawEntryHeader::GetState { is_completed: false } = entry.header);

        let_assert!(
            Ok(Entry::GetState(GetStateEntry {
                key,
                value: None::<GetStateValue>
            })) = ProtobufRawEntryCodec::deserialize(&entry)
        );
        assert_eq!(key, Bytes::from_static(b"total"));

        SimulatorAction::SendCompletion(
            service_invocation_id,
            Completion {
                entry_index: 1,
                result: CompletionResult::Empty,
            },
        )
    });
    register_set_state_and_output_steps(partition_processor_simulator);
}

fn register_set_state_and_output_steps(
    partition_processor_simulator: &mut PartitionProcessorSimulator,
) {
    partition_processor_simulator.append_handler_step(|out| {
        let_assert!(
            OutputEffect {
                kind: Kind::JournalEntry {
                    entry_index: 2,
                    entry
                },
                ..
            } = out
        );
        assert!(let RawEntryHeader::SetState = entry.header);

        SimulatorAction::Noop
    });
    partition_processor_simulator.append_handler_step(|out| {
        let_assert!(
            OutputEffect {
                kind: Kind::JournalEntry {
                    entry_index: 3,
                    entry
                },
                ..
            } = out
        );
        assert!(let RawEntryHeader::OutputStream = entry.header);

        let_assert!(Ok(Entry::OutputStream(OutputStreamEntry {
            result: EntryResult::Success(mut result)
        } )) = ProtobufRawEntryCodec::deserialize(&entry));
        let counter_update_result = CounterUpdateResult::decode(&mut result).unwrap();
        assert_eq!(counter_update_result.old_value, 0);
        assert_eq!(counter_update_result.new_value, 5);

        SimulatorAction::Noop
    });
}

#[ignore]
#[test(tokio::test)]
async fn bidi_stream() {
    let sid = ServiceInvocationId::new(
        "counter.Counter",
        Bytes::from_static(b"my-counter"),
        Uuid::now_v7(),
    );

    // Mock journal reader
    let journal_reader = InMemoryJournalStorage::default();

    // Start invoker
    let remote_invoker =
        Invoker::<ProtobufRawEntryCodec, _, HashMap<String, EndpointMetadata>>::new(
            journal_reader.clone(),
            [(
                sid.service_id.service_name.clone().to_string(),
                EndpointMetadata::new(
                    Uri::from_static("http://localhost:8080"),
                    ProtocolType::BidiStream,
                    DeliveryOptions::default(),
                ),
            )]
            .into(),
        );

    // Build the partition processor simulator
    let mut partition_processor_simulator =
        PartitionProcessorSimulator::new(journal_reader, remote_invoker.create_sender()).await;
    partition_processor_simulator
        .invoke(
            sid.clone(),
            "GetAndAdd",
            CounterAddRequest {
                counter_name: "my-counter".to_string(),
                value: 5,
            },
        )
        .await;
    register_counter_test_steps(&mut partition_processor_simulator);

    let (drain_signal, watch) = drain::channel();

    // Run and await for partition processor to complete, then send the shutdown signal
    let invoker_res = tokio::spawn(remote_invoker.run(watch));
    partition_processor_simulator.run().await;

    // Send the drain signal
    drain_signal.drain().await;

    // Check invoker did not panic
    invoker_res.await.unwrap();
}
