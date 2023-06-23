//! This module has a test to run manually that can be tested against the
//! [Counter example](https://github.com/restatedev/sdk-java/blob/main/examples/src/main/java/dev/restate/sdk/examples/BlockingCounter.java) in sdk-java.

mod mocks;

use crate::mocks::MockEntryEnricher;
use bytes::Bytes;
use hyper::Uri;
use mocks::{InMemoryJournalStorage, InMemoryStateStorage, SimulatorAction};
use prost::Message;
use restate_common::types::{CompletionResult, EnrichedEntryHeader, ServiceInvocationId};
use restate_invoker::{ChannelServiceHandle, Effect, EffectKind, Service};
use restate_journal::raw::RawEntryCodec;
use restate_journal::{
    Completion, Entry, EntryResult, GetStateEntry, GetStateValue, OutputStreamEntry,
};
use restate_service_metadata::{
    DeliveryOptions, EndpointMetadata, InMemoryServiceEndpointRegistry, ProtocolType,
};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_test_util::{assert, assert_eq, let_assert, test};
use uuid::Uuid;

type PartitionProcessorSimulator =
    mocks::PartitionProcessorSimulator<ChannelServiceHandle, ProtobufRawEntryCodec>;

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
            Effect {
                service_invocation_id,
                kind: EffectKind::JournalEntry {
                    entry_index: 1,
                    entry,
                    ..
                }
            } = out
        );
        assert!(let EnrichedEntryHeader::GetState { is_completed: false } = entry.header);

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
            Effect {
                kind: EffectKind::JournalEntry {
                    entry_index: 2,
                    entry,
                    ..
                },
                ..
            } = out
        );
        assert!(let EnrichedEntryHeader::SetState = entry.header);

        SimulatorAction::Noop
    });
    partition_processor_simulator.append_handler_step(|out| {
        let_assert!(
            Effect {
                kind: EffectKind::JournalEntry {
                    entry_index: 3,
                    entry,
                    ..
                },
                ..
            } = out
        );
        assert!(let EnrichedEntryHeader::OutputStream = entry.header);

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

    let mut service_endpoint_registry = InMemoryServiceEndpointRegistry::default();
    service_endpoint_registry.register_service_endpoint(
        sid.service_id.service_name.to_string(),
        EndpointMetadata::new(
            Uri::from_static("http://localhost:8080"),
            ProtocolType::BidiStream,
            DeliveryOptions::default(),
        ),
    );

    // Start invoker
    let options = restate_invoker::Options::default();
    let remote_invoker: Service<
        ProtobufRawEntryCodec,
        InMemoryJournalStorage,
        InMemoryStateStorage,
        MockEntryEnricher,
        InMemoryServiceEndpointRegistry,
    > = options.build(
        journal_reader.clone(),
        InMemoryStateStorage::default(),
        MockEntryEnricher::default(),
        service_endpoint_registry,
    );

    // Build the partition processor simulator
    let mut partition_processor_simulator =
        PartitionProcessorSimulator::new(journal_reader, remote_invoker.handle()).await;
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
