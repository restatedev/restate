// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::metric_definitions::{PARTITION_ACTUATOR_HANDLED, PARTITION_TIMER_DUE_HANDLED};
use crate::partition::leadership::{ActionEffect, LeadershipState};
use crate::partition::state_machine::{ActionCollector, Effects, StateMachine};
use crate::partition::storage::{DedupSequenceNumberResolver, PartitionStorage, Transaction};
use assert2::let_assert;
use futures::StreamExt;
use metrics::counter;
use restate_core::metadata;
use restate_network::Networking;
use restate_storage_rocksdb::{RocksDBStorage, RocksDBTransaction};
use restate_types::identifiers::{PartitionId, PartitionKey};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use tracing::{debug, instrument, trace, Span};

mod action_effect_handler;
mod leadership;
mod services;
pub mod shuffle;
mod state_machine;
pub mod storage;
pub mod types;

use restate_bifrost::{Bifrost, FindTailAttributes, LogReadStream, LogRecord, Record};
use restate_core::cancellation_watcher;
use restate_storage_api::deduplication_table::{
    DedupInformation, DedupSequenceNumber, EpochSequenceNumber, ProducerId,
};
use restate_storage_api::StorageError;
use restate_types::logs::{LogId, Lsn, SequenceNumber};
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::{Command, Destination, Envelope, Header};

#[derive(Debug)]
pub(super) struct PartitionProcessor<RawEntryCodec, InvokerInputSender> {
    pub partition_id: PartitionId,
    pub partition_key_range: RangeInclusive<PartitionKey>,

    num_timers_in_memory_limit: Option<usize>,
    channel_size: usize,

    invoker_tx: InvokerInputSender,

    rocksdb_storage: RocksDBStorage,

    _entry_codec: PhantomData<RawEntryCodec>,
}

impl<RawEntryCodec, InvokerInputSender> PartitionProcessor<RawEntryCodec, InvokerInputSender>
where
    RawEntryCodec: restate_types::journal::raw::RawEntryCodec + Default + Debug,
    InvokerInputSender: restate_invoker_api::ServiceHandle + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        num_timers_in_memory_limit: Option<usize>,
        channel_size: usize,
        invoker_tx: InvokerInputSender,
        rocksdb_storage: RocksDBStorage,
    ) -> Self {
        Self {
            partition_id,
            partition_key_range,
            num_timers_in_memory_limit,
            channel_size,
            invoker_tx,
            _entry_codec: Default::default(),
            rocksdb_storage,
        }
    }

    #[instrument(level = "info", skip_all, fields(partition_id = %self.partition_id, is_leader = tracing::field::Empty))]
    pub(super) async fn run(self, networking: Networking, bifrost: Bifrost) -> anyhow::Result<()> {
        let PartitionProcessor {
            partition_id,
            partition_key_range,
            num_timers_in_memory_limit,
            channel_size,
            invoker_tx,
            rocksdb_storage,
            ..
        } = self;

        let mut partition_storage =
            PartitionStorage::new(partition_id, partition_key_range.clone(), rocksdb_storage);

        let mut state_machine = Self::create_state_machine::<RawEntryCodec>(
            &mut partition_storage,
            partition_key_range.clone(),
        )
        .await?;

        let last_applied_lsn = partition_storage.load_applied_lsn().await?;
        let last_applied_lsn = last_applied_lsn.unwrap_or(Lsn::INVALID);
        if tracing::event_enabled!(tracing::Level::DEBUG) {
            let current_tail = bifrost
                .find_tail(LogId::from(partition_id), FindTailAttributes::default())
                .await?;
            debug!(
                last_applied_lsn = %last_applied_lsn,
                current_log_tail = ?current_tail,
                "PartitionProcessor creating log reader",
            );
        }
        let mut log_reader = LogReader::new(&bifrost, LogId::from(partition_id), last_applied_lsn);

        let mut action_collector = ActionCollector::default();
        let mut effects = Effects::default();

        let (mut state, mut action_effect_stream) = LeadershipState::follower(
            partition_id,
            partition_key_range.clone(),
            num_timers_in_memory_limit,
            channel_size,
            invoker_tx,
            bifrost,
            networking,
        );

        loop {
            tokio::select! {
                _ = cancellation_watcher() => break,
                record = log_reader.read_next() => {
                    let record = record?;
                    trace!(lsn = %record.0, "Processing bifrost record for '{}': {:?}", record.1.command.name(), record.1.header);

                    let mut transaction = partition_storage.create_transaction();

                    // clear buffers used when applying the next record
                    action_collector.clear();
                    effects.clear();

                    let leadership_change = Self::apply_record(
                            record,
                            &mut state_machine,
                            &mut transaction,
                            &mut action_collector,
                            &mut effects, state.is_leader(),
                            &partition_key_range)
                        .await?;

                    if let Some(announce_leader) = leadership_change {
                        let new_esn = EpochSequenceNumber::new(announce_leader.leader_epoch);

                        // update our own epoch sequence number to filter out messages from previous leaders
                        transaction.store_dedup_sequence_number(ProducerId::self_producer(), DedupSequenceNumber::Esn(new_esn)).await;
                        // commit all changes so far, this is important so that the actuators see all changes
                        // when becoming leader.
                        transaction.commit().await?;

                        // We can ignore all actions collected so far because as a new leader we have to instruct the
                        // actuators afresh.
                        action_collector.clear();

                        if announce_leader.node_id == metadata().my_node_id() {
                            let was_follower = !state.is_leader();
                            (state, action_effect_stream) = state.become_leader(new_esn, &mut partition_storage).await?;
                            if was_follower {
                                Span::current().record("is_leader", state.is_leader());
                                debug!(leader_epoch = %new_esn.leader_epoch, "Partition leadership acquired");
                            }
                        } else {
                            let was_leader = state.is_leader();
                            (state, action_effect_stream) = state.become_follower().await?;
                            if was_leader {
                                Span::current().record("is_leader", state.is_leader());
                                debug!(leader_epoch = %new_esn.leader_epoch, "Partition leadership lost to {}", announce_leader.node_id);
                            }
                        }
                    } else {
                        // Commit our changes and notify actuators about actions if we are the leader
                        transaction.commit().await?;
                        state.handle_actions(action_collector.drain(..)).await?;
                    }
                },
                action_effect = action_effect_stream.next() => {
                    counter!(PARTITION_ACTUATOR_HANDLED).increment(1);
                    let action_effect = action_effect.ok_or_else(|| anyhow::anyhow!("action effect stream is closed"))?;
                    state.handle_action_effect(action_effect).await?;
                },
                timer = state.run_timer() => {
                    counter!(PARTITION_TIMER_DUE_HANDLED).increment(1);
                    state.handle_action_effect(ActionEffect::Timer(timer)).await?;
                },
            }
        }

        debug!(restate.node = %metadata().my_node_id(), %partition_id, "Shutting partition processor down.");
        let _ = state.become_follower().await;

        Ok(())
    }

    async fn create_state_machine<Codec>(
        partition_storage: &mut PartitionStorage<RocksDBStorage>,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> Result<StateMachine<Codec>, restate_storage_api::StorageError>
    where
        Codec: restate_types::journal::raw::RawEntryCodec + Default + Debug,
    {
        let inbox_seq_number = partition_storage.load_inbox_seq_number().await?;
        let outbox_seq_number = partition_storage.load_outbox_seq_number().await?;

        let state_machine =
            StateMachine::new(inbox_seq_number, outbox_seq_number, partition_key_range);

        Ok(state_machine)
    }

    async fn apply_record<Codec>(
        record: (Lsn, Envelope),
        state_machine: &mut StateMachine<Codec>,
        transaction: &mut Transaction<RocksDBTransaction<'_>>,
        action_collector: &mut ActionCollector,
        effects: &mut Effects,
        is_leader: bool,
        partition_key_range: &RangeInclusive<PartitionKey>,
    ) -> Result<Option<AnnounceLeader>, state_machine::Error>
    where
        Codec: restate_types::journal::raw::RawEntryCodec + Default + Debug,
    {
        let (lsn, envelope) = record;
        transaction.store_applied_lsn(lsn).await?;

        if let Some(dedup_information) = is_targeted_to_me(&envelope.header, partition_key_range) {
            // deduplicate if deduplication information has been provided
            if let Some(dedup_information) = dedup_information {
                if is_outdated_or_duplicate(dedup_information, transaction).await? {
                    debug!(
                        "Ignoring outdated or duplicate message: {:?}",
                        envelope.header
                    );
                    return Ok(None);
                } else {
                    transaction
                        .store_dedup_sequence_number(
                            dedup_information.producer_id.clone(),
                            dedup_information.sequence_number,
                        )
                        .await;
                }
            }

            if let Command::AnnounceLeader(announce_leader) = envelope.command {
                let last_known_esn = transaction
                    .get_dedup_sequence_number(&ProducerId::self_producer())
                    .await?
                    .map(|dedup_sn| {
                        let_assert!(
                            DedupSequenceNumber::Esn(esn) = dedup_sn,
                            "self producer must store epoch sequence numbers!"
                        );
                        esn
                    });

                if last_known_esn
                    .map(|last_known_esn| {
                        last_known_esn.leader_epoch < announce_leader.leader_epoch
                    })
                    .unwrap_or(true)
                {
                    // leadership change detected, let's finish our transaction here
                    return Ok(Some(announce_leader));
                }
                debug!(
                    last_known_esn = %last_known_esn.as_ref().unwrap().leader_epoch,
                    announce_esn = %announce_leader.leader_epoch,
                    node_id = %announce_leader.node_id,
                    "Ignoring outdated leadership announcement."
                );
            } else {
                state_machine
                    .apply(
                        envelope.command,
                        effects,
                        transaction,
                        action_collector,
                        is_leader,
                    )
                    .await?;
            }
        } else {
            trace!(
                "Ignore message which is not targeted to me: {:?}",
                envelope.header
            );
        }

        Ok(None)
    }
}

fn is_targeted_to_me<'a>(
    header: &'a Header,
    partition_key_range: &RangeInclusive<PartitionKey>,
) -> Option<&'a Option<DedupInformation>> {
    match &header.dest {
        Destination::Processor {
            partition_key,
            dedup,
        } if partition_key_range.contains(partition_key) => Some(dedup),
        _ => None,
    }
}

async fn is_outdated_or_duplicate(
    dedup_information: &DedupInformation,
    dedup_resolver: &mut impl DedupSequenceNumberResolver,
) -> Result<bool, StorageError> {
    let last_dsn = dedup_resolver
        .get_dedup_sequence_number(&dedup_information.producer_id)
        .await?;

    // Check whether we have seen this message before
    let is_duplicate = if let Some(last_dsn) = last_dsn {
        match (last_dsn, &dedup_information.sequence_number) {
            (DedupSequenceNumber::Esn(last_esn), DedupSequenceNumber::Esn(esn)) => last_esn >= *esn,
            (DedupSequenceNumber::Sn(last_sn), DedupSequenceNumber::Sn(sn)) => last_sn >= *sn,
            (last_dsn, dsn) => panic!("sequence number types do not match: last sequence number '{:?}', received sequence number '{:?}'", last_dsn, dsn),
        }
    } else {
        false
    };

    Ok(is_duplicate)
}

struct LogReader {
    log_reader: LogReadStream,
}

impl LogReader {
    fn new(bifrost: &Bifrost, log_id: LogId, lsn: Lsn) -> Self {
        Self {
            log_reader: bifrost.create_reader(log_id, lsn),
        }
    }

    async fn read_next(&mut self) -> anyhow::Result<(Lsn, Envelope)> {
        let LogRecord { record, offset } = self.log_reader.read_next().await?;
        Self::deserialize_record(record).map(|envelope| (offset, envelope))
    }

    #[allow(dead_code)]
    async fn read_next_opt(&mut self) -> anyhow::Result<Option<(Lsn, Envelope)>> {
        let maybe_log_record = self.log_reader.read_next_opt().await?;

        maybe_log_record
            .map(|log_record| {
                Self::deserialize_record(log_record.record)
                    .map(|envelope| (log_record.offset, envelope))
            })
            .transpose()
    }

    fn deserialize_record(record: Record) -> anyhow::Result<Envelope> {
        match record {
            Record::Data(payload) => {
                let envelope = Envelope::from_bytes(payload.as_ref())?;
                Ok(envelope)
            }
            Record::TrimGap(_) => {
                unimplemented!("Currently not supported")
            }
            Record::Seal(_) => {
                unimplemented!("Currently not supported")
            }
        }
    }
}
