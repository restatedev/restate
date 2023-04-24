use crate::storage::v1::{
    key, Deduplication, Inbox, Journal, Outbox, PartitionStateMachine, State, Status, Timers,
};

pub mod storage {
    pub mod v1 {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/dev.restate.storage.scan.v1.rs"));
    }
}

impl From<restate_storage_rocksdb::Key> for key::Key {
    fn from(value: restate_storage_rocksdb::Key) -> Self {
        match value {
            restate_storage_rocksdb::Key::Deduplication(
                restate_storage_rocksdb::deduplication_table::DeduplicationKeyComponents {
                    partition_id,
                    producing_partition_id,
                },
            ) => key::Key::Deduplication(Deduplication {
                partition_id,
                producing_partition_id,
            }),
            restate_storage_rocksdb::Key::PartitionStateMachine(
                restate_storage_rocksdb::fsm_table::PartitionStateMachineKeyComponents {
                    partition_id,
                    state_id,
                },
            ) => key::Key::PartitionStateMachine(PartitionStateMachine {
                partition_id,
                state_id,
            }),
            restate_storage_rocksdb::Key::Inbox(
                restate_storage_rocksdb::inbox_table::InboxKeyComponents {
                    partition_key,
                    service_name,
                    service_key,
                    sequence_number,
                },
            ) => key::Key::Inbox(Inbox {
                partition_key,
                service_name: service_name.map(|s| s.into()),
                service_key,
                sequence_number,
            }),
            restate_storage_rocksdb::Key::Journal(
                restate_storage_rocksdb::journal_table::JournalKeyComponents {
                    partition_key,
                    service_name,
                    service_key,
                    journal_index,
                },
            ) => key::Key::Journal(Journal {
                partition_key,
                service_name: service_name.map(|s| s.into()),
                service_key,
                journal_index,
            }),
            restate_storage_rocksdb::Key::Outbox(
                restate_storage_rocksdb::outbox_table::OutboxKeyComponents {
                    partition_id,
                    message_index,
                },
            ) => key::Key::Outbox(Outbox {
                partition_id,
                message_index,
            }),
            restate_storage_rocksdb::Key::State(
                restate_storage_rocksdb::state_table::StateKeyComponents {
                    partition_key,
                    service_name,
                    service_key,
                    state_key,
                },
            ) => key::Key::State(State {
                partition_key,
                service_name: service_name.map(|s| s.into()),
                service_key,
                state_key,
            }),
            restate_storage_rocksdb::Key::Status(
                restate_storage_rocksdb::status_table::StatusKeyComponents {
                    partition_key,
                    service_name,
                    service_key,
                },
            ) => key::Key::Status(Status {
                partition_key,
                service_name: service_name.map(|s| s.into()),
                service_key,
            }),
            restate_storage_rocksdb::Key::Timers(
                restate_storage_rocksdb::timer_table::TimersKeyComponents {
                    partition_id,
                    timestamp,
                    service_name,
                    service_key,
                    invocation_id,
                    journal_index,
                },
            ) => key::Key::Timers(Timers {
                partition_id,
                timestamp,
                service_name: service_name.map(|s| s.into()),
                service_key,
                invocation_id,
                journal_index,
            }),
        }
    }
}

impl From<key::Key> for restate_storage_rocksdb::Key {
    fn from(value: key::Key) -> restate_storage_rocksdb::Key {
        match value {
            key::Key::Deduplication(Deduplication {
                partition_id,
                producing_partition_id,
            }) => restate_storage_rocksdb::Key::Deduplication(
                restate_storage_rocksdb::deduplication_table::DeduplicationKeyComponents {
                    partition_id,
                    producing_partition_id,
                },
            ),
            key::Key::PartitionStateMachine(PartitionStateMachine {
                partition_id,
                state_id,
            }) => restate_storage_rocksdb::Key::PartitionStateMachine(
                restate_storage_rocksdb::fsm_table::PartitionStateMachineKeyComponents {
                    partition_id,
                    state_id,
                },
            ),
            key::Key::Inbox(Inbox {
                partition_key,
                service_name,
                service_key,
                sequence_number,
            }) => restate_storage_rocksdb::Key::Inbox(
                restate_storage_rocksdb::inbox_table::InboxKeyComponents {
                    partition_key,
                    service_name: service_name.map(|s| s.into()),
                    service_key,
                    sequence_number,
                },
            ),
            key::Key::Journal(Journal {
                partition_key,
                service_name,
                service_key,
                journal_index,
            }) => restate_storage_rocksdb::Key::Journal(
                restate_storage_rocksdb::journal_table::JournalKeyComponents {
                    partition_key,
                    service_name: service_name.map(|s| s.into()),
                    service_key,
                    journal_index,
                },
            ),
            key::Key::Outbox(Outbox {
                partition_id,
                message_index,
            }) => restate_storage_rocksdb::Key::Outbox(
                restate_storage_rocksdb::outbox_table::OutboxKeyComponents {
                    partition_id,
                    message_index,
                },
            ),
            key::Key::State(State {
                partition_key,
                service_name,
                service_key,
                state_key,
            }) => restate_storage_rocksdb::Key::State(
                restate_storage_rocksdb::state_table::StateKeyComponents {
                    partition_key,
                    service_name: service_name.map(|s| s.into()),
                    service_key,
                    state_key,
                },
            ),
            key::Key::Status(Status {
                partition_key,
                service_name,
                service_key,
            }) => restate_storage_rocksdb::Key::Status(
                restate_storage_rocksdb::status_table::StatusKeyComponents {
                    partition_key,
                    service_name: service_name.map(|s| s.into()),
                    service_key,
                },
            ),
            key::Key::Timers(Timers {
                partition_id,
                timestamp,
                service_name,
                service_key,
                invocation_id,
                journal_index,
            }) => restate_storage_rocksdb::Key::Timers(
                restate_storage_rocksdb::timer_table::TimersKeyComponents {
                    partition_id,
                    timestamp,
                    service_name: service_name.map(|s| s.into()),
                    service_key,
                    invocation_id,
                    journal_index,
                },
            ),
        }
    }
}
