use crate::storage::v1::{
    key, Deduplication, Inbox, Journal, Key, Outbox, PartitionStateMachine, State, Status, Timers,
};
use restate_storage_rocksdb::deduplication_table::DeduplicationKey;
use restate_storage_rocksdb::fsm_table::PartitionStateMachineKey;
use restate_storage_rocksdb::inbox_table::InboxKey;
use restate_storage_rocksdb::journal_table::JournalKey;
use restate_storage_rocksdb::keys::TableKey;
use restate_storage_rocksdb::outbox_table::OutboxKey;
use restate_storage_rocksdb::state_table::StateKey;
use restate_storage_rocksdb::status_table::StatusKey;
use restate_storage_rocksdb::timer_table::TimersKey;

pub mod storage {
    pub mod v1 {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/dev.restate.storage.scan.v1.rs"));
    }
}

impl From<DeduplicationKey> for key::Key {
    fn from(
        DeduplicationKey {
            partition_id,
            producing_partition_id,
        }: DeduplicationKey,
    ) -> Self {
        key::Key::Deduplication(Deduplication {
            partition_id,
            producing_partition_id,
        })
    }
}

impl From<PartitionStateMachineKey> for key::Key {
    fn from(
        PartitionStateMachineKey {
            partition_id,
            state_id,
        }: PartitionStateMachineKey,
    ) -> Self {
        key::Key::PartitionStateMachine(PartitionStateMachine {
            partition_id,
            state_id,
        })
    }
}

impl From<InboxKey> for key::Key {
    fn from(
        InboxKey {
            partition_key,
            service_name,
            service_key,
            sequence_number,
        }: InboxKey,
    ) -> Self {
        key::Key::Inbox(Inbox {
            partition_key,
            service_name: service_name.map(|s| s.into()),
            service_key,
            sequence_number,
        })
    }
}

impl From<JournalKey> for key::Key {
    fn from(
        JournalKey {
            partition_key,
            service_name,
            service_key,
            journal_index,
        }: JournalKey,
    ) -> Self {
        key::Key::Journal(Journal {
            partition_key,
            service_name: service_name.map(|s| s.into()),
            service_key,
            journal_index,
        })
    }
}

impl From<OutboxKey> for key::Key {
    fn from(
        OutboxKey {
            partition_id,
            message_index,
        }: OutboxKey,
    ) -> Self {
        key::Key::Outbox(Outbox {
            partition_id,
            message_index,
        })
    }
}

impl From<StateKey> for key::Key {
    fn from(
        StateKey {
            partition_key,
            service_name,
            service_key,
            state_key,
        }: StateKey,
    ) -> Self {
        key::Key::State(State {
            partition_key,
            service_name: service_name.map(Into::into),
            service_key,
            state_key,
        })
    }
}

impl From<StatusKey> for key::Key {
    fn from(
        StatusKey {
            partition_key,
            service_name,
            service_key,
        }: StatusKey,
    ) -> Self {
        key::Key::Status(Status {
            partition_key,
            service_name: service_name.map(|s| s.into()),
            service_key,
        })
    }
}

impl From<TimersKey> for key::Key {
    fn from(
        TimersKey {
            partition_id,
            timestamp,
            service_name,
            service_key,
            invocation_id,
            journal_index,
        }: TimersKey,
    ) -> Self {
        key::Key::Timers(Timers {
            partition_id,
            timestamp,
            service_name: service_name.map(|s| s.into()),
            service_key,
            invocation_id,
            journal_index,
        })
    }
}

impl<K: TableKey + Into<key::Key>> From<K> for Key {
    fn from(value: K) -> Self {
        Key {
            key: Some(value.into()),
        }
    }
}

pub(crate) enum KeyWrapper {
    Dedup(DeduplicationKey),
    Fsm(PartitionStateMachineKey),
    Inbox(InboxKey),
    Journal(JournalKey),
    Outbox(OutboxKey),
    State(StateKey),
    Status(StatusKey),
    Timer(TimersKey),
}

impl KeyWrapper {
    pub(crate) fn table(&self) -> TableKind {
        match self {
            KeyWrapper::Fsm(_) => TableKind::PartitionStateMachine,
            KeyWrapper::Inbox(_) => TableKind::Inbox,
            KeyWrapper::Journal(_) => TableKind::Journal,
            KeyWrapper::Outbox(_) => TableKind::Outbox,
            KeyWrapper::State(_) => TableKind::State,
            KeyWrapper::Status(_) => TableKind::Status,
            KeyWrapper::Timer(_) => TableKind::Timers,
            KeyWrapper::Dedup(_) => TableKind::Deduplication,
        }
    }
}

impl From<Deduplication> for DeduplicationKey {
    fn from(
        Deduplication {
            partition_id,
            producing_partition_id,
        }: Deduplication,
    ) -> Self {
        DeduplicationKey {
            partition_id,
            producing_partition_id,
        }
    }
}

impl From<PartitionStateMachine> for PartitionStateMachineKey {
    fn from(
        PartitionStateMachine {
            partition_id,
            state_id,
        }: PartitionStateMachine,
    ) -> Self {
        PartitionStateMachineKey {
            partition_id,
            state_id,
        }
    }
}

impl From<Inbox> for InboxKey {
    fn from(
        Inbox {
            partition_key,
            service_name,
            service_key,
            sequence_number,
        }: Inbox,
    ) -> Self {
        InboxKey {
            partition_key,
            service_name: service_name.map(Into::into),
            service_key,
            sequence_number,
        }
    }
}

impl From<Journal> for JournalKey {
    fn from(
        Journal {
            partition_key,
            service_name,
            service_key,
            journal_index,
        }: Journal,
    ) -> Self {
        JournalKey {
            partition_key,
            service_name: service_name.map(Into::into),
            service_key,
            journal_index,
        }
    }
}

impl From<Outbox> for OutboxKey {
    fn from(
        Outbox {
            partition_id,
            message_index,
        }: Outbox,
    ) -> Self {
        OutboxKey {
            partition_id,
            message_index,
        }
    }
}

impl From<State> for StateKey {
    fn from(
        State {
            partition_key,
            service_name,
            service_key,
            state_key,
        }: State,
    ) -> Self {
        StateKey {
            partition_key,
            service_name: service_name.map(Into::into),
            service_key,
            state_key,
        }
    }
}

impl From<Status> for StatusKey {
    fn from(
        Status {
            partition_key,
            service_name,
            service_key,
        }: Status,
    ) -> Self {
        StatusKey {
            partition_key,
            service_name: service_name.map(Into::into),
            service_key,
        }
    }
}

impl From<Timers> for TimersKey {
    fn from(
        Timers {
            partition_id,
            timestamp,
            service_name,
            service_key,
            invocation_id,
            journal_index,
        }: Timers,
    ) -> Self {
        TimersKey {
            partition_id,
            timestamp,
            service_name: service_name.map(Into::into),
            service_key,
            invocation_id,
            journal_index,
        }
    }
}

impl From<key::Key> for KeyWrapper {
    fn from(pb_key: key::Key) -> Self {
        match pb_key {
            key::Key::Deduplication(k) => KeyWrapper::Dedup(k.into()),
            key::Key::PartitionStateMachine(k) => KeyWrapper::Fsm(k.into()),
            key::Key::Inbox(k) => KeyWrapper::Inbox(k.into()),
            key::Key::Journal(k) => KeyWrapper::Journal(k.into()),
            key::Key::Outbox(k) => KeyWrapper::Outbox(k.into()),
            key::Key::State(k) => KeyWrapper::State(k.into()),
            key::Key::Status(k) => KeyWrapper::Status(k.into()),
            key::Key::Timers(k) => KeyWrapper::Timer(k.into()),
        }
    }
}

use restate_storage_rocksdb::TableKind;
