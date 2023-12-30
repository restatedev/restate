// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::StateStorage;
use crate::partition::storage::PartitionStorage;
use bytes::Bytes;
use restate_storage_api::status_table::JournalMetadata;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{InvocationUuid, ServiceId};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::EntryIndex;
use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

pub(crate) mod deterministic;
pub(crate) mod non_deterministic;

// -- StateReader to abstract the partition processor storage

trait StateReader {
    fn read_state(
        &self,
        service_id: &ServiceId,
        key: &str,
    ) -> impl Future<Output = Result<Option<Bytes>, anyhow::Error>> + Send;

    fn read_virtual_journal_metadata(
        &self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<Option<(InvocationUuid, JournalMetadata)>, anyhow::Error>> + Send;

    fn read_virtual_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> impl Future<Output = Result<Option<EnrichedRawEntry>, anyhow::Error>> + Send;
}

impl StateReader for &PartitionStorage<RocksDBStorage> {
    async fn read_state(
        &self,
        service_id: &ServiceId,
        key: &str,
    ) -> Result<Option<Bytes>, anyhow::Error> {
        Ok(self
            .create_transaction()
            // TODO modify the load_state interface to get rid of the Bytes for the key
            .load_state(service_id, &Bytes::copy_from_slice(key.as_bytes()))
            .await?)
    }

    async fn read_virtual_journal_metadata(
        &self,
        service_id: &ServiceId,
    ) -> Result<Option<(InvocationUuid, JournalMetadata)>, anyhow::Error> {
        Ok(
            match crate::partition::state_machine::StateReader::get_invocation_status(
                &mut self.create_transaction(),
                service_id,
            )
            .await?
            {
                restate_storage_api::status_table::InvocationStatus::Virtual {
                    journal_metadata,
                    invocation_uuid,
                    ..
                } => Some((invocation_uuid, journal_metadata)),
                _ => None,
            },
        )
    }

    async fn read_virtual_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> Result<Option<EnrichedRawEntry>, anyhow::Error> {
        Ok(self
            .create_transaction()
            .load_journal_entry(service_id, entry_index)
            .await?)
    }
}

// -- Serde

trait StateSerde {
    type MaterializedType;

    fn decode(buf: Bytes) -> Result<Self::MaterializedType, anyhow::Error>;
    fn encode(v: &Self::MaterializedType) -> Result<Bytes, anyhow::Error>;
}

#[derive(Debug)]
struct Raw;

impl StateSerde for Raw {
    type MaterializedType = Bytes;

    fn decode(buf: Bytes) -> Result<Self::MaterializedType, anyhow::Error> {
        Ok(buf)
    }

    fn encode(v: &Self::MaterializedType) -> Result<Bytes, anyhow::Error> {
        Ok(v.clone())
    }
}

#[derive(Debug)]
struct Protobuf<T>(PhantomData<T>);

impl<T: prost::Message + Default> StateSerde for Protobuf<T> {
    type MaterializedType = T;

    fn decode(mut buf: Bytes) -> Result<Self::MaterializedType, anyhow::Error> {
        Ok(T::decode(&mut buf)?)
    }

    fn encode(v: &Self::MaterializedType) -> Result<Bytes, anyhow::Error> {
        Ok(v.encode_to_vec().into())
    }
}

#[derive(Debug)]
struct Bincode<T>(PhantomData<T>);

impl<T: serde::Serialize + for<'de> serde::Deserialize<'de>> StateSerde for Bincode<T> {
    type MaterializedType = T;

    fn decode(buf: Bytes) -> Result<Self::MaterializedType, anyhow::Error> {
        let (value, _) = bincode::serde::decode_from_slice(
            &buf,
            bincode::config::standard().with_variable_int_encoding(),
        )?;
        Ok(value)
    }

    fn encode(v: &Self::MaterializedType) -> Result<Bytes, anyhow::Error> {
        Ok(bincode::serde::encode_to_vec(
            v,
            bincode::config::standard().with_variable_int_encoding(),
        )?
        .into())
    }
}

#[derive(Clone, Debug)]
struct StateKey<Serde>(Cow<'static, str>, PhantomData<Serde>);

impl StateKey<Raw> {
    pub const fn new_raw(name: &'static str) -> StateKey<Raw> {
        Self(Cow::Borrowed(name), PhantomData)
    }
}

impl<T> StateKey<Protobuf<T>> {
    pub const fn new_pb(name: &'static str) -> StateKey<Protobuf<T>> {
        Self(Cow::Borrowed(name), PhantomData)
    }
}

impl<T> StateKey<Bincode<T>> {
    pub const fn new_bincode(name: &'static str) -> StateKey<Bincode<T>> {
        Self(Cow::Borrowed(name), PhantomData)
    }
}

impl<S> fmt::Display for StateKey<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T> From<String> for StateKey<T> {
    fn from(value: String) -> Self {
        StateKey(value.into(), PhantomData)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::Error;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_test_util::assert;
    use restate_types::invocation::ServiceInvocationSpanContext;
    use restate_types::journal::raw::RawEntryCodec;
    use restate_types::journal::{Completion, Entry};
    use std::collections::HashMap;
    use std::ops::Range;

    #[derive(Clone, Default)]
    pub(super) struct MockStateReader(
        pub(super) HashMap<String, Bytes>,
        pub(super) Option<(InvocationUuid, JournalMetadata, Vec<EnrichedRawEntry>)>,
    );

    impl MockStateReader {
        pub(super) fn set<Serde: StateSerde>(
            &mut self,
            k: &StateKey<Serde>,
            val: Serde::MaterializedType,
        ) -> &mut Self {
            self.0.insert(k.0.to_string(), Serde::encode(&val).unwrap());
            self
        }

        pub(super) fn append_journal_entry(&mut self, entry: Entry) -> &mut Self {
            self.append_enriched_journal_entry(ProtobufRawEntryCodec::serialize_enriched(entry))
        }

        pub(super) fn append_enriched_journal_entry(
            &mut self,
            entry: EnrichedRawEntry,
        ) -> &mut Self {
            let (_, meta, v) = self.1.get_or_insert_with(|| {
                (
                    InvocationUuid::now_v7(),
                    JournalMetadata::new(0, ServiceInvocationSpanContext::empty()),
                    vec![],
                )
            });
            meta.length += 1;
            v.push(entry);
            self
        }

        pub(super) fn complete_entry(&mut self, completion: Completion) -> &mut Self {
            let (_, _, v) = self.1.as_mut().expect("There must be a journal");
            ProtobufRawEntryCodec::write_completion(
                v.get_mut(completion.entry_index as usize)
                    .expect("There must be an entry"),
                completion.result,
            )
            .expect("Writing a completion must succeed");
            self
        }

        pub(super) fn assert_has_state<Serde: StateSerde + fmt::Debug>(
            &self,
            key: &StateKey<Serde>,
        ) -> Serde::MaterializedType {
            Serde::decode(
                self.0
                    .get(key.0.as_ref())
                    .unwrap_or_else(|| panic!("{:?} must be non-empty", key))
                    .clone(),
            )
            .unwrap_or_else(|_| panic!("{:?} must deserialize correctly", key))
        }

        pub(super) fn assert_has_not_state<Serde: StateSerde + fmt::Debug>(
            &self,
            key: &StateKey<Serde>,
        ) {
            assert!(self.0.get(key.0.as_ref()).is_none());
        }

        pub(super) fn assert_is_empty(&self) {
            assert!(self.0.is_empty());
        }

        pub(super) fn assert_has_journal(
            &self,
        ) -> (InvocationUuid, JournalMetadata, Vec<EnrichedRawEntry>) {
            self.1.clone().expect("There should be a journal")
        }

        pub(super) fn assert_has_no_journal(&self) {
            assert!(self.1.is_none())
        }

        pub(super) fn assert_has_journal_entry(&self, entry_index: usize) -> EnrichedRawEntry {
            self.1
                .clone()
                .expect("There should be a journal")
                .2
                .get(entry_index)
                .expect("There should be a journal entry")
                .clone()
        }

        pub(super) fn assert_has_journal_entries(
            &self,
            entry_index_range: Range<usize>,
        ) -> Vec<EnrichedRawEntry> {
            self.1
                .clone()
                .expect("There should be a journal")
                .2
                .get(entry_index_range)
                .expect("There must be journal entries")
                .to_vec()
        }
    }

    impl StateReader for &MockStateReader {
        async fn read_state(
            &self,
            _service_id: &ServiceId,
            key: &str,
        ) -> Result<Option<Bytes>, Error> {
            Ok(self.0.get(key).cloned())
        }

        async fn read_virtual_journal_metadata(
            &self,
            _: &ServiceId,
        ) -> Result<Option<(InvocationUuid, JournalMetadata)>, Error> {
            Ok(self.1.as_ref().map(|(id, m, _)| (*id, m.clone())))
        }

        async fn read_virtual_journal_entry(
            &self,
            _: &ServiceId,
            entry_index: EntryIndex,
        ) -> Result<Option<EnrichedRawEntry>, Error> {
            Ok(self
                .1
                .as_ref()
                .and_then(|(_, _, v)| v.get(entry_index as usize))
                .cloned())
        }
    }
}
