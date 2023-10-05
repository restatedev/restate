// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
#![allow(dead_code)] // TODO remove this once we start using all the infra

use crate::partition::state_machine::StateStorage;
use crate::partition::storage::PartitionStorage;
use bytes::Bytes;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::ServiceId;
use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

pub(crate) mod deterministic;
pub(crate) mod non_deterministic;

// -- StateReader to abstract the partition processor storage

#[async_trait::async_trait]
trait StateReader {
    async fn read_state(
        &self,
        service_id: &ServiceId,
        key: &str,
    ) -> Result<Option<Bytes>, anyhow::Error>;
}

#[async_trait::async_trait]
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
    #[allow(unused)]
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
    use restate_test_util::assert;
    use std::collections::HashMap;

    #[derive(Clone, Default)]
    pub(super) struct MockStateReader(pub(super) HashMap<String, Bytes>);

    impl MockStateReader {
        pub(super) fn set<Serde: StateSerde>(
            &mut self,
            k: &StateKey<Serde>,
            val: Serde::MaterializedType,
        ) {
            self.0.insert(k.0.to_string(), Serde::encode(&val).unwrap());
        }

        #[allow(dead_code)]
        pub(super) fn with<Serde: StateSerde>(
            mut self,
            k: &StateKey<Serde>,
            val: Serde::MaterializedType,
        ) -> Self {
            self.set(k, val);
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
    }

    #[async_trait::async_trait]
    impl StateReader for &MockStateReader {
        async fn read_state(
            &self,
            _service_id: &ServiceId,
            key: &str,
        ) -> Result<Option<Bytes>, Error> {
            Ok(self.0.get(key).cloned())
        }
    }
}
