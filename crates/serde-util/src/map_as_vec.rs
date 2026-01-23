// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{DeserializeAs, SerializeAs};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// Transformation to use with serde_as to serialize a map into a vec, and vice versa deserialize a vec into a map.
///
/// The value of the `HashMap` must implement [`MapAsVecItem`] to specify how to extract the key.
pub struct MapAsVec;

/// Trait for the item to support [`MapAsVecItem`] transformation.
pub trait MapAsVecItem {
    type Key: Eq + Hash;

    /// Item's key, to transform a `Vec<Self>` into `HashMap<Self::Key, Self>`.
    fn key(&self) -> Self::Key;
}

impl<T> MapAsVecItem for Arc<T>
where
    T: MapAsVecItem,
{
    type Key = T::Key;

    fn key(&self) -> Self::Key {
        <Arc<T> as Borrow<T>>::borrow(self).key()
    }
}

impl<'de, T: MapAsVecItem + Deserialize<'de>> DeserializeAs<'de, HashMap<T::Key, T>> for MapAsVec {
    fn deserialize_as<D>(deserializer: D) -> Result<HashMap<T::Key, T>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<T> = Deserialize::deserialize(deserializer)?;
        let mut map = HashMap::with_capacity(vec.len());
        for item in vec {
            map.insert(item.key(), item);
        }

        Ok(map)
    }
}

impl<T: MapAsVecItem + Serialize> SerializeAs<HashMap<T::Key, T>> for MapAsVec {
    fn serialize_as<S>(source: &HashMap<T::Key, T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(source.values())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde::{Deserialize, Serialize};

    use serde_with::serde_as;

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct Entry {
        key: String,
        value: String,
    }

    impl MapAsVecItem for Entry {
        type Key = String;

        fn key(&self) -> Self::Key {
            self.key.clone()
        }
    }

    #[serde_as]
    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(transparent)]
    struct MyStruct(#[serde_as(as = "MapAsVec")] HashMap<String, Entry>);

    #[test]
    fn roundtrip() {
        let e1 = Entry {
            key: "a".to_string(),
            value: "b".to_string(),
        };
        let e2 = Entry {
            key: "c".to_string(),
            value: "d".to_string(),
        };

        let expected = MyStruct(HashMap::from([
            ("a".to_owned(), e1.clone()),
            ("c".to_owned(), e2.clone()),
        ]));
        let serialized = serde_json::to_value(expected.clone()).unwrap();
        assert_eq!(serialized.as_array().unwrap().len(), 2);

        let actual: MyStruct = serde_json::from_value(serialized).unwrap();
        assert_eq!(actual, expected);
    }
}
