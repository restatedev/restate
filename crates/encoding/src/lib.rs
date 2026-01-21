// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod bilrost_as;
pub mod bilrost_encodings;
mod common;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::sync::Arc;

pub use bilrost_as::BilrostAsAdaptor;
pub use bilrost_encodings::{Arced, ArcedSlice, RestateEncoding};
pub use common::U128;
pub use restate_encoding_derive::{BilrostAs, BilrostNewType, NetSerde};
/// A marker trait for types that can be serialized and sent over the network.
///
/// Types implementing this trait are considered eligible for wire transmission,
/// typically via serialization. It is intended to be implemented automatically
/// using the `#[derive(NetSerde)]` macro.
///
/// # Example
/// ```ignore
/// #[derive(NetSerde)]
/// struct MyMessage {
///     a: u64,
///     b: String,
/// }
/// ```
pub trait NetSerde {}

macro_rules! impl_net_serde {
    ($t:ty) => {
        impl NetSerde for $t {}
    };
    ($($t:ty),+) => {
        $(impl_net_serde!($t);)+
    }
}

impl_net_serde!(
    bool,
    usize,
    u8,
    u16,
    u32,
    u64,
    u128,
    isize,
    i8,
    i16,
    i32,
    i64,
    i128,
    String,
    bytes::Bytes,
    bytestring::ByteString,
    std::time::Duration
);

macro_rules! impl_net_serde_tuple {
    ($($t:ident),+) => {
        impl<$($t),+> NetSerde for ($($t),+) where $($t: NetSerde),+ {}
    };
}

impl_net_serde_tuple!(T0, T1);
impl_net_serde_tuple!(T0, T1, T2);
impl_net_serde_tuple!(T0, T1, T2, T3);
impl_net_serde_tuple!(T0, T1, T2, T3, T4);
impl_net_serde_tuple!(T0, T1, T2, T3, T4, T5);
impl_net_serde_tuple!(T0, T1, T2, T3, T4, T5, T6);

impl<T> NetSerde for Vec<T> where T: NetSerde {}
impl<T> NetSerde for Option<T> where T: NetSerde {}
impl<K, V, S> NetSerde for HashMap<K, V, S>
where
    K: NetSerde,
    V: NetSerde,
{
}

impl<K, V> NetSerde for BTreeMap<K, V>
where
    K: NetSerde,
    V: NetSerde,
{
}

impl<V> NetSerde for HashSet<V> where V: NetSerde {}
impl<Idx> NetSerde for RangeInclusive<Idx> where Idx: NetSerde {}
impl<T> NetSerde for Arc<T> where T: NetSerde {}
impl<T> NetSerde for Arc<[T]> where T: NetSerde {}
impl<T> NetSerde for Box<T> where T: NetSerde {}
impl<T, const N: usize> NetSerde for [T; N] where T: NetSerde {}

#[cfg(test)]
mod test {
    use bilrost::{Message, OwnedMessage};
    use restate_encoding_derive::BilrostNewType;

    #[derive(BilrostNewType)]
    struct MyId(u64);

    #[derive(bilrost::Message)]
    struct Nested {
        id: MyId,
    }

    #[derive(bilrost::Message)]
    struct Flattened {
        id: u64,
    }

    #[test]
    fn test_new_type() {
        let x = Nested { id: MyId(10) };

        let bytes = x.encode_to_bytes();

        let y = Flattened::decode(bytes).expect("decodes");

        assert_eq!(x.id.0, y.id);
    }
}
