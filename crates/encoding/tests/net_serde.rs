use std::collections::HashMap;

use restate_encoding::NetSerde;
use static_assertions::assert_impl_all;

#[allow(dead_code)]
#[derive(NetSerde)]
struct SomeMessage {
    a: u64,
    b: String,
    c: (bool, u64),
    d: Inner,
}

#[allow(dead_code)]
#[derive(NetSerde)]
struct Inner(HashMap<u64, String>);

assert_impl_all!(SomeMessage: NetSerde);
