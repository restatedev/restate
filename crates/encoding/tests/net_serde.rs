// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_encoding::NetSerde;
use restate_platform::hash::HashMap;
use restate_platform::network::NetSerde;

#[allow(dead_code)]
#[derive(NetSerde)]
struct SomeMessage {
    a: u64,
    b: String,
    c: (bool, u64),
    d: Inner,
    #[net_serde(skip)]
    f: NotSendable,
}

struct NotSendable;

#[allow(dead_code)]
#[derive(NetSerde)]
struct Inner(HashMap<u64, String>);

const _: fn() = || {
    fn assert_impl_all<T: ?Sized + NetSerde>() {}
    assert_impl_all::<SomeMessage>();
};
