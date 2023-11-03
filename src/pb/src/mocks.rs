// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use once_cell::sync::Lazy;
use prost_reflect::DescriptorPool;

pub mod eventhandler {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]
    include!(concat!(env!("OUT_DIR"), "/eventhandler.rs"));
}

pub mod greeter {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]
    include!(concat!(env!("OUT_DIR"), "/greeter.rs"));
}

pub mod test {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

pub static DESCRIPTOR_POOL: Lazy<DescriptorPool> = Lazy::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set_test.bin")).as_ref(),
    )
    .expect("The built-in descriptor pool should be valid")
});

pub static DESCRIPTOR_POOL_V2_INCOMPATIBLE: Lazy<DescriptorPool> = Lazy::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(
            env!("OUT_DIR"),
            "/file_descriptor_set_test-v2_incompatible.bin"
        ))
        .as_ref(),
    )
    .expect("The built-in descriptor pool should be valid")
});

pub const GREETER_SERVICE_NAME: &str = "greeter.Greeter";
pub const ANOTHER_GREETER_SERVICE_NAME: &str = "greeter.AnotherGreeter";

pub const EVENT_HANDLER_SERVICE_NAME: &str = "eventhandler.EventHandler";
pub const KEYED_EVENT_HANDLER_SERVICE_NAME: &str = "eventhandler.KeyedEventHandler";
pub const STRING_KEYED_EVENT_HANDLER_SERVICE_NAME: &str = "eventhandler.StringKeyedEventHandler";
