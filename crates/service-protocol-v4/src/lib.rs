// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains the code-generated structs of [service-protocol](https://github.com/restatedev/service-protocol) and the codec to use them.

#[cfg(feature = "entry-codec")]
pub mod entry_codec;
#[cfg(feature = "message-codec")]
pub mod message_codec;

#[allow(clippy::enum_variant_names)]
// We need to allow dead code because the entry-codec feature only uses a subset of the defined
// service protocol messages. Otherwise, crates depending only on this feature fail clippy.
#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/dev.restate.service.protocol.rs"));
}
