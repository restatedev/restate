// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains the code-generated structs of [service-protocol](https://github.com/restatedev/service-protocol) and the codec to use them.
//! TODO(slinkydeveloper) get rid of this module when service-protocol version <= 3 gets dropped

pub const RESTATE_SERVICE_PROTOCOL_VERSION: u16 = 2;

#[cfg(feature = "codec")]
pub mod codec;
#[cfg(feature = "discovery")]
pub mod discovery;
#[cfg(feature = "message")]
pub mod message;
