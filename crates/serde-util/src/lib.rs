// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod byte_count;
mod header_map;
#[cfg(feature = "proto")]
mod proto;

pub mod authority;
pub mod default;
pub mod header_value;
mod map_as_vec;
mod version;

pub use byte_count::*;
pub use header_map::SerdeableHeaderHashMap;
pub use header_value::HeaderValueSerde;
pub use map_as_vec::{MapAsVec, MapAsVecItem};
#[cfg(feature = "proto")]
pub use proto::ProtobufEncoded;
pub use version::VersionSerde;
