// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines messages between replicated loglet instances

mod arc_encodings;
mod nonzero;
mod phantom_data;
mod range;

pub mod display_from_str;

pub use arc_encodings::{Arced, ArcedSlice};

/// The encoding used for all 3rd party types in the `bilrost_encoding` crate.
///
/// example:
/// ```ignore
/// #[derive(bilrost::Message)]
/// struct MyMessage {
///     #[bilrost(tag(1), encoding(RestateEncoding))]
///     range: RangeInclusive<u64>,
/// }
pub struct RestateEncoding;

bilrost::encoding_implemented_via_value_encoding!(RestateEncoding);
bilrost::implement_core_empty_state_rules!(RestateEncoding);
