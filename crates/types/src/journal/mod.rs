// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! To implement the Durable execution, we model the invocation state machine using a journal.
//! This module defines the journal model.

pub mod enriched;
mod entries;
pub mod raw;

// Re-export all the entries
use crate::invocation::ResponseResult;
use bytes::Bytes;
use bytestring::ByteString;
pub use entries::*;

pub type EntryIndex = u32;
