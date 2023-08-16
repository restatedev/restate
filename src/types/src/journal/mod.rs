// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
pub use entries::*;

use crate::identifiers::EndpointId;
use crate::invocation::{ResponseResult, ServiceInvocationSpanContext};
use bytes::Bytes;
use bytestring::ByteString;

pub type EntryIndex = u32;

/// Metadata associated with a journal
#[derive(Debug, Clone, PartialEq)]
pub struct JournalMetadata {
    pub endpoint_id: Option<EndpointId>,
    pub length: EntryIndex,
    pub method: String,
    pub span_context: ServiceInvocationSpanContext,
}

impl JournalMetadata {
    pub fn new(
        method: impl Into<String>,
        span_context: ServiceInvocationSpanContext,
        length: EntryIndex,
    ) -> Self {
        Self {
            endpoint_id: None,
            method: method.into(),
            span_context,
            length,
        }
    }
}
