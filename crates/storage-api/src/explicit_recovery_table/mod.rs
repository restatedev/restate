// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The state of an invocation on the explicit recovery protocol family that lives beside its
//! status: its invocation state, and the signals it received.

use std::future::Future;

use bytes::Bytes;

use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::SignalResult;

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

/// A stored signal: what it carried. Wraps [`SignalResult`] for the storage codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSignal(pub SignalResult);

impl PartitionStoreProtobufValue for StoredSignal {
    type ProtobufType = crate::protobuf_types::v1::ExplicitRecoverySignal;
}

pub trait ReadExplicitRecoveryTable {
    fn get_invocation_state(
        &mut self,
        invocation_id: &InvocationId,
        key: &Bytes,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send;

    /// Every key of the invocation state, with its value, in key order.
    fn get_all_invocation_state(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<Vec<(Bytes, Bytes)>>> + Send;

    /// The signal `name` number `index` (from 0), if the invocation received it.
    fn get_signal(
        &mut self,
        invocation_id: &InvocationId,
        name: &str,
        index: u32,
    ) -> impl Future<Output = Result<Option<SignalResult>>> + Send;

    /// The index the next signal of `name` gets: the number of signals of that name received.
    fn next_signal_index(
        &mut self,
        invocation_id: &InvocationId,
        name: &str,
    ) -> impl Future<Output = Result<u32>> + Send;
}

pub trait WriteExplicitRecoveryTable {
    fn put_invocation_state(
        &mut self,
        invocation_id: &InvocationId,
        key: &Bytes,
        value: &Bytes,
    ) -> Result<()>;

    fn delete_invocation_state(&mut self, invocation_id: &InvocationId, key: &Bytes) -> Result<()>;

    /// Deletes the invocation state, and leaves the signals.
    fn delete_all_invocation_state(&mut self, invocation_id: &InvocationId) -> Result<()>;

    fn put_signal(
        &mut self,
        invocation_id: &InvocationId,
        name: &str,
        index: u32,
        result: &SignalResult,
    ) -> Result<()>;

    /// Deletes everything this table holds for the invocation: state and signals.
    fn delete_all_explicit_recovery(&mut self, invocation_id: &InvocationId) -> Result<()>;
}
