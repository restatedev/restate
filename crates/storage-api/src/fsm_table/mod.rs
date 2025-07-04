// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use restate_types::SemanticRestateVersion;
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

pub trait ReadOnlyFsmTable {
    fn get_inbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_;

    fn get_outbox_seq_number(&mut self) -> impl Future<Output = Result<MessageIndex>> + Send + '_;

    fn get_applied_lsn(&mut self) -> impl Future<Output = Result<Option<Lsn>>> + Send + '_;

    fn get_min_restate_version(
        &mut self,
    ) -> impl Future<Output = Result<SemanticRestateVersion>> + Send + '_;
}

pub trait FsmTable: ReadOnlyFsmTable {
    fn put_applied_lsn(&mut self, lsn: Lsn) -> impl Future<Output = Result<()>> + Send;

    fn put_inbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> impl Future<Output = Result<()>> + Send;

    fn put_outbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> impl Future<Output = Result<()>> + Send;

    fn put_min_restate_version(
        &mut self,
        version: &SemanticRestateVersion,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Debug, Clone, Copy, derive_more::From, derive_more::Into)]
pub struct SequenceNumber(pub u64);

impl PartitionStoreProtobufValue for SequenceNumber {
    type ProtobufType = crate::protobuf_types::v1::SequenceNumber;
}
