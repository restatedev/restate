// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::partition_table::PartitionTableError;

#[derive(Debug, thiserror::Error)]
pub enum IngressDispatchError {
    #[error("bifrost error: {0}")]
    WalProtocol(#[from] restate_wal_protocol::Error),
    #[error("partition routing error: {0}")]
    PartitionRoutingError(#[from] PartitionTableError),
}
