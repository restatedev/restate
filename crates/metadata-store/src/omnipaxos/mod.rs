// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Request;
use omnipaxos::messages::Message;
use restate_rocksdb::RocksError;

mod service;
mod storage;
mod store;

pub use service::OmnipaxosMetadataStoreService;

type OmniPaxosMessage = Message<Request>;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("failed building OmniPaxos: {0}")]
    OmniPaxos(#[from] omnipaxos::errors::ConfigError),
    #[error("failed opening RocksDb: {0}")]
    OpeningRocksDb(#[from] RocksError),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {}
