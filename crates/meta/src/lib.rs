// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod error;
mod service;
mod storage;

pub use error::Error;
pub use service::{ApplyMode, Force, MetaHandle, MetaService};
pub use storage::{FileMetaReader, FileMetaStorage, MetaReader, MetaStorage};

use codederror::CodedError;

#[derive(Debug, thiserror::Error, CodedError)]
#[error("failed building the meta service: {0}")]
pub enum BuildError {
    Storage(
        #[from]
        #[code]
        storage::BuildError,
    ),
    #[code(unknown)]
    ServiceClient(#[from] restate_service_client::BuildError),
}
