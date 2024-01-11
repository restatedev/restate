// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_schema_impl::SchemasUpdateError;
use restate_service_protocol::discovery::ServiceDiscoveryError;

use crate::storage::MetaStorageError;

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum Error {
    #[error(transparent)]
    Discovery(
        #[from]
        #[code]
        ServiceDiscoveryError,
    ),
    #[error(transparent)]
    #[code(unknown)]
    Storage(#[from] MetaStorageError),
    #[error(transparent)]
    #[code(unknown)]
    SchemaRegistry(#[from] SchemasUpdateError),
    #[error("meta closed")]
    #[code(unknown)]
    MetaClosed,
    #[error("request aborted because the client went away")]
    #[code(unknown)]
    RequestAborted,
}
