// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use codederror::CodedError;
use restate_types::errors::GenericError;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("failed binding to address '{address}' specified in 'admin.bind_address'")]
    #[code(restate_errors::RT0004)]
    Binding {
        address: SocketAddr,
        #[source]
        source: GenericError,
    },
    #[error("error while running admin server: {0}")]
    #[code(unknown)]
    Running(GenericError),
}
