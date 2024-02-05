// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Options;
use codederror::CodedError;
use tracing::info;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("error")]
    #[code(unknown)]
    Error,
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error("error")]
    #[code(unknown)]
    Error,
}

#[derive(Debug)]
pub struct AdminRole {}

impl AdminRole {
    pub async fn run(self, shutdown_watch: drain::Watch) -> Result<(), Error> {
        let _ = shutdown_watch.signaled().await;
        info!("Stopped admin");
        Ok(())
    }
}

impl TryFrom<Options> for AdminRole {
    type Error = BuildError;

    fn try_from(_options: Options) -> Result<Self, Self::Error> {
        Ok(AdminRole {})
    }
}
