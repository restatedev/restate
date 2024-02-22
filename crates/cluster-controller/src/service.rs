// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::options::Options;
use codederror::CodedError;
use restate_core::cancellation_watcher;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("error")]
    #[code(unknown)]
    Error,
}

#[derive(Debug)]
pub struct Service {
    #[allow(dead_code)]
    options: Options,
}

// todo: Replace with proper handle
pub struct ClusterControllerHandle;

impl Service {
    pub fn new(options: Options) -> Self {
        Service { options }
    }

    pub fn handle(&self) -> ClusterControllerHandle {
        ClusterControllerHandle
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let _ = cancellation_watcher().await;
        Ok(())
    }
}
