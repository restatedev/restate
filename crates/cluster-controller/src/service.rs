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
pub type ClusterControllerHandle = ();

impl Service {
    pub fn new(options: Options) -> Self {
        Service { options }
    }

    pub fn handle(&self) -> ClusterControllerHandle {}

    pub async fn run(self, shutdown_watch: drain::Watch) -> Result<(), Error> {
        let _ = shutdown_watch.signaled().await;
        Ok(())
    }
}
