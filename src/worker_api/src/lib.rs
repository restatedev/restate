// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::ServiceInvocationId;
use std::future::Future;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("worker is unreachable")]
    Unreachable,
}

pub trait Handle {
    type Future: Future<Output = Result<(), Error>> + Send;

    /// Send a command to kill an invocation. This command is best-effort.
    fn kill_invocation(&self, service_invocation_id: ServiceInvocationId) -> Self::Future;
}
