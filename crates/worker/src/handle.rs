// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::future::Future;

use restate_types::invocation::InvocationTermination;
use restate_types::state_mut::ExternalStateMutation;

use crate::WorkerHandleError;

pub trait WorkerHandle {
    /// Send a command to terminate an invocation. This command is best-effort.
    fn terminate_invocation(
        &self,
        invocation_termination: InvocationTermination,
    ) -> impl Future<Output = Result<(), WorkerHandleError>> + Send;

    /// Send a command to mutate a state. This command is best-effort.
    fn external_state_mutation(
        &self,
        mutation: ExternalStateMutation,
    ) -> impl Future<Output = Result<(), WorkerHandleError>> + Send;
}
