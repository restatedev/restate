// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::ShutdownError;

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum ProviderError {
    Shutdown(#[from] ShutdownError),
    Other(#[from] anyhow::Error),
}
