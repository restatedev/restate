// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::ServiceId;
use bytes::Bytes;
use std::collections::HashMap;

/// ExternalStateMutation
///
/// represents an external request to mutate a user's state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExternalStateMutation {
    pub service_id: ServiceId,
    pub version: Option<String>,
    pub state: HashMap<Bytes, Bytes>,
}
