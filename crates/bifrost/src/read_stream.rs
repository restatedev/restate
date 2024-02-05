// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::bifrost::BifrostInner;
use crate::{LogId, Lsn};

pub struct LogReadStream {}

impl LogReadStream {
    pub(crate) fn new(_inner: Arc<BifrostInner>, _log_id: LogId, _from: Lsn) -> Self {
        Self {}
    }
}
