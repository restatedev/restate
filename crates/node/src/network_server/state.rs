// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use restate_core::task_center;
use restate_tracing_instrumentation::prometheus_metrics::Prometheus;

#[derive(Clone, derive_builder::Builder)]
pub struct NodeCtrlHandlerState {
    #[builder(default)]
    pub prometheus_handle: Arc<Prometheus>,
    pub task_center: task_center::Handle,
}
