// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use metrics_exporter_prometheus::PrometheusHandle;
use restate_bifrost::Bifrost;
use restate_storage_rocksdb::RocksDBStorage;

#[derive(Clone, derive_builder::Builder)]
pub struct HandlerState {
    pub prometheus_handle: Option<PrometheusHandle>,
    pub rocksdb_storage: Option<RocksDBStorage>,
    pub bifrost: Bifrost,
}
