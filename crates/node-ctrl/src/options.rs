// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use restate_bifrost::Bifrost;
use restate_cluster_controller::ClusterControllerHandle;
use restate_storage_rocksdb::RocksDBStorage;
use serde_with::serde_as;

use crate::service::NodeCtrlService;

/// # Node ctrl service options
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "NodeCtrlOptions"))]
#[cfg_attr(feature = "options_schema", schemars(default))]
pub struct Options {
    /// Address to bind for the Node ctrl Service.
    pub bind_address: SocketAddr,

    /// Timeout for idle histograms.
    ///
    /// The duration after which a histogram is considered idle and will be removed from
    /// metric responses to save memory. Unsetting means that histograms will never be removed.
    #[serde(with = "serde_with::As::<Option<serde_with::DisplayFromStr>>")]
    #[cfg_attr(feature = "options_schema", schemars(with = "Option<String>"))]
    pub histogram_inactivity_timeout: Option<humantime::Duration>,

    /// Disable prometheus metric recording and reporting. Default is `false`.
    pub disable_prometheus: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:5122".parse().unwrap(),
            histogram_inactivity_timeout: None,
            disable_prometheus: false,
        }
    }
}

impl Options {
    pub fn build(
        self,
        worker: Option<(RocksDBStorage, Bifrost)>,
        cluster_controller: Option<ClusterControllerHandle>,
    ) -> NodeCtrlService {
        NodeCtrlService::new(self, worker, cluster_controller)
    }
}
