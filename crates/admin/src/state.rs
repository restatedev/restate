// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//

use restate_bifrost::Bifrost;
use restate_core::metadata_store::MetadataStoreClient;
use restate_core::{MetadataWriter, TaskCenter};
use restate_node_services::node_svc::node_svc_client::NodeSvcClient;
use restate_service_protocol::discovery::ComponentDiscovery;
use tonic::transport::Channel;

#[derive(Clone, derive_builder::Builder)]
pub struct AdminServiceState<V> {
    pub metadata_writer: MetadataWriter,
    pub metadata_store_client: MetadataStoreClient,
    pub subscription_validator: V,
    pub component_discovery: ComponentDiscovery,
    pub bifrost: Bifrost,
    pub task_center: TaskCenter,
}

#[derive(Clone)]
pub struct QueryServiceState {
    pub node_svc_client: NodeSvcClient<Channel>,
}

impl<V> AdminServiceState<V> {
    pub fn new(
        metadata_writer: MetadataWriter,
        metadata_store_client: MetadataStoreClient,
        subscription_validator: V,
        component_discovery: ComponentDiscovery,
        bifrost: Bifrost,
        task_center: TaskCenter,
    ) -> Self {
        Self {
            metadata_writer,
            metadata_store_client,
            subscription_validator,
            component_discovery,
            bifrost,
            task_center,
        }
    }
}
