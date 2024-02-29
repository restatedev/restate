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

use restate_meta::{FileMetaReader, MetaHandle};
use restate_node_services::node_svc::node_svc_client::NodeSvcClient;
use restate_schema_impl::Schemas;
use tonic::transport::Channel;

#[derive(Clone, derive_builder::Builder)]
pub struct AdminServiceState<W> {
    meta_handle: MetaHandle,
    schemas: Schemas,
    worker_handle: W,
    node_svc_client: NodeSvcClient<Channel>,
    schema_reader: FileMetaReader,
}

#[derive(Clone)]
pub struct QueryServiceState {
    pub node_svc_client: NodeSvcClient<Channel>,
}

impl<W> AdminServiceState<W> {
    pub fn new(
        meta_handle: MetaHandle,
        schemas: Schemas,
        worker_handle: W,
        node_svc_client: NodeSvcClient<Channel>,
        schema_reader: FileMetaReader,
    ) -> Self {
        Self {
            meta_handle,
            schemas,
            worker_handle,
            node_svc_client,
            schema_reader,
        }
    }

    pub fn meta_handle(&self) -> &MetaHandle {
        &self.meta_handle
    }

    pub fn schemas(&self) -> &Schemas {
        &self.schemas
    }

    pub fn node_svc_client(&self) -> NodeSvcClient<Channel> {
        self.node_svc_client.clone()
    }

    pub fn schema_reader(&self) -> &FileMetaReader {
        &self.schema_reader
    }
}

impl<W: Clone> AdminServiceState<W> {
    pub fn worker_handle(&self) -> W {
        self.worker_handle.clone()
    }
}
