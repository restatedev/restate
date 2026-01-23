// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod connection_manager;
pub mod grpc_svc;
mod handler;
mod networking;

pub use connection_manager::ConnectionManager;
pub use grpc_svc::FILE_DESCRIPTOR_SET;
pub use grpc_svc::metadata_server_network_svc_server::MetadataServerNetworkSvcServer;
pub use handler::MetadataServerNetworkHandler;
pub use networking::{NetworkMessage, Networking};

const PEER_METADATA_KEY: &str = "x-restate-metadata-server-peer";
const CLUSTER_FINGERPRINT_METADATA_KEY: &str = "x-restate-ms-cf";
const CLUSTER_NAME_METADATA_KEY: &str = "x-restate-ms-cn";
