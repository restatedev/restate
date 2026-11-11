// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod codec;
mod connector;
mod svc_handler;

#[cfg(test)]
mod tests;

mod client {
    include!(concat!(
        env!("OUT_DIR"),
        "/fabric-client/restate.core_node_svc.CoreNodeSvc.rs"
    ));
}

mod server {
    include!(concat!(
        env!("OUT_DIR"),
        "/fabric-server/restate.core_node_svc.CoreNodeSvc.rs"
    ));
}

use tonic::codec::CompressionEncoding;

pub use connector::GrpcConnector;
pub use svc_handler::CoreNodeSvcHandler;

/// Default send compression for grpc clients
pub const DEFAULT_GRPC_COMPRESSION: CompressionEncoding = CompressionEncoding::Zstd;
