// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod connector;
mod svc_handler;

use tonic::codec::CompressionEncoding;

pub use connector::GrpcConnector;
pub use restate_types::net::connect_opts::DEFAULT_MAX_MESSAGE_SIZE as MAX_MESSAGE_SIZE;
pub use svc_handler::CoreNodeSvcHandler;

/// Default send compression for grpc clients
pub const DEFAULT_GRPC_COMPRESSION: CompressionEncoding = CompressionEncoding::Zstd;
