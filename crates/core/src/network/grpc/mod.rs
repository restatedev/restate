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

pub use connector::GrpcConnector;
pub use svc_handler::CoreNodeSvcHandler;
use tonic::codec::CompressionEncoding;

/// The maximum size for a grpc message for core networking service.
/// This impacts the buffer limit for prost codec.
pub const MAX_MESSAGE_SIZE: usize = 32 * 1024 * 1024;

/// Default send compression for grpc clients
pub const DEFAULT_GRPC_COMPRESSION: CompressionEncoding = CompressionEncoding::Zstd;
