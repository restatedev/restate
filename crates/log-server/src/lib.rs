// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod error;
mod grpc_svc_handler;
#[cfg(feature = "expose-internals")]
pub mod loglet_worker;
#[cfg(not(feature = "expose-internals"))]
mod loglet_worker;
#[cfg(feature = "expose-internals")]
pub mod logstore;
#[cfg(not(feature = "expose-internals"))]
mod logstore;
#[cfg(feature = "expose-internals")]
pub mod metadata;
#[cfg(not(feature = "expose-internals"))]
mod metadata;
mod metric_definitions;
mod network;
pub mod protobuf;
#[cfg(feature = "expose-internals")]
pub mod rocksdb_logstore;
#[cfg(not(feature = "expose-internals"))]
mod rocksdb_logstore;
mod service;

pub use error::LogServerBuildError;
pub use service::LogServerService;
