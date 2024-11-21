// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod connection;
mod connection_manager;
mod error;
mod handshake;
mod message_router;
pub(crate) mod metric_definitions;
mod multiplex;
pub mod net_util;
mod network_sender;
mod networking;
pub mod partition_processor_rpc_client;
pub mod protobuf;
pub mod rpc_router;
mod server_builder;
pub mod transport_connector;
mod types;

pub use connection::{OwnedConnection, WeakConnection};
pub use connection_manager::ConnectionManager;
pub use error::*;
pub use message_router::*;
pub use network_sender::*;
pub use networking::Networking;
pub use server_builder::NetworkServerBuilder;
pub use transport_connector::{GrpcConnector, TransportConnect};
pub use types::*;

#[cfg(any(test, feature = "test-util"))]
pub use connection::test_util::*;

#[cfg(any(test, feature = "test-util"))]
pub use transport_connector::test_util::*;
