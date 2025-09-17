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
pub mod grpc;
mod handshake;
mod incoming;
mod io;
mod lazy_connection;
mod message_router;
pub(crate) mod metric_definitions;
pub mod net_util;
mod network_sender;
mod networking;
pub mod protobuf;
mod server_builder;
pub mod tonic_service_filter;
mod tracking;
pub mod transport_connector;
mod types;

pub use connection::{ConnectThrottle, Connection};
pub use connection_manager::ConnectionManager;
pub use error::*;
pub use grpc::GrpcConnector;
pub use incoming::*;
pub use io::{DrainReason, SendToken};
pub use lazy_connection::*;
pub use message_router::*;
pub use network_sender::*;
pub use networking::Networking;
pub use protobuf::network::ConnectionDirection;
pub use server_builder::NetworkServerBuilder;
pub use transport_connector::TransportConnect;
pub use types::*;

#[cfg(feature = "test-util")]
pub use connection::test_util::*;
#[cfg(feature = "test-util")]
pub use incoming::test_util::*;

#[cfg(feature = "test-util")]
pub use transport_connector::test_util::*;
