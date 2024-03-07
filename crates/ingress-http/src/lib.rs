// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod handler;
mod layers;
mod metric_definitions;
mod options;
mod server;

pub use options::{Options, OptionsBuilder, OptionsBuilderError};
pub use server::{HyperServerIngress, IngressServerError, StartSignal};

use bytes::Bytes;
use std::net::{IpAddr, SocketAddr};

/// Client connection information for a given RPC request
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectInfo {
    remote: SocketAddr,
}

impl ConnectInfo {
    fn new(remote: SocketAddr) -> Self {
        Self { remote }
    }
    fn address(&self) -> IpAddr {
        self.remote.ip()
    }
    fn port(&self) -> u16 {
        self.remote.port()
    }
}

// Contains some mocks we use in unit tests in this crate
#[cfg(test)]
mod mocks {
    use restate_schema_api::component::mocks::MockComponentMetadataResolver;
    use restate_schema_api::component::ComponentMetadata;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub(super) struct GreetingRequest {
        pub person: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub(super) struct GreetingResponse {
        pub greeting: String,
    }

    pub(super) fn mock_component_resolver() -> MockComponentMetadataResolver {
        let mut components = MockComponentMetadataResolver::default();

        components.add(ComponentMetadata::mock_service(
            "greeter.Greeter",
            ["greet"],
        ));
        components.add(ComponentMetadata::mock_virtual_object(
            "greeter.GreeterObject",
            ["greet"],
        ));
        components.add({
            let mut private_component_metadata =
                ComponentMetadata::mock_service("greeter.GreeterPrivate", ["greet"]);
            private_component_metadata.public = false;
            private_component_metadata
        });

        components
    }
}
