// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![cfg(feature = "discovery")]

//! This module has a test to run manually that can be tested against the
//! [Counter example](https://github.com/restatedev/sdk-java/blob/main/examples/src/main/java/dev/restate/sdk/examples/BlockingCounter.java) in sdk-java.

use hyper::Uri;
use restate_service_client::{
    AssumeRoleCacheMode, Options as ServiceClientOptions, ServiceEndpointAddress,
};
use restate_service_protocol::discovery::*;
use restate_test_util::test;
use restate_types::retries::RetryPolicy;

#[ignore]
#[test(tokio::test)]
async fn counter_discovery() {
    let discovery = ServiceDiscovery::new(
        RetryPolicy::None,
        ServiceClientOptions::default().build(AssumeRoleCacheMode::None),
    );

    let discovered_metadata = discovery
        .discover(&DiscoverEndpoint::new(
            ServiceEndpointAddress::Http(
                Uri::from_static("http://localhost:9080"),
                Default::default(),
            ),
            Default::default(),
        ))
        .await
        .unwrap();

    println!("{:#?}", discovered_metadata);
}
