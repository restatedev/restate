#![cfg(feature = "discovery")]

//! This module has a test to run manually that can be tested against the
//! [Counter example](https://github.com/restatedev/sdk-java/blob/main/examples/src/main/java/dev/restate/sdk/examples/BlockingCounter.java) in sdk-java.

use hyper::Uri;
use restate_service_protocol::discovery::*;
use restate_test_util::test;
use restate_types::retries::RetryPolicy;

#[ignore]
#[test(tokio::test)]
async fn counter_discovery() {
    let discovery = ServiceDiscovery::new(RetryPolicy::None, None);

    let discovered_metadata = discovery
        .discover(
            &Uri::from_static("http://localhost:8080"),
            &Default::default(),
        )
        .await
        .unwrap();

    println!("{:#?}", discovered_metadata);
}
