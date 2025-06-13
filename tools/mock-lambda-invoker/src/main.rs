// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mock_lambda_invoker::SimpleRequestResponseInvoker;
use mock_lambda_invoker::client::ConfiguredClient;
use restate_types::config::ServiceClientOptions;
use tracing::warn;
use tracing_subscriber::filter::LevelFilter;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let format = tracing_subscriber::fmt::format().compact();

    tracing_subscriber::fmt()
        .event_format(format)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    //
    // TODO: take the arn etc' from args
    //
    let opts = ServiceClientOptions::default();
    let client = ConfiguredClient::new(&opts, "arn:", "greeter", "greet");

    for _ in 0..10 {
        let client_clone = client.clone();
        tokio::spawn(async move {
            let invoker = SimpleRequestResponseInvoker::new(client_clone);
            match invoker.invoke(None).await {
                Ok(_) => {
                    // success
                }
                Err(err) => {
                    // failure
                    warn!("{}", err);
                }
            }
        });
    }

    Ok(())
}
