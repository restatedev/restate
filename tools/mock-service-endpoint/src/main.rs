// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::filter::LevelFilter;

use mock_service_endpoint::listener::run_listener;

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

    let addr: SocketAddr = ([127, 0, 0, 1], 9080).into();
    let listener = TcpListener::bind(addr).await?;

    run_listener(listener, || info!("Listening on http://{}", addr)).await
}
