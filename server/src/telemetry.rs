// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{str::FromStr, time::Duration};

use http::{HeaderMap, HeaderValue, Uri, uri::PathAndQuery};
use restate_core::{TaskCenter, TaskKind, cancellation_watcher};
use restate_service_client::HttpClient;
use restate_types::config::CommonOptions;
use tokio::time::Instant;
use tracing::debug;

use crate::build_info::{RESTATE_SERVER_TARGET_TRIPLE, RESTATE_SERVER_VERSION, is_debug};

static TELEMETRY_URI: &str = "https://restate.gateway.scarf.sh/restate-server/";
const TELEMETRY_PERIOD: Duration = Duration::from_secs(3600 * 24);

#[allow(clippy::large_enum_variant)]
pub enum Telemetry {
    Enabled(Box<TelemetryEnabled>),
    Disabled,
}

pub struct TelemetryEnabled {
    client: HttpClient,
    session_id: String,
    start_time: Instant,
}

impl Telemetry {
    pub fn create(options: &CommonOptions) -> Self {
        if options.disable_telemetry {
            Self::Disabled
        } else {
            let client = HttpClient::from_options(&options.service_client.http);
            let session_id = ulid::Ulid::new().to_string();

            Self::Enabled(Box::new(TelemetryEnabled {
                client,
                session_id,
                start_time: Instant::now(),
            }))
        }
    }

    pub fn start(self) {
        match self {
            Telemetry::Disabled => {}
            Telemetry::Enabled(enabled) => {
                if let Err(err) =
                    TaskCenter::spawn_child(TaskKind::SystemService, "telemetry-service", async {
                        let cancelled = cancellation_watcher();

                        tokio::select! {
                            _ = enabled.run() => {}
                            _ = cancelled =>{}
                        }

                        Ok(())
                    })
                {
                    debug!(error = %err, "Failed to start telemetry service");
                }
            }
        }
    }
}

impl TelemetryEnabled {
    async fn run(self) {
        let mut interval = tokio::time::interval(TELEMETRY_PERIOD);
        loop {
            interval.tick().await; // first tick completes immediately
            self.send_telemetry().await;
        }
    }

    async fn send_telemetry(&self) {
        let uptime_hours =
            (Instant::now().duration_since(self.start_time).as_secs() / 3600).to_string();
        let session_id = self.session_id.as_str();

        let is_debug = is_debug();
        let uri = Uri::from_str(&format!(
            "{TELEMETRY_URI}?target={RESTATE_SERVER_TARGET_TRIPLE}&version={RESTATE_SERVER_VERSION}&debug={is_debug}&uptime={uptime_hours}&session={session_id}",
        )).expect("uri must parse");

        debug!(%uri, "Sending telemetry data");

        match self
            .client
            .request(
                uri,
                None,
                http::Method::GET,
                http_body_util::Empty::new(),
                PathAndQuery::from_static("/"),
                HeaderMap::from_iter([(
                    http::header::USER_AGENT,
                    HeaderValue::from_static("restate-server"),
                )]),
            )
            .await
        {
            Ok(resp) => {
                debug!(status = %resp.status(), "Sent telemetry data")
            }
            Err(err) => {
                debug!(error = %err, "Failed to send telemetry data")
            }
        }
    }
}
