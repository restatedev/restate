// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::collections::HashMap;
use std::sync::OnceLock;

use http::HeaderMap;
use opentelemetry_otlp::{
    Protocol, SpanExporter as OTelSpanExporter, WithExportConfig, WithHttpConfig, WithTonicConfig,
};
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, ClientTlsConfig};

use restate_serde_util::SerdeableHeaderHashMap;
use restate_types::GenerationalNodeId;

static GLOBAL_NODE_ID: OnceLock<GenerationalNodeId> = OnceLock::new();

pub fn set_global_node_id(node_id: GenerationalNodeId) {
    GLOBAL_NODE_ID
        .set(node_id)
        .expect("Global NodeId is not set")
}

#[derive(Debug, Clone)]
pub enum ExporterBuilder {
    Tonic {
        metadata: MetadataMap,
        channel: Channel,
        protocol: Protocol,
    },
    Http {
        client: reqwest::Client,
        headers: HashMap<String, String>,
        protocol: Protocol,
        endpoint: http::Uri,
    },
}

impl ExporterBuilder {
    pub fn new(
        endpoint: impl AsRef<str>,
        tracing_headers: SerdeableHeaderHashMap,
    ) -> Result<Self, super::Error> {
        // Parse it as a URI and extract the scheme.
        let endpoint = endpoint.as_ref();

        let uri = endpoint
            .parse::<http::Uri>()
            .map_err(|e| super::bad_endpoint(format!("{endpoint}: URI: {e}")))?;

        let scheme = uri
            .scheme()
            .ok_or_else(|| super::bad_endpoint(format!("{endpoint}: no scheme")))?
            .to_string();

        // Tokenize the scheme on '+' to determine the type of exporter.
        let mut scheme_tokens: Vec<&str> = scheme.split('+').collect();
        scheme_tokens.sort();

        enum Transport {
            Tonic, // gRPC
            Http,  // HTTP(s)
        }

        // Map specific token combinations to ultimate endpoint scheme, exporter
        // transport, and exporter protocol.
        let (final_scheme, use_transport, use_protocol) = match scheme_tokens.as_slice() {
            ["http"] => ("http", Transport::Tonic, Protocol::Grpc),
            ["https"] => ("https", Transport::Tonic, Protocol::Grpc),
            ["grpc"] => ("http", Transport::Tonic, Protocol::Grpc),
            ["grpc", "otlp"] => ("http", Transport::Tonic, Protocol::Grpc),
            ["http", "otlp"] => ("http", Transport::Http, Protocol::HttpBinary),
            ["https", "otlp"] => ("https", Transport::Http, Protocol::HttpBinary),
            ["http", "otlp", "proto"] => ("http", Transport::Http, Protocol::HttpBinary),
            ["https", "otlp", "proto"] => ("https", Transport::Http, Protocol::HttpBinary),
            ["http", "json", "otlp"] => ("http", Transport::Http, Protocol::HttpJson),
            ["https", "json", "otlp"] => ("https", Transport::Http, Protocol::HttpJson),
            _ => return Err(super::bad_endpoint(format!("{endpoint}: invalid scheme"))),
        };

        // Reconstruct the endpoint with the ultimate scheme from above.
        let endpoint = http::uri::Builder::from(uri)
            .scheme(final_scheme)
            .build()
            .map_err(|e| super::bad_endpoint(format!("rebuild endpoint: {e}")))?;

        // Build the exporter as specified.
        let builder = match use_transport {
            Transport::Tonic => {
                let metadata_headers: MetadataMap =
                    MetadataMap::from_headers(HeaderMap::from_iter(HashMap::from(tracing_headers)));
                let channel = Channel::builder(endpoint)
                    .tls_config(ClientTlsConfig::new().with_native_roots())
                    .map_err(|err| super::Error::Other(err.into()))?
                    .connect_lazy();

                ExporterBuilder::Tonic {
                    metadata: metadata_headers,
                    channel,
                    protocol: use_protocol,
                }
            }
            Transport::Http => {
                let client = reqwest::Client::builder()
                    .use_rustls_tls() // match with_tonic with_tls_config
                    .tls_built_in_root_certs(true) // match with_tonic with_tls_config
                    .build()
                    .map_err(|e| super::bad_endpoint(format!("build HTTP client: {e}")))?;
                let string_headers: HashMap<String, String> = HashMap::from(tracing_headers)
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            k.as_str().into(),
                            String::from_utf8_lossy(v.as_bytes()).into(),
                        )
                    })
                    .collect();
                ExporterBuilder::Http {
                    client,
                    headers: string_headers,
                    protocol: use_protocol,
                    endpoint,
                }
            }
        };

        Ok(builder)
    }

    pub fn build(&self) -> Result<opentelemetry_otlp::SpanExporter, super::Error> {
        match self {
            ExporterBuilder::Tonic {
                metadata,
                channel,
                protocol,
            } => Ok(OTelSpanExporter::builder()
                .with_tonic()
                .with_channel(channel.clone())
                .with_metadata(metadata.clone())
                .with_protocol(*protocol)
                .build()
                .map_err(|e| super::bad_endpoint(format!("build gRPC exporter: {e}")))?),

            ExporterBuilder::Http {
                client,
                headers,
                protocol,
                endpoint,
            } => Ok(OTelSpanExporter::builder()
                .with_http()
                .with_http_client(client.clone())
                .with_protocol(*protocol)
                .with_headers(headers.clone())
                .with_endpoint(endpoint.to_string())
                .build()
                .map_err(|e| super::bad_endpoint(format!("build HTTP exporter: {e}")))?),
        }
    }
}
