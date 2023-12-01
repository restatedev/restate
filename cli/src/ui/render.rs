// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_meta_rest_model::endpoints::{ProtocolType, ServiceEndpoint};

pub fn render_endpoint_url(svc_endpoint: &ServiceEndpoint) -> String {
    match svc_endpoint {
        ServiceEndpoint::Http { uri, .. } => uri.to_string(),
        ServiceEndpoint::Lambda { arn, .. } => arn.to_string(),
    }
}

pub fn render_endpoint_type(svc_endpoint: &ServiceEndpoint) -> String {
    match svc_endpoint {
        ServiceEndpoint::Http { protocol_type, .. } => {
            format!(
                "HTTP {}",
                if protocol_type == &ProtocolType::BidiStream {
                    "2"
                } else {
                    "1"
                }
            )
        }
        ServiceEndpoint::Lambda { .. } => "AWS Lambda".to_string(),
    }
}
