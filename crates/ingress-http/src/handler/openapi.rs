// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::Full;
use serde_json::{Map, Value};

use restate_core::TaskCenter;
use restate_types::config::Configuration;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::schema::service::ServiceMetadataResolver;

use super::{APPLICATION_JSON, Handler};
use crate::handler::error::HandlerError;

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + InvocationTargetResolver + Send + Sync + 'static,
{
    pub(crate) fn handle_openapi<B: http_body::Body>(
        &mut self,
        req: Request<B>,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }

        let ingress_address = TaskCenter::with_current(|tc| {
            Configuration::pinned()
                .ingress
                .advertised_address(tc.address_book())
        });

        let schemas = self.schemas.pinned();
        let service_names = schemas.list_service_names();

        // Collect individual OpenAPI specs. Schema names are already prefixed
        // with the service name (or dev.restate.openapi.prefix) at generation
        // time, so no renaming is needed here — just merge.
        let mut specs: Vec<Value> = Vec::with_capacity(service_names.len());
        for name in &service_names {
            if let Some(spec) =
                schemas.resolve_latest_service_openapi(name, ingress_address.clone())
            {
                specs.push(spec);
            }
        }

        // Merge all specs into one
        let mut iter = specs.into_iter();
        let mut merged = iter.next().unwrap_or_else(empty_openapi);
        for next in iter {
            merge_openapi(&mut merged, next);
        }

        // Replace per-service info with a generic title for the combined document
        if let Some(info) = merged.get_mut("info").and_then(Value::as_object_mut) {
            info.insert(
                "title".to_owned(),
                Value::String("Restate Services".to_owned()),
            );
            info.insert(
                "description".to_owned(),
                Value::String(
                    "Combined OpenAPI specification for all registered Restate services."
                        .to_owned(),
                ),
            );
        }

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, APPLICATION_JSON)
            .body(Full::new(
                serde_json::to_vec(&merged)
                    .expect("Serializing OpenAPI spec must not fail")
                    .into(),
            ))
            .unwrap())
    }
}

fn empty_openapi() -> Value {
    serde_json::json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Restate Services",
            "description": "Combined OpenAPI specification for all registered Restate services."
        },
        "paths": {}
    })
}

/// Shallow merge of `other` OpenAPI JSON into `target`.
/// Merges paths, component schemas/responses/parameters, tags, and servers.
fn merge_openapi(target: &mut Value, other: Value) {
    let other = match other {
        Value::Object(m) => m,
        _ => return,
    };

    let target = match target.as_object_mut() {
        Some(m) => m,
        None => return,
    };

    // Merge paths
    if let Some(Value::Object(other_paths)) = other.get("paths") {
        let paths = target
            .entry("paths")
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(paths) = paths.as_object_mut() {
            for (path, item) in other_paths {
                paths.entry(path).or_insert_with(|| item.clone());
            }
        }
    }

    // Merge components (schemas, responses, parameters)
    if let Some(Value::Object(other_components)) = other.get("components") {
        let components = target
            .entry("components")
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(components) = components.as_object_mut() {
            for section in ["schemas", "responses", "parameters"] {
                if let Some(Value::Object(other_section)) = other_components.get(section) {
                    let target_section = components
                        .entry(section)
                        .or_insert_with(|| Value::Object(Map::new()));
                    if let Some(target_section) = target_section.as_object_mut() {
                        for (name, value) in other_section {
                            target_section.entry(name).or_insert_with(|| value.clone());
                        }
                    }
                }
            }
        }
    }

    // Merge tags (deduplicate by name)
    if let Some(Value::Array(other_tags)) = other.get("tags") {
        let tags = target
            .entry("tags")
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Some(tags) = tags.as_array_mut() {
            for tag in other_tags {
                let tag_name = tag.get("name").and_then(Value::as_str);
                let already_present = tag_name.is_some_and(|name| {
                    tags.iter()
                        .any(|t| t.get("name").and_then(Value::as_str) == Some(name))
                });
                if !already_present {
                    tags.push(tag.clone());
                }
            }
        }
    }

    // Merge servers (deduplicate by url)
    if let Some(Value::Array(other_servers)) = other.get("servers") {
        let servers = target
            .entry("servers")
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Some(servers) = servers.as_array_mut() {
            for server in other_servers {
                let server_url = server.get("url").and_then(Value::as_str);
                let already_present = server_url.is_some_and(|url| {
                    servers
                        .iter()
                        .any(|s| s.get("url").and_then(Value::as_str) == Some(url))
                });
                if !already_present {
                    servers.push(server.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_openapi_combines_paths_and_schemas() {
        let mut target = json!({
            "openapi": "3.1.0",
            "info": { "title": "A" },
            "paths": {
                "/a/greet": { "post": {} }
            },
            "components": {
                "schemas": {
                    "AgreetRequest": { "type": "object" }
                }
            },
            "tags": [{ "name": "Send" }],
            "servers": [{ "url": "http://localhost:8080/" }]
        });

        let other = json!({
            "openapi": "3.1.0",
            "info": { "title": "B" },
            "paths": {
                "/b/run": { "post": {} }
            },
            "components": {
                "schemas": {
                    "BrunRequest": { "type": "string" }
                }
            },
            "tags": [{ "name": "Send" }, { "name": "Attach" }],
            "servers": [{ "url": "http://localhost:8080/" }, { "url": "http://other:9090/" }]
        });

        merge_openapi(&mut target, other);

        // Both paths present
        let paths = target["paths"].as_object().unwrap();
        assert!(paths.contains_key("/a/greet"));
        assert!(paths.contains_key("/b/run"));

        // Both schemas present
        let schemas = target["components"]["schemas"].as_object().unwrap();
        assert!(schemas.contains_key("AgreetRequest"));
        assert!(schemas.contains_key("BrunRequest"));

        // Tags deduplicated
        let tags = target["tags"].as_array().unwrap();
        let tag_names: Vec<&str> = tags.iter().filter_map(|t| t["name"].as_str()).collect();
        assert_eq!(tag_names, vec!["Send", "Attach"]);

        // Servers deduplicated
        let servers = target["servers"].as_array().unwrap();
        let urls: Vec<&str> = servers.iter().filter_map(|s| s["url"].as_str()).collect();
        assert_eq!(urls, vec!["http://localhost:8080/", "http://other:9090/"]);
    }

    #[test]
    fn merge_openapi_does_not_overwrite_existing_schemas() {
        let mut target = json!({
            "paths": {},
            "components": {
                "schemas": { "Shared": { "type": "object", "title": "from_target" } }
            }
        });

        let other = json!({
            "paths": {},
            "components": {
                "schemas": { "Shared": { "type": "object", "title": "from_other" } }
            }
        });

        merge_openapi(&mut target, other);

        // Target's version wins
        assert_eq!(
            target["components"]["schemas"]["Shared"]["title"],
            "from_target"
        );
    }
}
