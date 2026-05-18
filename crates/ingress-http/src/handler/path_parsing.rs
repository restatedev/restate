// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::HandlerError;
use super::{Handler, RequestType};
use http::Uri;
use restate_types::ServiceName;
use restate_types::identifiers::InvocationId;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_util_string::{ReString, ToReString};

pub(crate) enum WorkflowRequestType {
    /// (name, key)
    Attach(ReString, ReString),
    /// (name, key)
    GetOutput(ReString, ReString),
}

impl WorkflowRequestType {
    /// Parse workflow request from unversioned path: `/restate/workflow/{name}/{key}/attach|output`
    /// (old ingress API)
    fn from_path_chunks<'a>(
        mut path_parts: impl Iterator<Item = &'a str>,
    ) -> Result<Self, HandlerError> {
        let workflow_name = path_parts
            .next()
            .ok_or(HandlerError::BadWorkflowPath)?
            .to_restring();
        let workflow_key =
            urlencoding::decode(path_parts.next().ok_or(HandlerError::BadWorkflowPath)?)
                .map_err(HandlerError::UrlDecodingError)?
                .to_restring();

        match path_parts.next().ok_or(HandlerError::BadWorkflowPath)? {
            "output" => Ok(WorkflowRequestType::GetOutput(workflow_name, workflow_key)),
            "attach" => Ok(WorkflowRequestType::Attach(workflow_name, workflow_key)),
            _ => Err(HandlerError::NotFound),
        }
    }
}

pub(crate) enum InvocationTargetType {
    InvocationId(String),
    IdempotencyId {
        name: String,
        target: TargetType,
        handler: String,
        idempotency_id: String,
    },
}

pub(crate) enum InvocationRequestType {
    Attach(InvocationTargetType),
    GetOutput(InvocationTargetType),
}

impl InvocationRequestType {
    fn from_path_chunks<'a, Schemas>(
        mut path_parts: impl Iterator<Item = &'a str>,
        schemas: &Schemas,
    ) -> Result<Self, HandlerError>
    where
        Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    {
        // 🤔 This code could use the experimental api https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.next_chunk

        let first_chunk = path_parts.next().ok_or(HandlerError::BadInvocationPath)?;
        let second_chunk = path_parts.next().ok_or(HandlerError::BadInvocationPath)?;

        let (invocation_target, last_chunk) = if let Some(third_chunk) = path_parts.next() {
            // Idempotency id to either keyed or unkeyed service
            let service_name = first_chunk.to_owned();

            // We need to query the service type before continuing to parse
            let service_type = schemas
                .resolve_latest_service_type(&service_name)
                .ok_or_else(|| HandlerError::ServiceNotFound(service_name.clone()))?;

            let (target, next_chunk, next_next_chunk) = if service_type.is_keyed() {
                (
                    TargetType::Keyed {
                        key: urlencoding::decode(second_chunk)
                            .map_err(HandlerError::UrlDecodingError)?
                            .into_owned(),
                    },
                    third_chunk,
                    path_parts.next().ok_or(HandlerError::BadInvocationPath)?,
                )
            } else {
                (TargetType::Unkeyed, second_chunk, third_chunk)
            };

            let handler = next_chunk.to_owned();
            let idempotency_id = next_next_chunk.to_owned();

            (
                InvocationTargetType::IdempotencyId {
                    name: service_name,
                    target,
                    handler,
                    idempotency_id,
                },
                path_parts.next().ok_or(HandlerError::BadInvocationPath)?,
            )
        } else {
            (
                InvocationTargetType::InvocationId(first_chunk.to_owned()),
                second_chunk,
            )
        };

        // Output or attach
        match last_chunk {
            "output" => Ok(InvocationRequestType::GetOutput(invocation_target)),
            "attach" => Ok(InvocationRequestType::Attach(invocation_target)),
            _ => Err(HandlerError::NotFound),
        }
    }
}

pub(crate) enum AwakeableRequestType {
    Resolve { awakeable_id: String },
    Reject { awakeable_id: String },
}

impl AwakeableRequestType {
    fn from_path_chunks<'a>(
        mut path_parts: impl Iterator<Item = &'a str>,
    ) -> Result<Self, HandlerError> {
        // Parse awakeables id
        let awakeable_id = path_parts
            .next()
            .ok_or(HandlerError::BadAwakeablesPath)?
            .to_string();

        // Resolve or reject
        match path_parts.next().ok_or(HandlerError::BadAwakeablesPath)? {
            "resolve" => Ok(AwakeableRequestType::Resolve { awakeable_id }),
            "reject" => Ok(AwakeableRequestType::Reject { awakeable_id }),
            _ => Err(HandlerError::NotFound),
        }
    }
}

pub(crate) enum TargetType {
    Unkeyed,
    Keyed { key: String },
}

pub(crate) enum InvokeType {
    Call,
    Send,
}

pub(crate) struct ServiceRequestType {
    pub(crate) name: ServiceName,
    pub(crate) handler: String,
    pub(crate) target: TargetType,
    pub(crate) invoke_ty: InvokeType,
    /// Scope from the `/restate/scope/{scopeKey}/...` path prefix.
    pub(crate) scope: Option<ReString>,
}

impl ServiceRequestType {
    /// Extracts the service request type from the unversioned ingress API:
    ///
    /// - `/{vo}/{key}/{handler}/send`
    /// - `/{service}/{handler}/send`
    fn from_path_chunks<'a, Schemas>(
        mut path_parts: impl Iterator<Item = &'a str>,
        service_name: &str,
        schemas: &Schemas,
    ) -> Result<Self, HandlerError>
    where
        Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    {
        // We need to query the service type before continuing to parse
        let service_type = schemas
            .resolve_latest_service_type(service_name)
            .ok_or_else(|| HandlerError::ServiceNotFound(service_name.to_owned()))?;

        let target_type = if service_type.is_keyed() {
            TargetType::Keyed {
                key: urlencoding::decode(path_parts.next().ok_or(HandlerError::BadServicePath)?)
                    .map_err(HandlerError::UrlDecodingError)?
                    .into_owned(),
            }
        } else {
            TargetType::Unkeyed
        };

        let handler = path_parts
            .next()
            .ok_or(HandlerError::BadServicePath)?
            .to_owned();

        let last_segment = path_parts.next();

        let invoke_ty = match last_segment {
            None => InvokeType::Call,
            Some("send") => InvokeType::Send,
            Some(_) => return Err(HandlerError::BadServicePath),
        };

        if path_parts.next().is_some() {
            return Err(HandlerError::BadServicePath);
        }

        Ok(Self {
            name: ServiceName::new(service_name),
            handler,
            target: target_type,
            invoke_ty,
            scope: None,
        })
    }
}

/// Parse the new ingress API verbs under `/restate/...`. The verb has already been
/// consumed by `parse_path`. Supported shapes:
///   - `call/{service}/{handler}` or `call/{service}/{key}/{handler}`
///   - `send/{service}/{handler}` or `send/{service}/{key}/{handler}`
///   - `scope/{scopeKey}/call/{service}/{handler}`
///   - `scope/{scopeKey}/send/{service}/{key}/{handler}`
///   - `attach/{invocation_id}` or `output/{invocation_id}`
///   - `attach` or `output` (POST with body describing the target)
///   - `lookup`
fn parse_restate_api_verb<'a, Schemas>(
    verb: &str,
    mut path_parts: impl Iterator<Item = &'a str>,
    schemas: &Schemas,
) -> Result<RequestType, HandlerError>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    match verb {
        "call" | "send" => parse_call_or_send(verb, None, path_parts, schemas),
        "scope" => {
            let scope_key =
                urlencoding::decode(path_parts.next().ok_or(HandlerError::BadRestateApiPath)?)
                    .map_err(HandlerError::UrlDecodingError)?
                    .to_restring();
            let inner_verb = path_parts.next().ok_or(HandlerError::BadRestateApiPath)?;
            parse_call_or_send(inner_verb, Some(scope_key), path_parts, schemas)
        }
        "attach" | "output" => match path_parts.next() {
            None => Ok(if verb == "attach" {
                RequestType::AttachByTarget
            } else {
                RequestType::OutputByTarget
            }),
            Some(id_str) => {
                if path_parts.next().is_some() {
                    return Err(HandlerError::BadRestateApiPath);
                }
                let invocation_id = id_str
                    .parse::<InvocationId>()
                    .map_err(|e| HandlerError::BadInvocationId(id_str.to_owned(), e))?;
                Ok(if verb == "attach" {
                    RequestType::Attach(invocation_id)
                } else {
                    RequestType::Output(invocation_id)
                })
            }
        },
        "lookup" => {
            if path_parts.next().is_some() {
                return Err(HandlerError::BadRestateApiPath);
            }
            Ok(RequestType::Lookup)
        }
        _ => Err(HandlerError::NotFound),
    }
}

/// Dispatch a service-invocation verb (`call` or `send`) with an optional scope.
fn parse_call_or_send<'a, Schemas>(
    verb: &str,
    scope: Option<ReString>,
    mut path_parts: impl Iterator<Item = &'a str>,
    schemas: &Schemas,
) -> Result<RequestType, HandlerError>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    let invoke_ty = match verb {
        "call" => InvokeType::Call,
        "send" => InvokeType::Send,
        _ => return Err(HandlerError::BadRestateApiPath),
    };

    let service_name = path_parts.next().ok_or(HandlerError::BadRestateApiPath)?;

    let service_type = schemas
        .resolve_latest_service_type(service_name)
        .ok_or_else(|| HandlerError::ServiceNotFound(service_name.to_owned()))?;

    let target = if service_type.is_keyed() {
        TargetType::Keyed {
            key: urlencoding::decode(path_parts.next().ok_or(HandlerError::BadRestateApiPath)?)
                .map_err(HandlerError::UrlDecodingError)?
                .into_owned(),
        }
    } else {
        TargetType::Unkeyed
    };

    let handler = path_parts
        .next()
        .ok_or(HandlerError::BadRestateApiPath)?
        .to_owned();

    if path_parts.next().is_some() {
        return Err(HandlerError::BadRestateApiPath);
    }

    Ok(RequestType::Service(ServiceRequestType {
        name: ServiceName::new(service_name),
        handler,
        target,
        invoke_ty,
        scope,
    }))
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    /// This function takes care of parsing the path of the request, inferring the correct request type
    pub(super) fn parse_path(&mut self, uri: &Uri) -> Result<RequestType, HandlerError> {
        let num_segments = uri.path().bytes().filter(|&b| b == b'/').count();

        // The minimum number of segments are 2 when calling /service/handler through the old ingress API.
        // The maximum number of segments are 7 when calling /restate/scope/my-scope/call/service/service-key/handler
        if !(2..=7).contains(&num_segments) {
            return Err(HandlerError::BadPath(uri.path().to_owned()));
        }

        let mut segments = uri.path().split('/').skip(1);
        let first_segment = segments.next().ok_or(HandlerError::NotFound)?;

        let schema = self.schemas.live_load();
        match first_segment {
            "restate" => {
                let verb = segments.next().ok_or(HandlerError::NotFound)?;
                match verb {
                    "health" => Ok(RequestType::Health),
                    "awakeables" | "a" => Ok(RequestType::Awakeable(
                        AwakeableRequestType::from_path_chunks(segments)?,
                    )),
                    "invocation" => Ok(RequestType::Invocation(
                        InvocationRequestType::from_path_chunks(segments, schema)?,
                    )),
                    "workflow" => Ok(RequestType::Workflow(
                        WorkflowRequestType::from_path_chunks(segments)?,
                    )),
                    _ => parse_restate_api_verb(verb, segments, schema),
                }
            }
            "openapi" => Ok(RequestType::OpenAPI),
            segment => {
                // Old unversioned ingress API: /service/handler or /service/key/handler
                Ok(RequestType::Service(ServiceRequestType::from_path_chunks(
                    segments, segment, schema,
                )?))
            }
        }
    }
}
