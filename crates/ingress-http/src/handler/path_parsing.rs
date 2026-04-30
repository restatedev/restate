// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Handler;
use super::HandlerError;
use http::Uri;
use restate_types::ServiceName;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_util_string::{ReString, ToReString};

pub(crate) enum WorkflowRequestType {
    /// (name, key, scope)
    Attach(ReString, ReString, Option<ReString>),
    /// (name, key, scope)
    GetOutput(ReString, ReString, Option<ReString>),
}

impl WorkflowRequestType {
    /// Parse workflow request from unversioned path: `/restate/workflow/{name}/{key}/attach|output`
    fn from_path_chunks<'a>(
        mut path_parts: impl Iterator<Item = &'a str>,
    ) -> Result<Self, HandlerError> {
        let workflow_name =
            ReString::new_owned(path_parts.next().ok_or(HandlerError::BadWorkflowPath)?);
        let workflow_key = ReString::new_owned(
            urlencoding::decode(path_parts.next().ok_or(HandlerError::BadWorkflowPath)?)
                .map_err(HandlerError::UrlDecodingError)?,
        );

        match path_parts.next().ok_or(HandlerError::BadWorkflowPath)? {
            "output" => Ok(WorkflowRequestType::GetOutput(
                workflow_name,
                workflow_key,
                None,
            )),
            "attach" => Ok(WorkflowRequestType::Attach(
                workflow_name,
                workflow_key,
                None,
            )),
            _ => Err(HandlerError::NotFound),
        }
    }

    /// Parse workflow request from versioned path segments (after `workflow` keyword).
    /// Expects: `{name}/{key}/attach|output`
    fn from_v1_path_chunks<'a>(
        mut path_parts: impl Iterator<Item = &'a str>,
        scope: Option<ReString>,
    ) -> Result<Self, HandlerError> {
        let workflow_name =
            ReString::new_owned(path_parts.next().ok_or(HandlerError::BadApiV1Path)?);
        let workflow_key = ReString::new_owned(
            urlencoding::decode(path_parts.next().ok_or(HandlerError::BadApiV1Path)?)
                .map_err(HandlerError::UrlDecodingError)?,
        );

        let action = path_parts.next().ok_or(HandlerError::BadApiV1Path)?;

        if path_parts.next().is_some() {
            return Err(HandlerError::BadApiV1Path);
        }

        match action {
            "output" => Ok(WorkflowRequestType::GetOutput(
                workflow_name,
                workflow_key,
                scope,
            )),
            "attach" => Ok(WorkflowRequestType::Attach(
                workflow_name,
                workflow_key,
                scope,
            )),
            _ => Err(HandlerError::BadApiV1Path),
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
    /// Scope from the `/api/v1/scope/{scopeKey}/...` path prefix.
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

pub(crate) enum RequestType {
    Health,
    OpenAPI,
    Awakeable(AwakeableRequestType),
    Invocation(InvocationRequestType),
    Service(ServiceRequestType),
    Workflow(WorkflowRequestType),
}

/// Parse the remainder of an `/api/v1/...` path after the `api/v1` prefix has
/// been consumed. The iterator should yield segments starting from the verb or
/// scope keyword, e.g.:
///   - `call/{service}/{handler}` or `call/{service}/{key}/{handler}`
///   - `send/{service}/{handler}` or `send/{service}/{key}/{handler}`
///   - `scope/{scopeKey}/call/{service}/{handler}`
///   - `scope/{scopeKey}/send/{service}/{key}/{handler}`
fn parse_api_v1<'a, Schemas>(
    mut path_parts: impl Iterator<Item = &'a str>,
    schemas: &Schemas,
) -> Result<RequestType, HandlerError>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    let segment = path_parts.next().ok_or(HandlerError::NotFound)?;

    match segment {
        "call" | "send" | "workflow" => parse_api_v1_verb(segment, None, path_parts, schemas),
        "scope" => {
            let scope_key =
                urlencoding::decode(path_parts.next().ok_or(HandlerError::BadApiV1Path)?)
                    .map_err(HandlerError::UrlDecodingError)?
                    .to_restring();
            let verb = path_parts.next().ok_or(HandlerError::BadApiV1Path)?;
            parse_api_v1_verb(verb, Some(scope_key), path_parts, schemas)
        }
        _ => Err(HandlerError::BadApiV1Path),
    }
}

/// Dispatch a v1 API verb (`call`, `send`, or `workflow`) with an optional scope.
fn parse_api_v1_verb<'a, Schemas>(
    verb: &str,
    scope: Option<ReString>,
    mut path_parts: impl Iterator<Item = &'a str>,
    schemas: &Schemas,
) -> Result<RequestType, HandlerError>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    // Workflow verb returns a WorkflowRequestType instead of ServiceRequestType
    if verb == "workflow" {
        return Ok(RequestType::Workflow(
            WorkflowRequestType::from_v1_path_chunks(path_parts, scope.map(ReString::new_owned))?,
        ));
    }

    let invoke_ty = match verb {
        "call" => InvokeType::Call,
        "send" => InvokeType::Send,
        _ => return Err(HandlerError::BadApiV1Path),
    };

    let service_name = path_parts.next().ok_or(HandlerError::BadApiV1Path)?;

    let service_type = schemas
        .resolve_latest_service_type(service_name)
        .ok_or_else(|| HandlerError::ServiceNotFound(service_name.to_owned()))?;

    let target = if service_type.is_keyed() {
        TargetType::Keyed {
            key: urlencoding::decode(path_parts.next().ok_or(HandlerError::BadApiV1Path)?)
                .map_err(HandlerError::UrlDecodingError)?
                .into_owned(),
        }
    } else {
        TargetType::Unkeyed
    };

    let handler = path_parts
        .next()
        .ok_or(HandlerError::BadApiV1Path)?
        .to_owned();

    if path_parts.next().is_some() {
        return Err(HandlerError::BadApiV1Path);
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
    pub(crate) fn parse_path(&mut self, uri: &Uri) -> Result<RequestType, HandlerError> {
        let num_segments = uri.path().bytes().filter(|&b| b == b'/').count();

        // The minimum number of segments are 2 when calling /service/handler through the old ingress API.
        // The maximum number of segments are 8 when calling /api/v1/scope/my-scope/call/service/service-key/handler
        if !(2..=8).contains(&num_segments) {
            return Err(HandlerError::BadPath(uri.path().to_owned()));
        }

        let mut segments = uri.path().split('/').skip(1);
        let first_segment = segments.next().ok_or(HandlerError::NotFound)?;

        let schema = self.schemas.live_load();
        match first_segment {
            "restate" => match segments.next().ok_or(HandlerError::NotFound)? {
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
                _ => Err(HandlerError::NotFound),
            },
            "openapi" => Ok(RequestType::OpenAPI),
            "api" if num_segments >= 5 => {
                // Old API has at most 4 segments, so >=5 is unambiguously new API.
                let version = segments.next().unwrap();

                if version == "v1" {
                    parse_api_v1(segments, schema)
                } else {
                    Err(HandlerError::BadPath(uri.path().to_owned()))
                }
            }
            segment => {
                // Old unversioned API (including "api" as a service name when <5 segments)
                Ok(RequestType::Service(ServiceRequestType::from_path_chunks(
                    segments, segment, schema,
                )?))
            }
        }
    }
}
