// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use restate_types::schema::invocation_target::InvocationTargetResolver;

pub(crate) enum WorkflowRequestType {
    Attach(String, String),
    GetOutput(String, String),
}

impl WorkflowRequestType {
    fn from_path_chunks<'a>(
        mut path_parts: impl Iterator<Item = &'a str>,
    ) -> Result<Self, HandlerError> {
        // Parse invocation id
        let workflow_name = path_parts
            .next()
            .ok_or(HandlerError::BadWorkflowPath)?
            .to_owned();
        let workflow_key =
            urlencoding::decode(path_parts.next().ok_or(HandlerError::BadWorkflowPath)?)
                .map_err(HandlerError::UrlDecodingError)?
                .into_owned();

        // Resolve or reject
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
    pub(crate) name: String,
    pub(crate) handler: String,
    pub(crate) target: TargetType,
    pub(crate) invoke_ty: InvokeType,
}

impl ServiceRequestType {
    fn from_path_chunks<'a, Schemas>(
        mut path_parts: impl Iterator<Item = &'a str>,
        service_name: String,
        schemas: &Schemas,
    ) -> Result<Self, HandlerError>
    where
        Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    {
        // We need to query the service type before continuing to parse
        let service_type = schemas
            .resolve_latest_service_type(&service_name)
            .ok_or_else(|| HandlerError::ServiceNotFound(service_name.clone()))?;

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
            name: service_name,
            handler,
            target: target_type,
            invoke_ty,
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

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    /// This function takes care of parsing the path of the request, inferring the correct request type
    pub(crate) fn parse_path(&mut self, uri: &Uri) -> Result<RequestType, HandlerError> {
        let mut path_parts = uri.path().split('/').skip(1);

        let first_segment = path_parts.next().ok_or(HandlerError::NotFound)?;

        let schema = self.schemas.live_load();
        match first_segment {
            "restate" => match path_parts.next().ok_or(HandlerError::NotFound)? {
                "health" => Ok(RequestType::Health),
                "awakeables" | "a" => Ok(RequestType::Awakeable(
                    AwakeableRequestType::from_path_chunks(path_parts)?,
                )),
                "invocation" => Ok(RequestType::Invocation(
                    InvocationRequestType::from_path_chunks(path_parts, schema)?,
                )),
                "workflow" => Ok(RequestType::Workflow(
                    WorkflowRequestType::from_path_chunks(path_parts)?,
                )),
                _ => Err(HandlerError::NotFound),
            },
            "openapi" => Ok(RequestType::OpenAPI),
            segment => Ok(RequestType::Service(ServiceRequestType::from_path_chunks(
                path_parts,
                segment.to_owned(),
                schema,
            )?)),
        }
    }
}
