// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use restate_schema_api::service::ServiceMetadataResolver;
use restate_types::invocation::ServiceType;
use std::collections::VecDeque;

pub(crate) enum AwakeableRequestType {
    Resolve { awakeable_id: String },
    Reject { awakeable_id: String },
}

impl AwakeableRequestType {
    fn from_path_chunks(mut path_parts: VecDeque<&str>) -> Result<Self, HandlerError> {
        // Parse awakeables id
        let awakeable_id = path_parts
            .pop_front()
            .ok_or(HandlerError::BadAwakeablesPath)?
            .to_string();

        // Resolve or reject
        match path_parts
            .pop_front()
            .ok_or(HandlerError::BadAwakeablesPath)?
        {
            "resolve" => Ok(AwakeableRequestType::Resolve { awakeable_id }),
            "reject" => Ok(AwakeableRequestType::Reject { awakeable_id }),
            _ => Err(HandlerError::NotFound),
        }
    }
}

pub(crate) enum TargetType {
    Service,
    VirtualObject { key: String },
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
    fn from_path_chunks<Schemas>(
        mut path_parts: VecDeque<&str>,
        service_name: String,
        schemas: &Schemas,
    ) -> Result<Self, HandlerError>
    where
        Schemas: ServiceMetadataResolver + Clone + Send + Sync + 'static,
    {
        // We need to query the service type before continuing to parse
        let ct = schemas
            .resolve_latest_service_type(&service_name)
            .ok_or(HandlerError::NotFound)?;

        let target_type = match ct {
            ServiceType::Service => TargetType::Service,
            ServiceType::VirtualObject => TargetType::VirtualObject {
                key: urlencoding::decode(
                    path_parts.pop_front().ok_or(HandlerError::BadServicePath)?,
                )
                .map_err(HandlerError::UrlDecodingError)?
                .into_owned(),
            },
        };

        let handler = path_parts
            .pop_front()
            .ok_or(HandlerError::BadServicePath)?
            .to_owned();

        let last_segment = path_parts.pop_front();

        let invoke_ty = match last_segment {
            None => InvokeType::Call,
            Some("send") => InvokeType::Send,
            Some(_) => return Err(HandlerError::BadServicePath),
        };

        if !path_parts.is_empty() {
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
    Service(ServiceRequestType),
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + Clone + Send + Sync + 'static,
{
    /// This function takes care of parsing the path of the request, inferring the correct request type
    pub(crate) fn parse_path(&self, uri: &Uri) -> Result<RequestType, HandlerError> {
        let mut path_parts: VecDeque<&str> = uri.path().split('/').skip(1).collect();

        let first_segment = path_parts.pop_front().ok_or(HandlerError::NotFound)?;

        match first_segment {
            "restate" => match path_parts.pop_front().ok_or(HandlerError::NotFound)? {
                "health" => Ok(RequestType::Health),
                "awakeables" | "a" => Ok(RequestType::Awakeable(
                    AwakeableRequestType::from_path_chunks(path_parts)?,
                )),
                _ => Err(HandlerError::NotFound),
            },
            "openapi" => Ok(RequestType::OpenAPI),
            segment => Ok(RequestType::Service(ServiceRequestType::from_path_chunks(
                path_parts,
                segment.to_owned(),
                &self.schemas,
            )?)),
        }
    }
}
