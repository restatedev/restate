// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::handler::error::HandlerError;
use crate::handler::path_parsing::AwakeableRequestType;
use crate::handler::Handler;
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use restate_schema_api::component::ComponentMetadataResolver;

impl<Schemas> Handler<Schemas>
where
    Schemas: ComponentMetadataResolver + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_awakeable<B: http_body::Body>(
        &self,
        _req: Request<B>,
        _awakeable_request_type: AwakeableRequestType,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        unimplemented!()
    }
}
