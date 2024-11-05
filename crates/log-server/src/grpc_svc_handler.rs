// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate service protocol, which is
// released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/proto/blob/main/LICENSE

use async_trait::async_trait;
use tonic::{Request, Response, Status};

use restate_types::logs::RecordCache;

use crate::logstore::LogStore;
use crate::protobuf::log_server_svc_server::LogServerSvc;
use crate::protobuf::{
    GetDigestRequest, GetDigestResponse, GetLogletInfoRequest, GetLogletInfoResponse,
};

pub struct LogServerSvcHandler<S> {
    _log_store: S,
    _record_cache: RecordCache,
}

impl<S> LogServerSvcHandler<S>
where
    S: LogStore + Clone + Sync + Send + 'static,
{
    pub fn new(_log_store: S, _record_cache: RecordCache) -> Self {
        Self {
            _log_store,
            _record_cache,
        }
    }
}

#[async_trait]
impl<S> LogServerSvc for LogServerSvcHandler<S>
where
    S: LogStore + Clone + Sync + Send + 'static,
{
    async fn get_digest(
        &self,
        _request: Request<GetDigestRequest>,
    ) -> Result<Response<GetDigestResponse>, Status> {
        todo!()
    }

    async fn get_loglet_info(
        &self,
        _request: Request<GetLogletInfoRequest>,
    ) -> Result<Response<GetLogletInfoResponse>, Status> {
        todo!()
    }
}
