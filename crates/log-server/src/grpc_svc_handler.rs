// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use tonic::{Request, Response, Status};

use restate_types::logs::{LogletOffset, RecordCache, SequenceNumber};
use restate_types::net::log_server::{GetDigest, LogServerResponseHeader, LogletInfo};
use restate_types::replicated_loglet::ReplicatedLogletId;

use crate::logstore::LogStore;
use crate::metadata::LogletStateMap;
use crate::protobuf::log_server_svc_server::LogServerSvc;
use crate::protobuf::{
    GetDigestRequest, GetDigestResponse, GetLogletInfoRequest, GetLogletInfoResponse,
};

pub struct LogServerSvcHandler<S> {
    log_store: S,
    state_map: LogletStateMap,
    _record_cache: RecordCache,
}

impl<S> LogServerSvcHandler<S>
where
    S: LogStore + Clone + Sync + Send + 'static,
{
    pub fn new(log_store: S, state_map: LogletStateMap, _record_cache: RecordCache) -> Self {
        Self {
            log_store,
            state_map,
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
        request: Request<GetDigestRequest>,
    ) -> Result<Response<GetDigestResponse>, Status> {
        let request = request.into_inner();
        let loglet_id = ReplicatedLogletId::from(request.loglet_id);
        let state = self
            .state_map
            .get_or_load(loglet_id, &self.log_store)
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let msg = GetDigest {
            header: restate_types::net::log_server::LogServerRequestHeader {
                loglet_id: request.loglet_id.into(),
                known_global_tail: LogletOffset::INVALID,
            },
            from_offset: request.from_offset.into(),
            to_offset: request.to_offset.into(),
        };

        let digest = self
            .log_store
            .get_records_digest(msg, &state)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let response = GetDigestResponse {
            digest: Some(digest.into()),
        };
        Ok(Response::new(response))
    }

    async fn get_loglet_info(
        &self,
        request: Request<GetLogletInfoRequest>,
    ) -> Result<Response<GetLogletInfoResponse>, Status> {
        let request = request.into_inner();
        let loglet_id = ReplicatedLogletId::from(request.loglet_id);
        let state = self
            .state_map
            .get_or_load(loglet_id, &self.log_store)
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let header = LogServerResponseHeader::new(state.local_tail(), state.known_global_tail());
        let info = LogletInfo {
            header,
            trim_point: state.trim_point(),
        };

        let response = GetLogletInfoResponse {
            info: Some(info.into()),
        };
        Ok(Response::new(response))
    }
}
