// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use restate_types::logs::Payload;
use restate_types::DEFAULT_STORAGE_DIRECTORY;
use serde_json::json;
use std::path::Path;

use crate::loglet::{Loglet, LogletBase, LogletOffset, LogletProvider};
use crate::metadata::LogletParams;
use crate::{Error, LogRecord, Options};

pub fn default_config() -> serde_json::Value {
    json!( {"path": Path::new(DEFAULT_STORAGE_DIRECTORY).join("logs").into_os_string()})
}

pub struct FileLogletProvider {}

impl FileLogletProvider {
    pub fn new(_options: &Options) -> Arc<Self> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl LogletProvider for FileLogletProvider {
    async fn get_loglet(
        &self,
        _config: &LogletParams,
    ) -> Result<std::sync::Arc<dyn Loglet<Offset = LogletOffset>>, Error> {
        todo!()
    }
}

pub struct FileLoglet {
    _params: LogletParams,
}

#[async_trait]
impl LogletBase for FileLoglet {
    type Offset = LogletOffset;
    async fn append(&self, _payload: Payload) -> Result<LogletOffset, Error> {
        todo!()
    }

    async fn find_tail(&self) -> Result<Option<LogletOffset>, Error> {
        todo!()
    }

    async fn get_trim_point(&self) -> Result<Self::Offset, Error> {
        todo!()
    }

    async fn read_next_single(
        &self,
        _after: Self::Offset,
    ) -> Result<LogRecord<Self::Offset>, Error> {
        todo!()
    }

    async fn read_next_single_opt(
        &self,
        _after: Self::Offset,
    ) -> Result<Option<LogRecord<Self::Offset>>, Error> {
        todo!()
    }
}
