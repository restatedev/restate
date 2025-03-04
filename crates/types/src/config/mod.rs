// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod admin;
mod aws;
mod bifrost;
mod common;
mod http;
mod ingress;
mod kafka;
mod log_server;
mod metadata_server;
mod networking;
mod object_store;
mod query_engine;
mod rocksdb;
mod worker;

pub use admin::*;
pub use aws::*;
pub use bifrost::*;
pub use common::*;
pub use http::*;
pub use ingress::*;
pub use kafka::*;
pub use log_server::*;
pub use metadata_server::*;
pub use networking::*;
pub use object_store::*;
pub use query_engine::*;
pub use rocksdb::*;
pub use worker::*;

pub fn print_warning_deprecated_config_option(deprecated: &str, replacement: Option<&str>) {
    // we can't use tracing since config loading happens before tracing is initialized
    if let Some(replacement) = replacement {
        eprintln!(
            "Using the deprecated config option '{deprecated}' instead of '{replacement}'. Please update the config to use '{replacement}' instead."
        );
    } else {
        eprintln!("Using the deprecated config option '{deprecated}'.");
    }
}
