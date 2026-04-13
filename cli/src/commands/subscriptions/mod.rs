// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod create;
mod delete;
mod describe;
mod list;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "sub", alias = "subscription")]
pub enum Subscriptions {
    /// List the registered subscriptions
    List(list::List),
    /// Register a new subscription
    #[clap(alias = "register")]
    Create(create::Create),
    /// Print detailed information about a subscription
    Describe(describe::Describe),
    /// Remove a subscription
    Delete(delete::Delete),
}

/// Parses `kafka://<cluster>/<topic>` and returns the cluster name. Returns
/// `None` for any other scheme. Used to opportunistically resolve the
/// referenced cluster's broker addresses on `describe`.
pub(crate) fn kafka_cluster_from_source(source: &str) -> Option<String> {
    let uri: http::Uri = source.parse().ok()?;
    if uri.scheme_str() != Some("kafka") {
        return None;
    }
    uri.host().map(|s| s.to_string())
}
