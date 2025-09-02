// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod admin_client;
mod admin_interface;
#[cfg(feature = "cloud")]
pub mod cloud;
pub mod datafusion_helpers;
mod datafusion_http_client;
mod errors;

pub use self::admin_client::AdminClient;
pub use self::admin_client::Error as MetasClientError;
pub use self::admin_client::{MAX_ADMIN_API_VERSION, MIN_ADMIN_API_VERSION};
pub use self::admin_interface::AdminClientInterface;
pub use self::admin_interface::Deployment;
pub use self::datafusion_http_client::DataFusionHttpClient;

use futures::StreamExt;
use futures::stream::FuturesUnordered;

pub(crate) async fn collect_and_split_futures<Fut, Ok, Err>(
    iter: impl IntoIterator<Item = Fut>,
) -> (Vec<Ok>, Vec<Err>)
where
    Fut: Future<Output = Result<Ok, Err>> + Send,
    Ok: Send,
    Err: Send,
{
    iter.into_iter()
        .collect::<FuturesUnordered<_>>()
        .fold((vec![], vec![]), |(mut done, mut failed), result| async {
            match result {
                Ok(ok) => done.push(ok),
                Err(err) => failed.push(err),
            }
            (done, failed)
        })
        .await
}
