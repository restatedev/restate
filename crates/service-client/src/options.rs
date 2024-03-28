// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use super::lambda::{
    Options as LambdaClientOptions, OptionsBuilder as LambdaClientOptionsBuilder,
    OptionsBuilderError as LambdaClientOptionsBuilderError,
};

use derive_getters::{Dissolve, Getters};
use serde_with::serde_as;

pub use super::http::{
    Options as HttpClientOptions, OptionsBuilder as HttpClientOptionsBuilder,
    OptionsBuilderError as HttpClientOptionsBuilderError,
};

/// # Client options
#[serde_as]
#[derive(
    Debug, Getters, Dissolve, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder,
)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "ServiceClientOptions", default)
)]
#[builder(default)]
#[derive(Default)]
pub struct Options {
    #[serde(flatten)]
    http: HttpClientOptions,
    #[serde(flatten)]
    lambda: LambdaClientOptions,
}
