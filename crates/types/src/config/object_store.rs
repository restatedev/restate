// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[serde_as]
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "ObjectStoreOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct ObjectStoreOptions {
    /// The AWS configuration profile to use for S3 object store destinations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_profile: Option<String>,

    /// AWS region to use with S3 object store destinations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_region: Option<String>,

    /// AWS access key. We strongly recommend against using long-lived credentials; set up a
    /// configuration profile with a role-based session credentials provider instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_access_key_id: Option<String>,

    /// AWS secret key. We strongly recommend against using long-lived credentials; set up a
    /// configuration profile with a role-based session credentials provider instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_secret_access_key: Option<String>,

    /// AWS session token. We strongly recommend against using long-lived credentials; set up a
    /// configuration profile with a role-based session credentials provider instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_session_token: Option<String>,

    /// AWS endpoint URL for S3 object store destinations. Some S3-compatible stores may require
    /// you to use this.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_endpoint_url: Option<String>,

    /// Allow plain HTTP to be used with the object store endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_allow_http: Option<bool>,
}
