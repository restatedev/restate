// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
    /// # AWS profile
    ///
    /// The AWS configuration profile to use for S3 object store destinations. If you use
    /// named profiles in your AWS configuration, you can replace all the other settings with
    /// a single profile reference. See the [AWS documentation on profiles]
    /// (https://docs.aws.amazon.com/sdkref/latest/guide/file-format.html) for more.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_profile: Option<String>,

    /// # AWS region
    ///
    /// AWS region to use with S3 object store destinations. This may be inferred from the
    /// environment, for example the current region when running in EC2. Because of the
    /// request signing algorithm this must have a value. For Minio, you can generally
    /// set this to any string, such as `us-east-1`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_region: Option<String>,

    /// # AWS access key
    ///
    /// Username for Minio, or consult the service documentation for other S3-compatible stores.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_access_key_id: Option<String>,

    /// # AWS secret key
    ///
    /// Password for Minio, or consult the service documentation for other S3-compatible stores.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_secret_access_key: Option<String>,

    /// # AWS session token
    ///
    /// This is only needed with short-term STS session credentials.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_session_token: Option<String>,

    /// # Object store API endpoint URL override
    ///
    /// When you use Amazon S3, this is typically inferred from the region and there is no need to
    /// set it. With other object stores, you will have to provide an appropriate HTTP(S) endpoint.
    /// If *not* using HTTPS, also set `aws-allow-http` to `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_endpoint_url: Option<String>,

    /// # Allow insecure HTTP
    ///
    /// Allow plain HTTP to be used with the object store endpoint. Required when the endpoint URL
    /// that isn't using HTTPS.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_allow_http: Option<bool>,
}
