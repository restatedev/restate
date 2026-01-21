// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_serde_util::ByteCount;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

/// # AWS options
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(rename = "AwsClientOptions", default))]
#[serde(rename_all = "kebab-case")]
#[builder(default)]
pub struct AwsLambdaOptions {
    /// # AWS Profile
    ///
    /// Name of the AWS profile to select. Defaults to 'AWS_PROFILE' env var, or otherwise
    /// the `default` profile.
    pub aws_profile: Option<String>,

    /// # AssumeRole external ID
    ///
    /// An external ID to apply to any AssumeRole operations taken by this client.
    /// https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
    /// Can be overridden by the `AWS_EXTERNAL_ID` environment variable.
    pub aws_assume_role_external_id: Option<String>,

    /// # Request Compression threshold
    ///
    /// Request minimum size to enable compression.
    /// The request size includes the total of the journal replay and its framing using Restate service protocol, without accounting for the json envelope and the base 64 encoding.
    ///
    /// Default: 4MB (The default AWS Lambda Limit is 6MB, 4MB roughly accounts for +33% of Base64 and the json envelope).
    pub request_compression_threshold: Option<ByteCount>,
}

impl Default for AwsLambdaOptions {
    fn default() -> Self {
        Self {
            aws_profile: None,
            aws_assume_role_external_id: None,
            request_compression_threshold: Some((4usize * 1024 * 1024).into()),
        }
    }
}
