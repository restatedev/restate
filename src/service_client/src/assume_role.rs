// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Some parts copied from https://github.com/awslabs/aws-sdk-rust/blob/0.55.x/sdk/aws-config/src/sts/assume_role.rs
//! License Apache-2.0

use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::future::ProvideCredentials;
use aws_sdk_sts::operation::assume_role::AssumeRoleError;
use std::time::SystemTime;

/// AssumeRoleProvider implements ProvideCredentials by assuming a provided role
/// It is materially very similar to AssumeRoleProvider in the aws-config crate, except
/// it is able to reuse the same sts client across many providers
#[derive(Debug)]
pub(crate) struct AssumeRoleProvider {
    client: aws_sdk_sts::Client,
    role_arn: String,
    external_id: Option<String>,
}

impl AssumeRoleProvider {
    pub(crate) fn new(
        client: aws_sdk_sts::Client,
        role_arn: String,
        external_id: Option<String>,
    ) -> Self {
        Self {
            client,
            role_arn,
            external_id,
        }
    }
}

impl aws_credential_types::provider::ProvideCredentials for AssumeRoleProvider {
    fn provide_credentials<'a>(&'a self) -> ProvideCredentials<'a>
    where
        Self: 'a,
    {
        ProvideCredentials::new(async {
            let mut fluent_builder = self
                .client
                .assume_role()
                .role_session_name(format!(
                    "restate-{}",
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                ))
                .role_arn(self.role_arn.clone());

            if let Some(external_id) = &self.external_id {
                fluent_builder = fluent_builder.external_id(external_id.clone());
            }

            let assumed = fluent_builder.send().await;
            match assumed {
                Ok(assumed) => into_credentials(assumed.credentials, "AssumeRoleProvider"),
                Err(aws_smithy_http::result::SdkError::ServiceError(ref context))
                    if matches!(
                        context.err(),
                        AssumeRoleError::RegionDisabledException(_)
                            | AssumeRoleError::MalformedPolicyDocumentException(_)
                    ) =>
                {
                    Err(CredentialsError::invalid_configuration(
                        assumed.err().unwrap(),
                    ))
                }

                Err(aws_smithy_http::result::SdkError::ServiceError(_)) => {
                    Err(CredentialsError::provider_error(assumed.err().unwrap()))
                }
                Err(err) => Err(CredentialsError::provider_error(err)),
            }
        })
    }
}

fn into_credentials(
    sts_credentials: Option<aws_sdk_sts::types::Credentials>,
    provider_name: &'static str,
) -> aws_credential_types::provider::Result {
    let sts_credentials = sts_credentials
        .ok_or_else(|| CredentialsError::unhandled("STS credentials must be defined"))?;
    let expiration = std::time::SystemTime::try_from(
        sts_credentials
            .expiration
            .ok_or_else(|| CredentialsError::unhandled("missing expiration"))?,
    )
    .map_err(|_| {
        CredentialsError::unhandled(
            "credential expiration time cannot be represented by a SystemTime",
        )
    })?;
    Ok(aws_credential_types::Credentials::new(
        sts_credentials
            .access_key_id
            .ok_or_else(|| CredentialsError::unhandled("access key id missing from result"))?,
        sts_credentials
            .secret_access_key
            .ok_or_else(|| CredentialsError::unhandled("secret access token missing"))?,
        sts_credentials.session_token,
        Some(expiration),
        provider_name,
    ))
}
