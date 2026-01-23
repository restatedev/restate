// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use aws_config::default_provider::region::DefaultRegionChain;
use aws_config::{BehaviorVersion, Region};
use aws_smithy_runtime_api::client::identity::ResolveCachedIdentity;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder;
use futures::FutureExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{BackoffConfig, ObjectStore, ObjectStoreScheme, RetryConfig};
use tracing::debug;
use url::Url;

use restate_types::retries::RetryPolicy;

pub async fn create_object_store_client(
    url: Url,
    options: &restate_types::config::ObjectStoreOptions,
    retry_policy: &RetryPolicy,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    // We do not use the top-level convenience method object_store::parse_url() as the
    // store-specific builders' from_env() provide a more ergonomic setup mechanism that allows us
    // to avoid having too many provider-specific config keys, while still supporting them when it
    // makes sense to do so.
    let object_store: Arc<dyn ObjectStore> = match ObjectStoreScheme::parse(&url)?.0 {
        ObjectStoreScheme::AmazonS3 => {
            // We use the AWS SDK configuration and credentials provider so that the conventional
            // AWS environment variables and config files work as expected. The object_store crate
            // has its own configuration mechanism which doesn't support many of the AWS
            // conventions. This differs quite a lot from the Lambda invoker which uses the AWS SDK,
            // and that would be a very surprising inconsistency for customers. This mechanism
            // allows us to infer the region and securely obtain session credentials without any
            // hard-coded configuration.
            let builder = match &options.aws_profile {
                Some(profile) => {
                    debug!(profile, "Using AWS profile for object store access");

                    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
                        .profile_name(profile)
                        .load()
                        .await;

                    let region = options
                        .aws_region
                        .clone()
                        .map(Region::new)
                        .or_else(|| sdk_config.region().cloned())
                        .context("Unable to determine AWS region")?;

                    debug!(?region, ?profile, "Using AWS SDK credentials provider");

                    let builder = AmazonS3Builder::new()
                        .with_region(region.to_string())
                        .with_credentials(Arc::new(AwsSdkCredentialsProvider::new(&sdk_config)?));

                    if let Some(endpoint_url) = sdk_config.endpoint_url() {
                        debug!(endpoint_url, "Using custom AWS endpoint override");
                        // we'll override this with the explicit endpoint URL from Restate config, if any, later on
                        builder.with_endpoint(endpoint_url)
                    } else {
                        builder
                    }
                }

                None => {
                    if options.aws_access_key_id.is_none() {
                        let sdk_config =
                            aws_config::defaults(BehaviorVersion::latest()).load().await;

                        let region = options
                            .aws_region
                            .clone()
                            .map(Region::new)
                            .or_else(|| sdk_config.region().cloned())
                            .context("Unable to determine AWS region")?;

                        debug!(?region, "Using AWS SDK credentials provider");

                        AmazonS3Builder::new()
                            .with_region(region.to_string())
                            .with_credentials(Arc::new(AwsSdkCredentialsProvider::new(
                                &sdk_config,
                            )?))
                    } else {
                        let region = if let Some(region) = options.aws_region.as_ref() {
                            Region::new(region.clone())
                        } else {
                            DefaultRegionChain::builder()
                                .build()
                                .region()
                                .await
                                .context("Unable to determine AWS region")?
                        };

                        AmazonS3Builder::new().with_region(region.to_string())
                    }
                }
            };

            let builder = if let Some(region) = &options.aws_region {
                builder.with_region(region)
            } else {
                builder
            };
            let env_allow_http_fallback = std::env::var("AWS_ALLOW_HTTP")
                .ok()
                .map(|s| s.trim().eq_ignore_ascii_case("true"));
            let allow_insecure_http = options.aws_allow_http.or(env_allow_http_fallback);
            let builder = if let Some(allow_http) = allow_insecure_http {
                builder.with_allow_http(allow_http)
            } else {
                builder
            };
            let env_endpoint_url_fallback = std::env::var("AWS_ENDPOINT_URL_S3")
                .ok()
                .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok());
            let s3_endpoint = options
                .aws_endpoint_url
                .as_ref()
                .or(env_endpoint_url_fallback.as_ref());
            if !allow_insecure_http.unwrap_or_default()
                && s3_endpoint.is_some_and(|endpoint| {
                    Url::parse(endpoint).is_ok_and(|url| url.scheme().eq_ignore_ascii_case("http"))
                })
            {
                anyhow::bail!(
                    "Misconfiguration detected: an HTTP endpoint URL \"{}\" override is set for object \
                store destination \"{}\", but plain HTTP is not allowed. Please set 'aws-allow-http' \
                to `true` to enable this destination",
                    s3_endpoint.expect("is some"),
                    url
                );
            }
            let builder = if let Some(endpoint_url) = s3_endpoint {
                builder.with_endpoint(endpoint_url)
            } else {
                builder
            };
            let builder = if let Some(access_key_id) = &options.aws_access_key_id {
                builder.with_access_key_id(access_key_id)
            } else {
                builder
            };
            let builder = if let Some(secret_access_key) = &options.aws_secret_access_key {
                builder.with_secret_access_key(secret_access_key)
            } else {
                builder
            };
            let builder = if let Some(token) = &options.aws_session_token {
                builder.with_token(token)
            } else {
                builder
            };

            let builder = builder
                .with_url(url)
                .with_conditional_put(S3ConditionalPut::ETagMatch)
                .with_retry(from_retry_policy(retry_policy));

            Arc::new(builder.build()?)
        }
        ObjectStoreScheme::MicrosoftAzure => {
            Arc::new(MicrosoftAzureBuilder::from_env().with_url(url).build()?)
        }
        ObjectStoreScheme::GoogleCloudStorage => Arc::new(
            GoogleCloudStorageBuilder::from_env()
                .with_url(url)
                .build()?,
        ),
        #[cfg(any(test, feature = "test-util"))]
        ObjectStoreScheme::Local => object_store::parse_url(&url)?.0.into(),
        _ => bail!("Unsupported protocol: {url}"),
    };

    Ok(object_store)
}

/// Convert Restate [RetryPolicy] into [object_store::RetryConfig].
fn from_retry_policy(retry_policy: &RetryPolicy) -> RetryConfig {
    match retry_policy {
        RetryPolicy::None => RetryConfig {
            max_retries: 0, // zero disables retries in object_store::RetryConfig
            ..Default::default()
        },
        RetryPolicy::FixedDelay {
            interval,
            max_attempts,
        } => RetryConfig {
            max_retries: max_attempts
                .unwrap_or(NonZeroUsize::new(usize::MAX).expect("non-zero"))
                .into(),
            backoff: BackoffConfig {
                init_backoff: *interval,
                max_backoff: *interval,
                base: 1.,
            },
            retry_timeout: Duration::MAX,
        },
        RetryPolicy::Exponential {
            initial_interval,
            factor,
            max_attempts,
            max_interval,
        } => RetryConfig {
            max_retries: max_attempts
                .unwrap_or(NonZeroUsize::new(usize::MAX).expect("non-zero"))
                .into(),
            backoff: BackoffConfig {
                init_backoff: *initial_interval,
                max_backoff: max_interval.unwrap_or(Duration::MAX),
                base: f64::from(*factor),
            },
            retry_timeout: Duration::MAX,
        },
    }
}

#[derive(Debug)]
struct AwsSdkCredentialsProvider {
    identity_cache: aws_smithy_runtime_api::client::identity::SharedIdentityCache,
    identity_resolver: aws_smithy_runtime_api::client::identity::SharedIdentityResolver,
    runtime_components: aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
    config_bag: aws_smithy_types::config_bag::ConfigBag,
}

impl AwsSdkCredentialsProvider {
    fn new(config: &aws_config::SdkConfig) -> anyhow::Result<Self> {
        let identity_cache = config
            .identity_cache()
            .context("Could not find AWS credentials provider")?;

        let credentials_provider = config
            .credentials_provider()
            .context("Could not find AWS credentials provider")?;

        let identity_resolver =
            aws_smithy_runtime_api::client::identity::SharedIdentityResolver::new(
                credentials_provider,
            );

        // runtime_components will be ignored by the credentials provider
        // but, we still need to create one, which is really not easy.
        // https://github.com/awslabs/aws-sdk-rust/discussions/923#discussioncomment-7471550
        let runtime_components = RuntimeComponentsBuilder::for_tests()
            .with_time_source(Some(aws_smithy_async::time::SystemTimeSource::new()))
            .with_sleep_impl(Some(aws_smithy_async::rt::sleep::TokioSleep::new()))
            .build()
            .context("Could not build AWS runtime components")?;

        Ok(Self {
            identity_cache,
            identity_resolver,
            runtime_components,
            config_bag: aws_smithy_types::config_bag::ConfigBag::base(),
        })
    }
}

impl object_store::CredentialProvider for AwsSdkCredentialsProvider {
    type Credential = object_store::aws::AwsCredential;

    fn get_credential<'a, 'async_trait>(
        &'a self,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = object_store::Result<Arc<Self::Credential>>>
                + Send
                + 'async_trait,
        >,
    >
    where
        Self: 'async_trait,
        'a: 'async_trait,
    {
        async {
            let identity = self
                .identity_cache
                .resolve_cached_identity(
                    self.identity_resolver.clone(),
                    &self.runtime_components,
                    &self.config_bag,
                )
                .await
                .map_err(|e| {
                    // object_store's error detail rendering is not great but aws_config logs the
                    // detailed underlying cause at WARN level so we don't need to do it again here
                    object_store::Error::Unauthenticated {
                        path: "<n/a>".to_string(),
                        source: e,
                    }
                })?;

            let creds = identity.data::<aws_credential_types::Credentials>().ok_or_else(
                || object_store::Error::Unauthenticated {
                    path: "<n/a>".to_string(),
                    source: anyhow::anyhow!("wrong identity type for SigV4. Expected AWS credentials but got `{identity:?}").into(),
                }
            )?;

            Ok(Arc::new(object_store::aws::AwsCredential {
                key_id: creds.access_key_id().to_string(),
                secret_key: creds.secret_access_key().to_string(),
                token: creds.session_token().map(|t| t.to_string()),
            }))
        }
        .boxed()
    }
}
