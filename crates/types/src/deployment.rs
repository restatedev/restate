// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;

use http::{HeaderName, HeaderValue, Uri};

use restate_ty::LambdaARN;

use crate::identifiers::DeploymentId;
use crate::service_protocol::ServiceProtocolVersion;

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct HttpDeploymentAddress {
    pub uri: Uri,
}

impl HttpDeploymentAddress {
    pub fn new(uri: Uri) -> Self {
        Self { uri }
    }
}

impl fmt::Display for HttpDeploymentAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.uri, f)
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct LambdaDeploymentAddress {
    pub arn: LambdaARN,
    pub assume_role_arn: Option<String>,
}

impl LambdaDeploymentAddress {
    pub fn new(arn: LambdaARN, assume_role_arn: Option<String>) -> Self {
        Self {
            arn,
            assume_role_arn,
        }
    }
}

impl fmt::Display for LambdaDeploymentAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.arn, f)
    }
}

/// This is the representation of a deployment address
#[derive(Debug, Clone, PartialEq, derive_more::From)]
pub enum DeploymentAddress {
    Http(HttpDeploymentAddress),
    Lambda(LambdaDeploymentAddress),
}

impl fmt::Display for DeploymentAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DeploymentAddress::Http(d) => fmt::Display::fmt(d, f),
            DeploymentAddress::Lambda(d) => fmt::Display::fmt(d, f),
        }
    }
}

pub type Headers = HashMap<HeaderName, HeaderValue>;

pub type Metadata = HashMap<String, String>;

pub mod metadata {
    use std::fmt;

    #[non_exhaustive]
    #[derive(Debug, Clone, PartialEq, derive_more::From)]
    pub struct MetadataKey {
        key: &'static str,
        display: &'static str,
    }

    impl fmt::Display for MetadataKey {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", self.display)
        }
    }

    macro_rules! define_metadata {
        ($($const_name:ident($key:literal): $display:literal),* $(,)?) => {
            $(
                pub const $const_name: MetadataKey = MetadataKey {
                    key: $key,
                    display: $display,
                };
            )*

            impl<'a> TryFrom<&'a str> for MetadataKey {
                type Error = &'a str;

                fn try_from(value: &'a str) -> Result<Self, Self::Error> {
                    match value {
                        $(
                            $key => Ok($const_name),
                        )*
                        v => Err(v)
                    }
                }
            }
        };
    }

    define_metadata!(
        GIT_COMMIT("git.commit.sha"): "Git commit SHA",
        GITHUB_REPOSITORY("github.repository"): "Github Repository",
        GITHUB_ACTIONS_RUN_ID("github.actions.run.id"): "Github Actions Run id",
    );
}

/// Deployment which was chosen to run an invocation on.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PinnedDeployment {
    pub deployment_id: DeploymentId,
    pub service_protocol_version: ServiceProtocolVersion,
}

impl PinnedDeployment {
    pub fn new(
        deployment_id: DeploymentId,
        service_protocol_version: ServiceProtocolVersion,
    ) -> Self {
        Self {
            deployment_id,
            service_protocol_version,
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
mod mocks {
    use super::*;

    impl DeploymentAddress {
        pub fn mock() -> Self {
            HttpDeploymentAddress::new("http://localhost:9080".parse().unwrap()).into()
        }

        pub fn mock_uri(uri: &str) -> Self {
            HttpDeploymentAddress::new(uri.parse().unwrap()).into()
        }
    }
}
