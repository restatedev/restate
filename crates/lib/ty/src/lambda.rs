// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, serde_with::SerializeDisplay, serde_with::DeserializeFromStr)]
pub struct LambdaARN {
    arn: Arc<str>,
    region: std::ops::Range<u32>,
}

impl LambdaARN {
    pub fn region(&self) -> &str {
        &self.arn[(self.region.start as usize)..(self.region.end as usize)]
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for LambdaARN {
    fn schema_name() -> String {
        "LambdaARN".into()
    }

    fn json_schema(_: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::String.into()),
            format: Some("arn".to_string()),
            ..Default::default()
        }
        .into()
    }
}

impl Display for LambdaARN {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.arn.fmt(f)
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum InvalidLambdaARN {
    #[error("A qualified ARN must have 8 components delimited by `:`")]
    InvalidFormat,
    #[error(
        "A qualified ARN needs a version or alias suffix. If you want to use the unpublished version, provide $LATEST and make sure your shell doesn't treat it as a variable"
    )]
    MissingVersionSuffix,
    #[error("First component of the ARN must be `arn`")]
    InvalidPrefix,
    #[error("ARN must refer to a `function` resource")]
    InvalidResourceType,
    #[error(
        "Partition, service, region, account ID, function name and version must all be non-empty"
    )]
    InvalidComponent,
    #[error("ARN must be for the lambda service")]
    InvalidService,
    #[error("Could not create valid URI for this ARN; likely malformed")]
    InvalidURI,
}

impl FromStr for LambdaARN {
    type Err = InvalidLambdaARN;

    fn from_str(arn: &str) -> Result<Self, Self::Err> {
        let mut split = arn.splitn(8, ':');
        let invalid_format = || InvalidLambdaARN::InvalidFormat;
        let prefix = split.next().ok_or_else(invalid_format)?;
        let partition = split.next().ok_or_else(invalid_format)?;
        let service = split.next().ok_or_else(invalid_format)?;
        let region = split.next().ok_or_else(invalid_format)?;
        let account_id = split.next().ok_or_else(invalid_format)?;
        let resource_type = split.next().ok_or_else(invalid_format)?;
        let name = split.next().ok_or_else(invalid_format)?;
        let version = split.next().ok_or(InvalidLambdaARN::MissingVersionSuffix)?;

        if prefix != "arn" {
            return Err(InvalidLambdaARN::InvalidPrefix);
        }
        if resource_type != "function" {
            return Err(InvalidLambdaARN::InvalidResourceType);
        }
        if service != "lambda" {
            return Err(InvalidLambdaARN::InvalidService);
        }
        if partition.is_empty() || region.is_empty() || account_id.is_empty() || name.is_empty() {
            return Err(InvalidLambdaARN::InvalidComponent);
        }

        if version.is_empty() {
            // special case this common mistake
            return Err(InvalidLambdaARN::MissingVersionSuffix);
        }

        // arn:<partition>:lambda:<region>:
        //                        ^       ^
        let region_start = 3 + 1 + (partition.len() as u32) + 1 + 6 + 1;
        let region_end = region_start + (region.len() as u32);
        let lambda = Self {
            arn: Arc::<str>::from(arn),
            region: region_start..region_end,
        };

        Ok(lambda)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_lambda_arn() {
        let good = "arn:aws:lambda:eu-central-1:1234567890:function:e2e-node-services:version";

        let expected = LambdaARN::from_str(good).unwrap();
        let parsed = expected.to_string();

        assert_eq!(good, parsed);
        assert_eq!("eu-central-1", expected.region());
    }

    #[test]
    fn missing_version_lambda_arn() {
        for bad in [
            "arn:aws:lambda:eu-central-1:1234567890:function:e2e-node-services",
            "arn:aws:lambda:eu-central-1:1234567890:function:e2e-node-services:",
        ] {
            assert_eq!(
                LambdaARN::from_str(bad).unwrap_err(),
                InvalidLambdaARN::MissingVersionSuffix
            );
        }
    }
}
