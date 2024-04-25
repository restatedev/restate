// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use bytestring::ByteString;
use itertools::Itertools;
use restate_types::invocation::{HandlerType, ServiceType};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

pub const DEFAULT_IDEMPOTENCY_RETENTION: Duration = Duration::from_secs(60 * 60 * 24);

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InvocationTargetMetadata {
    pub public: bool,
    pub idempotency_retention: Duration,
    pub service_ty: ServiceType,
    pub handler_ty: HandlerType,
    pub input_rules: InputRules,
    pub output_rules: OutputRules,
}

/// This API resolves invocation targets.
pub trait InvocationTargetResolver {
    /// Returns None if the service handler doesn't exist, Some(basic_service_metadata) otherwise.
    fn resolve_latest_invocation_target(
        &self,
        service_name: impl AsRef<str>,
        handler_name: impl AsRef<str>,
    ) -> Option<InvocationTargetMetadata>;
}

// --- Input rules

#[derive(Debug, thiserror::Error)]
pub enum InputValidationError {
    #[error("Expected body and content-type to be empty, but wasn't with content-type '{0:?}'")]
    NonEmptyInput(Option<String>),
    #[error("Empty content-type")]
    EmptyContentType,
    #[error("Empty body not allowed")]
    EmptyValue,
    #[error("Bad configuration of the input validation rules")]
    BadConfiguration,
    #[error("Content-type '{0}' does not match '{1}'")]
    ContentTypeNotMatching(String, InputContentType),
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InputRules {
    /// Input validation will try each of these rules. Validation passes if at least one rule matches.
    pub input_validation_rules: Vec<InputValidationRule>,
}

impl InputRules {
    pub fn validate(
        &self,
        content_type: Option<&str>,
        buf: &Bytes,
    ) -> Result<(), InputValidationError> {
        let mut res = Err(InputValidationError::BadConfiguration);

        for rule in &self.input_validation_rules {
            res = rule.validate(content_type, buf);
            if res.is_ok() {
                return Ok(());
            }
        }

        res
    }
}

impl Default for InputRules {
    fn default() -> Self {
        Self {
            input_validation_rules: vec![
                InputValidationRule::NoBodyAndContentType,
                InputValidationRule::ContentType {
                    content_type: InputContentType::Any,
                },
            ],
        }
    }
}

impl fmt::Display for InputRules {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.input_validation_rules.len() {
            0 => write!(f, "<invalid rules>"),
            1 => write!(f, "{}", self.input_validation_rules[0]),
            _ => write!(
                f,
                "one of [{}]",
                self.input_validation_rules
                    .iter()
                    .map(|s| format!("\"{s}\""))
                    .join(", ")
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum InputValidationRule {
    // Input and content-type must be empty
    NoBodyAndContentType,

    // Validates only the content-type, not the content
    ContentType {
        // Can use wildcards
        content_type: InputContentType,
    },

    // Validates the input as json value
    JsonValue {
        // Can use wildcards
        content_type: InputContentType,
        // TODO add json schema.
    },
}

impl fmt::Display for InputValidationRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputValidationRule::NoBodyAndContentType => write!(f, "none"),
            InputValidationRule::ContentType { content_type } => {
                write!(f, "value of content-type '{}'", content_type)
            }
            InputValidationRule::JsonValue { content_type } => {
                write!(f, "JSON value of content-type '{}'", content_type)
            }
        }
    }
}

impl InputValidationRule {
    fn validate(
        &self,
        input_content_type: Option<&str>,
        buf: &Bytes,
    ) -> Result<(), InputValidationError> {
        match self {
            InputValidationRule::NoBodyAndContentType => {
                if !(buf.is_empty() && input_content_type.is_none()) {
                    return Err(InputValidationError::NonEmptyInput(
                        input_content_type.map(str::to_owned),
                    ));
                }
            }
            InputValidationRule::ContentType { content_type } => {
                if input_content_type.is_none() {
                    return Err(InputValidationError::EmptyContentType);
                }
                content_type.validate(input_content_type.unwrap())?;
            }
            InputValidationRule::JsonValue { content_type } => {
                if input_content_type.is_none() {
                    return Err(InputValidationError::EmptyContentType);
                }
                content_type.validate(input_content_type.unwrap())?;

                if buf.is_empty() {
                    // In JSON empty values are not allowed
                    return Err(InputValidationError::EmptyValue);
                }

                // TODO add additional json validation.
            }
        }
        Ok(())
    }
}

/// Describes a content type in the same format of the [`Accept` header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum InputContentType {
    /// `*/*`
    #[default]
    Any,
    /// `<MIME_type>/*`
    MimeType(ByteString),
    /// `<MIME_type>/<MIME_subtype>`
    MimeTypeAndSubtype(ByteString, ByteString),
}

impl InputContentType {
    fn validate(&self, input_content_type: &str) -> Result<(), InputValidationError> {
        match self {
            InputContentType::Any => Ok(()),
            InputContentType::MimeType(ty) => {
                let (first_part, _) = self.extract_content_type_parts(input_content_type)?;
                if ty != first_part {
                    return Err(InputValidationError::ContentTypeNotMatching(
                        input_content_type.to_owned(),
                        self.clone(),
                    ));
                }
                Ok(())
            }
            InputContentType::MimeTypeAndSubtype(ty, sub_ty) => {
                let (first_part, second_part) =
                    self.extract_content_type_parts(input_content_type)?;
                if ty != first_part {
                    return Err(InputValidationError::ContentTypeNotMatching(
                        input_content_type.to_owned(),
                        self.clone(),
                    ));
                }
                if sub_ty != second_part {
                    return Err(InputValidationError::ContentTypeNotMatching(
                        input_content_type.to_owned(),
                        self.clone(),
                    ));
                }
                Ok(())
            }
        }
    }

    fn extract_content_type_parts<'a>(
        &'a self,
        input_content_type: &'a str,
    ) -> Result<(&str, &str), InputValidationError> {
        return match input_content_type.trim().split_once('/') {
            Some((first_part, second_part)) => Ok((first_part, second_part)),
            None => Err(InputValidationError::ContentTypeNotMatching(
                input_content_type.to_owned(),
                self.clone(),
            )),
        };
    }
}

impl fmt::Display for InputContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputContentType::Any => write!(f, "*/*"),
            InputContentType::MimeType(t) => write!(f, "{}/*", t),
            InputContentType::MimeTypeAndSubtype(t, st) => write!(f, "{}/{}", t, st),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("can't parse the content-type '{str}': {reason}")]
pub struct BadInputContentType {
    str: String,
    reason: &'static str,
}

impl FromStr for InputContentType {
    type Err = BadInputContentType;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().split_once('/') {
            None => Err(BadInputContentType {
                str: s.to_owned(),
                reason: "must contain one /",
            }),
            Some(("*", "*")) => Ok(InputContentType::Any),
            Some(("*", _)) => Err(BadInputContentType {
                str: s.to_owned(),
                reason: "the wildcard format */subType is not supported",
            }),
            Some((t, "*")) => Ok(InputContentType::MimeType(t.into())),
            Some((t, st)) => Ok(InputContentType::MimeTypeAndSubtype(t.into(), st.into())),
        }
    }
}

// --- Output rules

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OutputRules {
    pub content_type_rule: OutputContentTypeRule,
}

impl OutputRules {
    pub fn infer_content_type(&self, is_output_empty: bool) -> Option<http::HeaderValue> {
        match &self.content_type_rule {
            OutputContentTypeRule::None => None,
            OutputContentTypeRule::Set {
                content_type,
                set_content_type_if_empty,
                ..
            } => {
                if is_output_empty && !set_content_type_if_empty {
                    None
                } else {
                    Some(content_type.clone())
                }
            }
        }
    }
}

impl fmt::Display for OutputRules {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.content_type_rule)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OutputContentTypeRule {
    None,
    Set {
        #[cfg_attr(
            feature = "serde",
            serde(with = "serde_with::As::<restate_serde_util::HeaderValueSerde>")
        )]
        content_type: http::HeaderValue,
        // If true, sets the content-type even if the output is empty.
        // Otherwise, don't set the content-type.
        set_content_type_if_empty: bool,
        // If true, this should be a JSON Value.
        has_json_schema: bool,
    },
}

impl Default for OutputContentTypeRule {
    fn default() -> Self {
        Self::Set {
            content_type: http::HeaderValue::from_static("application/json"),
            set_content_type_if_empty: false,
            has_json_schema: false,
        }
    }
}

impl fmt::Display for OutputContentTypeRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputContentTypeRule::None => write!(f, "none"),
            OutputContentTypeRule::Set {
                content_type,
                has_json_schema,
                set_content_type_if_empty,
            } => {
                if *set_content_type_if_empty {
                    write!(f, "optional ")?;
                }
                if *has_json_schema {
                    write!(f, "JSON ")?;
                }
                write!(
                    f,
                    "value of content-type '{}'",
                    String::from_utf8_lossy(content_type.as_bytes())
                )
            }
        }
    }
}

#[cfg(feature = "mocks")]
#[allow(dead_code)]
pub mod mocks {
    use super::*;

    use std::collections::HashMap;

    #[derive(Debug, Clone)]
    pub struct MockService(HashMap<String, InvocationTargetMetadata>);

    #[derive(Debug, Default, Clone)]
    pub struct MockInvocationTargetResolver(HashMap<String, MockService>);

    impl MockInvocationTargetResolver {
        pub fn add(
            &mut self,
            service_name: &str,
            handlers: impl IntoIterator<Item = (impl ToString, InvocationTargetMetadata)>,
        ) {
            self.0.insert(
                service_name.to_owned(),
                MockService(
                    handlers
                        .into_iter()
                        .map(|(s, i)| (s.to_string(), i))
                        .collect(),
                ),
            );
        }
    }

    impl InvocationTargetResolver for MockInvocationTargetResolver {
        fn resolve_latest_invocation_target(
            &self,
            service_name: impl AsRef<str>,
            handler_name: impl AsRef<str>,
        ) -> Option<InvocationTargetMetadata> {
            self.0
                .get(service_name.as_ref())
                .and_then(|c| c.0.get(handler_name.as_ref()).cloned())
        }
    }

    impl InvocationTargetMetadata {
        pub fn mock(service_ty: ServiceType, handler_ty: HandlerType) -> Self {
            Self {
                public: true,
                idempotency_retention: DEFAULT_IDEMPOTENCY_RETENTION,
                service_ty,
                handler_ty,
                input_rules: Default::default(),
                output_rules: Default::default(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_input_valid {
        ($rule:expr, $ct:expr, $body:expr) => {
            let res = $rule.validate($ct, &$body);
            assert!(
                res.is_ok(),
                "Rule {:?} with content-type {:?} and body {:?} should be valid, error: {:?}",
                $rule,
                $ct as Option<&'static str>,
                $body,
                res.unwrap_err()
            )
        };
    }

    macro_rules! assert_input_not_valid {
        ($rule:expr, $ct:expr, $body:expr) => {
            let res = $rule.validate($ct, &$body);
            assert!(
                res.is_err(),
                "Rule {:?} with content-type {:?} and body {:?} should be invalid",
                $rule,
                $ct as Option<&'static str>,
                $body
            )
        };
    }

    #[test]
    fn validate_default() {
        let input_rules = InputRules::default();

        assert_input_valid!(input_rules, Some("application/json"), Bytes::new());
        assert_input_valid!(input_rules, Some("text/plain"), Bytes::new());
        assert_input_valid!(
            input_rules,
            Some("application/json"),
            Bytes::from_static(b"{}")
        );
        assert_input_not_valid!(input_rules, None, Bytes::from_static(b"123"));
    }

    #[test]
    fn validate_empty_only() {
        let input_rules = InputRules {
            input_validation_rules: vec![InputValidationRule::NoBodyAndContentType],
        };

        assert_input_valid!(input_rules, None, Bytes::new());
        assert_input_not_valid!(input_rules, Some("application/json"), Bytes::new());
        assert_input_not_valid!(input_rules, None, Bytes::from_static(b"{}"));
    }

    #[test]
    fn validate_any_content_type() {
        let input_rules = InputRules {
            input_validation_rules: vec![InputValidationRule::ContentType {
                content_type: InputContentType::Any,
            }],
        };

        assert_input_valid!(
            input_rules,
            Some("application/restate+json"),
            Bytes::from_static(b"{}")
        );
        assert_input_valid!(input_rules, Some("application/restate+json"), Bytes::new());
        assert_input_not_valid!(input_rules, None, Bytes::from_static(b"{}"));
        assert_input_not_valid!(input_rules, None, Bytes::new());
    }

    #[test]
    fn validate_mime_content_type() {
        let input_rules = InputRules {
            input_validation_rules: vec![InputValidationRule::ContentType {
                content_type: InputContentType::MimeType("application".into()),
            }],
        };

        assert_input_valid!(input_rules, Some("application/restate+json"), Bytes::new());
        assert_input_not_valid!(input_rules, Some("text/json"), Bytes::new());
    }

    #[test]
    fn validate_mime_and_subtype_content_type() {
        let input_rules = InputRules {
            input_validation_rules: vec![InputValidationRule::ContentType {
                content_type: InputContentType::MimeTypeAndSubtype(
                    "application".into(),
                    "json".into(),
                ),
            }],
        };

        assert_input_valid!(input_rules, Some("application/json"), Bytes::new());
        assert_input_not_valid!(input_rules, Some("application/cbor"), Bytes::new());
    }

    #[test]
    fn validate_either_empty_or_non_empty_json() {
        let input_rules = InputRules {
            input_validation_rules: vec![
                InputValidationRule::NoBodyAndContentType,
                InputValidationRule::JsonValue {
                    content_type: InputContentType::Any,
                },
            ],
        };

        assert_input_valid!(input_rules, None, Bytes::new());
        assert_input_valid!(
            input_rules,
            Some("application/restate+json"),
            Bytes::from_static(b"{}")
        );
        assert_input_not_valid!(input_rules, Some("application/json"), Bytes::new());
    }

    #[test]
    fn validate_json_only() {
        let input_rules = InputRules {
            input_validation_rules: vec![InputValidationRule::JsonValue {
                content_type: InputContentType::MimeTypeAndSubtype(
                    "application".into(),
                    "restate+json".into(),
                ),
            }],
        };

        assert_input_valid!(
            input_rules,
            Some("application/restate+json"),
            Bytes::from_static(b"{}")
        );
        assert_input_not_valid!(
            input_rules,
            Some("application/json"),
            Bytes::from_static(b"{}")
        );
        assert_input_not_valid!(input_rules, None, Bytes::from_static(b"{}"));
        assert_input_not_valid!(input_rules, Some("application/restate+json"), Bytes::new());
    }

    #[test]
    fn infer_content_type_default() {
        let input_rules = OutputRules::default();

        assert_eq!(
            input_rules.infer_content_type(false),
            Some(http::HeaderValue::from_static("application/json"))
        );
        assert_eq!(input_rules.infer_content_type(true), None);
    }

    #[test]
    fn infer_content_type_set_content_type_if_empty() {
        let ct = http::HeaderValue::from_static("application/restate");
        let input_rules = OutputRules {
            content_type_rule: OutputContentTypeRule::Set {
                content_type: ct.clone(),
                set_content_type_if_empty: true,
                has_json_schema: false,
            },
        };

        assert_eq!(input_rules.infer_content_type(false), Some(ct.clone()));
        assert_eq!(input_rules.infer_content_type(true), Some(ct));
    }

    #[test]
    fn infer_content_type_empty() {
        let input_rules = OutputRules {
            content_type_rule: OutputContentTypeRule::None,
        };

        assert_eq!(input_rules.infer_content_type(false), None);
        assert_eq!(input_rules.infer_content_type(true), None);
    }
}
