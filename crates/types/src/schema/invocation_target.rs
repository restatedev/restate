// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::invocation::{InvocationRetention, InvocationTargetType, WorkflowHandlerType};

use bytes::Bytes;
use bytestring::ByteString;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use std::{cmp, fmt};

pub const DEFAULT_IDEMPOTENCY_RETENTION: Duration = Duration::from_secs(60 * 60 * 24);
pub const DEFAULT_WORKFLOW_COMPLETION_RETENTION: Duration = Duration::from_secs(60 * 60 * 24);

// TODO this type is not supposed to be serde!!! PLEASE DON'T USE THEM IN A SERIALIZEABLE DATA STRUCTURE.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    from = "serde_hacks::InvocationTargetMetadata",
    into = "serde_hacks::InvocationTargetMetadata"
)]
pub struct InvocationTargetMetadata {
    pub public: bool,

    /// Retention timer to be used for the completion. See [`InvocationTargetMetadata::compute_retention`] for more details.
    pub completion_retention: Duration,
    /// Retention timer to be used for the journal. See [`InvocationTargetMetadata::compute_retention`] for more details.
    pub journal_retention: Duration,

    pub target_ty: InvocationTargetType,
    pub input_rules: InputRules,
    pub output_rules: OutputRules,
}

impl InvocationTargetMetadata {
    pub fn compute_retention(&self, has_idempotency_key: bool) -> InvocationRetention {
        // See https://github.com/restatedev/restate/issues/892#issuecomment-2841609088
        match (self.target_ty, has_idempotency_key) {
            (InvocationTargetType::Workflow(WorkflowHandlerType::Workflow), _) | (_, true) => {
                // We should retain the completion when the call is to a workflow, or has idempotency key
                InvocationRetention {
                    completion_retention: self.completion_retention,
                    // We need to make sure journal_retention is smaller or equal to completion_retention,
                    // due to implementation requirement that invocation status must be retained at least as long as the journal.
                    journal_retention: cmp::min(self.journal_retention, self.completion_retention),
                }
            }
            (_, _) if !self.journal_retention.is_zero() => InvocationRetention {
                // To retain the journal, we must retain the completion too. No way out of this.
                completion_retention: self.journal_retention,
                journal_retention: self.journal_retention,
            },
            _ => InvocationRetention::none(),
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn json_schema(&self) -> Option<serde_json::Value> {
        for rule in &self.input_validation_rules {
            if let InputValidationRule::JsonValue { schema, .. } = rule {
                return schema.clone();
            }
        }
        None
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
            2 if self
                .input_validation_rules
                .contains(&InputValidationRule::NoBodyAndContentType) =>
            {
                // Skip to write none when only 2 rules and one of them is none
                // see https://github.com/restatedev/restate/issues/2101
                let (first, second) = (
                    &self.input_validation_rules[0],
                    &self.input_validation_rules[1],
                );
                write!(
                    f,
                    "{}",
                    if first == &InputValidationRule::NoBodyAndContentType {
                        second
                    } else {
                        first
                    }
                )
            }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        // Right now we don't use this schema for anything except printing,
        // so no need to use a more specialized type (we validate the schema is valid inside the schema registry updater)
        schema: Option<serde_json::Value>,
    },
}

impl fmt::Display for InputValidationRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputValidationRule::NoBodyAndContentType => write!(f, "none"),
            InputValidationRule::ContentType { content_type } => {
                write!(f, "{content_type}")
            }
            InputValidationRule::JsonValue {
                content_type,
                schema,
            } => try_display_json_detailed_info(f, schema.as_ref(), || content_type),
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
            InputValidationRule::JsonValue { content_type, .. } => {
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
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
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
    ) -> Result<(&'a str, &'a str), InputValidationError> {
        let ct_without_args = input_content_type
            .split_once(';')
            .map(|(ct, _)| ct)
            .unwrap_or(input_content_type);

        match ct_without_args.trim().split_once('/') {
            Some((first_part, second_part)) => Ok((first_part, second_part)),
            None => Err(InputValidationError::ContentTypeNotMatching(
                input_content_type.to_owned(),
                self.clone(),
            )),
        }
    }
}

impl fmt::Display for InputContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputContentType::Any => write!(f, "*/*"),
            InputContentType::MimeType(t) => write!(f, "{t}/*"),
            InputContentType::MimeTypeAndSubtype(t, st) => write!(f, "{t}/{st}"),
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRules {
    pub content_type_rule: OutputContentTypeRule,
    // Json schema, if present
    pub json_schema: Option<serde_json::Value>,
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

    pub fn json_schema(&self) -> Option<serde_json::Value> {
        self.json_schema.as_ref().cloned()
    }
}

impl fmt::Display for OutputRules {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        try_display_json_detailed_info(f, self.json_schema.as_ref(), || &self.content_type_rule)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputContentTypeRule {
    None,
    Set {
        #[serde(with = "serde_with::As::<restate_serde_util::HeaderValueSerde>")]
        content_type: http::HeaderValue,
        // If true, sets the content-type even if the output is empty.
        // Otherwise, don't set the content-type.
        set_content_type_if_empty: bool,
        // If true, this should be a JSON Value.
        // We don't need this field anymore, but we can't remove because we break back-compat
        #[deprecated]
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
                set_content_type_if_empty,
                ..
            } => {
                if *set_content_type_if_empty {
                    write!(f, "optional ")?;
                }
                write!(f, "{}", String::from_utf8_lossy(content_type.as_bytes()))
            }
        }
    }
}

fn try_display_json_detailed_info<D: fmt::Display>(
    f: &mut fmt::Formatter<'_>,
    schema: Option<&serde_json::Value>,
    default: impl FnOnce() -> D,
) -> fmt::Result {
    if let Some(serde_json::Value::String(title)) = schema.and_then(|s| s.get("title")) {
        write!(f, "JSON '{title}'")
    } else if let Some(serde_json::Value::String(ty)) = schema.and_then(|s| s.get("type")) {
        write!(f, "JSON {ty}")
    } else {
        write!(f, "{}", default())
    }
}

#[cfg(feature = "test-util")]
#[allow(dead_code)]
pub mod test_util {
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
        pub fn mock(invocation_target_type: InvocationTargetType) -> Self {
            Self {
                public: true,
                completion_retention: DEFAULT_IDEMPOTENCY_RETENTION,
                journal_retention: Duration::ZERO,
                target_ty: invocation_target_type,
                input_rules: Default::default(),
                output_rules: Default::default(),
            }
        }
    }
}

mod serde_hacks {
    use super::*;

    /// Some good old serde hacks here to make sure we can parse the old data structure.
    /// See the tests for more details on the old data structure and the various cases.
    /// Revisit this for Restate 1.5
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct InvocationTargetMetadata {
        pub public: bool,

        #[serde(default)]
        pub completion_retention: Option<Duration>,
        /// This is unused at this point, we just write it for backward compatibility.
        #[serde(default)]
        pub idempotency_retention: Duration,
        #[serde(default, skip_serializing_if = "Duration::is_zero")]
        pub journal_retention: Duration,

        pub target_ty: InvocationTargetType,
        pub input_rules: InputRules,
        pub output_rules: OutputRules,
    }

    impl From<super::InvocationTargetMetadata> for InvocationTargetMetadata {
        fn from(
            super::InvocationTargetMetadata {
                public,
                completion_retention,
                journal_retention,
                target_ty,
                input_rules,
                output_rules,
            }: super::InvocationTargetMetadata,
        ) -> Self {
            // This is the 1.3 business logic we need to please here.
            // Note that completion_retention was set only for Workflow methods anyway
            //   if has_idempotency_key {
            //     Some(cmp::max(
            //       self.completion_retention.unwrap_or_default(),
            //       self.idempotency_retention,
            //     ))
            //   } else {
            //     self.completion_retention
            //   }
            let completion_retention_old =
                if target_ty == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow) {
                    Some(completion_retention)
                } else {
                    None
                };
            Self {
                public,
                completion_retention: completion_retention_old,
                // Just always set it, won't hurt and doesn't violate the code below.
                idempotency_retention: completion_retention,
                journal_retention,
                target_ty,
                input_rules,
                output_rules,
            }
        }
    }

    impl From<InvocationTargetMetadata> for super::InvocationTargetMetadata {
        fn from(
            InvocationTargetMetadata {
                public,
                completion_retention,
                idempotency_retention,
                journal_retention,
                target_ty,
                input_rules,
                output_rules,
            }: InvocationTargetMetadata,
        ) -> Self {
            Self {
                public,
                // In the old data structure, either completion_retention was filled and used, or idempotency retention was filled.
                // So just try to pick first the one, then the other
                completion_retention: completion_retention.unwrap_or(idempotency_retention),
                journal_retention,
                target_ty,
                input_rules,
                output_rules,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod input_output_rules {
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
            assert_input_valid!(
                input_rules,
                Some("application/json; charset=utf8"),
                Bytes::new()
            );
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
            assert_input_valid!(
                input_rules,
                Some("application/json; charset=utf8"),
                Bytes::new()
            );
            assert_input_not_valid!(input_rules, Some("application/cbor"), Bytes::new());
        }

        #[test]
        fn validate_either_empty_or_non_empty_json() {
            let input_rules = InputRules {
                input_validation_rules: vec![
                    InputValidationRule::NoBodyAndContentType,
                    InputValidationRule::JsonValue {
                        content_type: InputContentType::Any,
                        schema: Default::default(),
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
                    schema: Default::default(),
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
                json_schema: None,
            };

            assert_eq!(input_rules.infer_content_type(false), Some(ct.clone()));
            assert_eq!(input_rules.infer_content_type(true), Some(ct));
        }

        #[test]
        fn infer_content_type_empty() {
            let input_rules = OutputRules {
                content_type_rule: OutputContentTypeRule::None,
                json_schema: None,
            };

            assert_eq!(input_rules.infer_content_type(false), None);
            assert_eq!(input_rules.infer_content_type(true), None);
        }
    }

    mod invocation_target_metadata {
        use super::*;

        pub const IDEMPOTENCY_RETENTION: Duration = Duration::from_secs(1);
        pub const WORKFLOW_COMPLETION_RETENTION: Duration = Duration::from_secs(2);

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct OldInvocationTargetMetadata {
            pub public: bool,
            pub completion_retention: Option<Duration>,
            pub idempotency_retention: Duration,
            pub target_ty: InvocationTargetType,
            pub input_rules: InputRules,
            pub output_rules: OutputRules,
        }

        impl OldInvocationTargetMetadata {
            pub fn compute_retention(&self, has_idempotency_key: bool) -> Option<Duration> {
                if has_idempotency_key {
                    Some(cmp::max(
                        self.completion_retention.unwrap_or_default(),
                        self.idempotency_retention,
                    ))
                } else {
                    self.completion_retention
                }
            }
        }

        #[test]
        fn workflow_invoke_forward_compatibility() {
            let expected_retention = InvocationRetention {
                completion_retention: WORKFLOW_COMPLETION_RETENTION,
                journal_retention: Default::default(),
            };

            let old = OldInvocationTargetMetadata {
                public: false,
                completion_retention: Some(WORKFLOW_COMPLETION_RETENTION),
                target_ty: InvocationTargetType::Workflow(WorkflowHandlerType::Workflow),
                input_rules: Default::default(),
                output_rules: Default::default(),
                idempotency_retention: IDEMPOTENCY_RETENTION,
            };

            let new: InvocationTargetMetadata =
                flexbuffers::from_slice(flexbuffers::to_vec(old).unwrap().as_slice()).unwrap();
            assert_eq!(new.compute_retention(false), expected_retention);

            // Roundtrip should work fine
            let new: InvocationTargetMetadata =
                flexbuffers::from_slice(flexbuffers::to_vec(new).unwrap().as_slice()).unwrap();
            assert_eq!(new.compute_retention(false), expected_retention);
        }

        #[test]
        fn idempotent_invoke_forward_compatibility() {
            let expected_retention = InvocationRetention {
                completion_retention: IDEMPOTENCY_RETENTION,
                journal_retention: Default::default(),
            };

            let old = OldInvocationTargetMetadata {
                public: false,
                completion_retention: None,
                target_ty: InvocationTargetType::Workflow(WorkflowHandlerType::Workflow),
                input_rules: Default::default(),
                output_rules: Default::default(),
                idempotency_retention: IDEMPOTENCY_RETENTION,
            };

            let new: InvocationTargetMetadata =
                flexbuffers::from_slice(flexbuffers::to_vec(old).unwrap().as_slice()).unwrap();
            assert_eq!(new.compute_retention(true), expected_retention);

            // Roundtrip should work fine
            let new: InvocationTargetMetadata =
                flexbuffers::from_slice(flexbuffers::to_vec(new).unwrap().as_slice()).unwrap();
            assert_eq!(new.compute_retention(true), expected_retention);
        }

        #[test]
        fn workflow_invoke_backward_compatibility() {
            let new = InvocationTargetMetadata {
                public: false,
                target_ty: InvocationTargetType::Workflow(WorkflowHandlerType::Workflow),
                completion_retention: WORKFLOW_COMPLETION_RETENTION,
                journal_retention: Default::default(),
                input_rules: Default::default(),
                output_rules: Default::default(),
            };

            let old: OldInvocationTargetMetadata =
                flexbuffers::from_slice(flexbuffers::to_vec(new).unwrap().as_slice()).unwrap();
            assert_eq!(
                old.compute_retention(false),
                Some(WORKFLOW_COMPLETION_RETENTION)
            );
        }

        #[test]
        fn idempotent_invoke_backward_compatibility() {
            let new = InvocationTargetMetadata {
                public: false,
                completion_retention: IDEMPOTENCY_RETENTION,
                journal_retention: Default::default(),
                target_ty: InvocationTargetType::Service,
                input_rules: Default::default(),
                output_rules: Default::default(),
            };

            let old: OldInvocationTargetMetadata =
                flexbuffers::from_slice(flexbuffers::to_vec(new).unwrap().as_slice()).unwrap();
            assert_eq!(old.compute_retention(true), Some(IDEMPOTENCY_RETENTION));
            assert_eq!(old.compute_retention(false), None);
        }
    }
}
