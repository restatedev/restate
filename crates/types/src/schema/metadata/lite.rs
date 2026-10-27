// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wire-compatible mirror of [`SchemaUnindexed`] that omits the handlers' JSON schemas.
//!
//! The JSON schemas are not needed by the partition processors but dominate the schema's memory
//! footprint. Decoding a full schema payload into these types skips the JSON schema fields
//! entirely (unknown fields are ignored by both bilrost and serde), and decoding a lite payload
//! into the full types yields `None` for them.
//!
//! All conversions destructure exhaustively so that adding a field to a full type fails to
//! compile until the corresponding lite type is updated.

use super::*;

use crate::schema::invocation_target::{
    InputContentType, InputValidationRule, OutputContentTypeRule,
};

/// [`SchemaUnindexed`] without the handlers' JSON schemas.
#[derive(derive_more::Debug, Clone, serde::Serialize, serde::Deserialize, bilrost::Message)]
#[serde(try_from = "serde_hacks::SchemaLite", into = "serde_hacks::SchemaLite")]
#[debug("SchemaUnindexedLite(version: {version})")]
pub struct SchemaUnindexedLite {
    #[bilrost(tag(1))]
    pub(super) version: Version,
    #[bilrost(tag(2))]
    pub(super) deployments: HashMap<DeploymentId, DeploymentLite>,
    #[bilrost(tag(3))]
    pub(super) subscriptions: HashMap<SubscriptionId, Subscription>,
    #[bilrost(tag(4))]
    pub(super) kafka_clusters: HashMap<String, KafkaCluster>,
    // See `SchemaUnindexed::bilrost_encoded`.
    // TODO(v1.10): Remove together with `SchemaUnindexed::bilrost_encoded`.
    #[bilrost(tag(5))]
    pub(super) bilrost_encoded: bool,
}

impl SchemaUnindexedLite {
    pub fn verify(&self) -> Result<(), UnknownDeploymentType> {
        for deployment in self.deployments.values() {
            if deployment.ty.is_none() {
                return Err(UnknownDeploymentType(deployment.id));
            }
        }

        Ok(())
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn is_bilrost_encoded(&self) -> bool {
        self.bilrost_encoded
    }

    pub fn clone_from_schema_unindexed(
        SchemaUnindexed {
            version,
            deployments,
            subscriptions,
            kafka_clusters,
            bilrost_encoded,
        }: &SchemaUnindexed,
    ) -> Self {
        Self {
            version: *version,
            deployments: deployments
                .iter()
                .map(|(id, deployment)| (*id, DeploymentLite::clone_from(deployment)))
                .collect(),
            subscriptions: subscriptions.clone(),
            kafka_clusters: kafka_clusters.clone(),
            bilrost_encoded: *bilrost_encoded,
        }
    }

    pub fn clone_from_schema(schema: &Schema) -> Self {
        Self::clone_from_schema_unindexed(&schema.unindexed)
    }
}

impl Versioned for SchemaUnindexedLite {
    fn version(&self) -> Version {
        self.version
    }
}

impl From<SchemaUnindexedLite> for SchemaUnindexed {
    fn from(
        SchemaUnindexedLite {
            version,
            deployments,
            subscriptions,
            kafka_clusters,
            bilrost_encoded,
        }: SchemaUnindexedLite,
    ) -> Self {
        Self {
            version,
            deployments: deployments
                .into_iter()
                .map(|(id, deployment)| (id, deployment.into()))
                .collect(),
            subscriptions,
            kafka_clusters,
            bilrost_encoded,
        }
    }
}

impl From<SchemaUnindexedLite> for Schema {
    fn from(lite: SchemaUnindexedLite) -> Self {
        SchemaUnindexed::from(lite).into()
    }
}

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub(super) struct DeploymentLite {
    #[bilrost(tag = 1)]
    pub(super) id: DeploymentId,
    // See `Deployment::ty`
    #[bilrost(oneof(2, 3))]
    ty: Option<DeploymentType>,
    #[bilrost(tag = 4)]
    delivery_options: DeliveryOptions,
    #[bilrost(tag = 5)]
    supported_protocol_versions: RangeInclusive<i32>,
    #[bilrost(tag = 6)]
    sdk_version: Option<String>,
    #[bilrost(tag = 7)]
    created_at: MillisSinceEpoch,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[bilrost(tag = 8)]
    metadata: HashMap<String, String>,
    #[serde_as(as = "restate_serde_util::MapAsVec")]
    #[bilrost(tag = 9, encoding(map<general_packed, Arced>))]
    services: HashMap<String, Arc<ServiceRevisionLite>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[bilrost(tag = 10)]
    limits: Option<DeploymentLimits>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag = 11)]
    json_schemas: Option<JsonSchemasId>,
}

impl DeploymentLite {
    fn clone_from(
        Deployment {
            id,
            ty,
            delivery_options,
            supported_protocol_versions,
            sdk_version,
            created_at,
            metadata,
            services,
            limits,
            json_schemas,
        }: &Deployment,
    ) -> Self {
        Self {
            id: *id,
            ty: ty.clone(),
            delivery_options: delivery_options.clone(),
            supported_protocol_versions: supported_protocol_versions.clone(),
            sdk_version: sdk_version.clone(),
            created_at: *created_at,
            metadata: metadata.clone(),
            services: services
                .iter()
                .map(|(name, service)| {
                    (
                        name.clone(),
                        Arc::new(ServiceRevisionLite::clone_from(service)),
                    )
                })
                .collect(),
            limits: *limits,
            json_schemas: *json_schemas,
        }
    }
}

impl From<DeploymentLite> for Deployment {
    fn from(
        DeploymentLite {
            id,
            ty,
            delivery_options,
            supported_protocol_versions,
            sdk_version,
            created_at,
            metadata,
            services,
            limits,
            json_schemas,
        }: DeploymentLite,
    ) -> Self {
        Self {
            id,
            ty,
            delivery_options,
            supported_protocol_versions,
            sdk_version,
            created_at,
            metadata,
            services: services
                .into_iter()
                .map(|(name, service)| (name, Arc::new(Arc::unwrap_or_clone(service).into())))
                .collect(),
            limits,
            json_schemas,
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bilrost::Message)]
struct ServiceRevisionLite {
    #[bilrost(tag(1))]
    name: String,
    #[serde_as(as = "restate_serde_util::MapAsVec")]
    #[bilrost(tag(2))]
    handlers: HashMap<String, HandlerLite>,
    #[bilrost(tag(3))]
    ty: ServiceType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag(4))]
    documentation: Option<String>,
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    #[bilrost(tag(5))]
    metadata: HashMap<String, String>,
    #[bilrost(tag(6))]
    revision: identifiers::ServiceRevision,
    #[bilrost(tag(7))]
    public: bool,
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[bilrost(tag(8))]
    idempotency_retention: Option<Duration>,
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[bilrost(tag(9))]
    workflow_completion_retention: Option<Duration>,
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[bilrost(tag(10))]
    journal_retention: Option<Duration>,
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[bilrost(tag(11))]
    inactivity_timeout: Option<Duration>,
    #[serde(
        with = "serde_with::As::<Option<FriendlyDuration>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[bilrost(tag(12))]
    abort_timeout: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    #[bilrost(tag(13))]
    enable_lazy_state: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag(19))]
    state_preload_policy: Option<StatePreloadPolicy>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<FriendlyDuration>>"
    )]
    #[bilrost(tag(14))]
    retry_policy_initial_interval: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag(15))]
    retry_policy_exponentiation_factor: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag(16))]
    retry_policy_max_attempts: Option<NonZeroUsize>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<FriendlyDuration>>"
    )]
    #[bilrost(tag(17))]
    retry_policy_max_interval: Option<Duration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag(18))]
    retry_policy_on_max_attempts: Option<OnMaxAttempts>,
}

impl MapAsVecItem for ServiceRevisionLite {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.clone()
    }
}

impl ServiceRevisionLite {
    fn clone_from(
        ServiceRevision {
            name,
            handlers,
            ty,
            documentation,
            metadata,
            revision,
            public,
            idempotency_retention,
            workflow_completion_retention,
            journal_retention,
            inactivity_timeout,
            abort_timeout,
            enable_lazy_state,
            state_preload_policy,
            retry_policy_initial_interval,
            retry_policy_exponentiation_factor,
            retry_policy_max_attempts,
            retry_policy_max_interval,
            retry_policy_on_max_attempts,
            service_openapi_cache: _,
        }: &ServiceRevision,
    ) -> Self {
        Self {
            name: name.clone(),
            handlers: handlers
                .iter()
                .map(|(name, handler)| (name.clone(), HandlerLite::clone_from(handler)))
                .collect(),
            ty: *ty,
            documentation: documentation.clone(),
            metadata: metadata.clone(),
            revision: *revision,
            public: *public,
            idempotency_retention: *idempotency_retention,
            workflow_completion_retention: *workflow_completion_retention,
            journal_retention: *journal_retention,
            inactivity_timeout: *inactivity_timeout,
            abort_timeout: *abort_timeout,
            enable_lazy_state: *enable_lazy_state,
            state_preload_policy: state_preload_policy.clone(),
            retry_policy_initial_interval: *retry_policy_initial_interval,
            retry_policy_exponentiation_factor: *retry_policy_exponentiation_factor,
            retry_policy_max_attempts: *retry_policy_max_attempts,
            retry_policy_max_interval: *retry_policy_max_interval,
            retry_policy_on_max_attempts: *retry_policy_on_max_attempts,
        }
    }
}

impl From<ServiceRevisionLite> for ServiceRevision {
    fn from(
        ServiceRevisionLite {
            name,
            handlers,
            ty,
            documentation,
            metadata,
            revision,
            public,
            idempotency_retention,
            workflow_completion_retention,
            journal_retention,
            inactivity_timeout,
            abort_timeout,
            enable_lazy_state,
            state_preload_policy,
            retry_policy_initial_interval,
            retry_policy_exponentiation_factor,
            retry_policy_max_attempts,
            retry_policy_max_interval,
            retry_policy_on_max_attempts,
        }: ServiceRevisionLite,
    ) -> Self {
        Self {
            name,
            handlers: handlers
                .into_iter()
                .map(|(name, handler)| (name, handler.into()))
                .collect(),
            ty,
            documentation,
            metadata,
            revision,
            public,
            idempotency_retention,
            workflow_completion_retention,
            journal_retention,
            inactivity_timeout,
            abort_timeout,
            enable_lazy_state,
            state_preload_policy,
            retry_policy_initial_interval,
            retry_policy_exponentiation_factor,
            retry_policy_max_attempts,
            retry_policy_max_interval,
            retry_policy_on_max_attempts,
            service_openapi_cache: default_empty_openapi_cache(),
        }
    }
}

#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bilrost::Message)]
struct HandlerLite {
    #[bilrost(tag = 1)]
    name: String,
    #[bilrost(tag = 2)]
    target_ty: InvocationTargetType,
    #[bilrost(tag = 3)]
    input_rules: InputRulesLite,
    #[bilrost(tag = 4)]
    output_rules: OutputRulesLite,
    #[bilrost(tag = 5)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    public: Option<bool>,
    #[bilrost(tag = 6)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    idempotency_retention: Option<Duration>,
    #[bilrost(tag = 7)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workflow_completion_retention: Option<Duration>,
    #[bilrost(tag = 8)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    journal_retention: Option<Duration>,
    #[bilrost(tag = 9)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inactivity_timeout: Option<Duration>,
    #[bilrost(tag = 10)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    abort_timeout: Option<Duration>,
    #[bilrost(tag = 11)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    documentation: Option<String>,
    #[bilrost(tag = 12)]
    #[serde(skip_serializing_if = "Option::is_none", default)]
    enable_lazy_state: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[bilrost(tag = 19)]
    state_preload_policy: Option<StatePreloadPolicy>,
    #[bilrost(tag = 13)]
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    metadata: HashMap<String, String>,
    #[bilrost(tag = 14)]
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<FriendlyDuration>>"
    )]
    retry_policy_initial_interval: Option<Duration>,
    #[bilrost(tag = 15)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_exponentiation_factor: Option<f32>,
    #[bilrost(tag = 16)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_max_attempts: Option<NonZeroUsize>,
    #[bilrost(tag = 17)]
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_with::As::<Option<FriendlyDuration>>"
    )]
    retry_policy_max_interval: Option<Duration>,
    #[bilrost(tag = 18)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy_on_max_attempts: Option<OnMaxAttempts>,
}

impl MapAsVecItem for HandlerLite {
    type Key = String;

    fn key(&self) -> Self::Key {
        self.name.clone()
    }
}

impl HandlerLite {
    fn clone_from(
        Handler {
            name,
            target_ty,
            input_rules,
            output_rules,
            public,
            idempotency_retention,
            workflow_completion_retention,
            journal_retention,
            inactivity_timeout,
            abort_timeout,
            documentation,
            enable_lazy_state,
            state_preload_policy,
            metadata,
            retry_policy_initial_interval,
            retry_policy_exponentiation_factor,
            retry_policy_max_attempts,
            retry_policy_max_interval,
            retry_policy_on_max_attempts,
        }: &Handler,
    ) -> Self {
        Self {
            name: name.clone(),
            target_ty: *target_ty,
            input_rules: InputRulesLite::clone_from(input_rules),
            output_rules: OutputRulesLite::clone_from(output_rules),
            public: *public,
            idempotency_retention: *idempotency_retention,
            workflow_completion_retention: *workflow_completion_retention,
            journal_retention: *journal_retention,
            inactivity_timeout: *inactivity_timeout,
            abort_timeout: *abort_timeout,
            documentation: documentation.clone(),
            enable_lazy_state: *enable_lazy_state,
            state_preload_policy: state_preload_policy.clone(),
            metadata: metadata.clone(),
            retry_policy_initial_interval: *retry_policy_initial_interval,
            retry_policy_exponentiation_factor: *retry_policy_exponentiation_factor,
            retry_policy_max_attempts: *retry_policy_max_attempts,
            retry_policy_max_interval: *retry_policy_max_interval,
            retry_policy_on_max_attempts: *retry_policy_on_max_attempts,
        }
    }
}

impl From<HandlerLite> for Handler {
    fn from(
        HandlerLite {
            name,
            target_ty,
            input_rules,
            output_rules,
            public,
            idempotency_retention,
            workflow_completion_retention,
            journal_retention,
            inactivity_timeout,
            abort_timeout,
            documentation,
            enable_lazy_state,
            state_preload_policy,
            metadata,
            retry_policy_initial_interval,
            retry_policy_exponentiation_factor,
            retry_policy_max_attempts,
            retry_policy_max_interval,
            retry_policy_on_max_attempts,
        }: HandlerLite,
    ) -> Self {
        Self {
            name,
            target_ty,
            input_rules: Arc::new(input_rules.into()),
            output_rules: Arc::new(output_rules.into()),
            public,
            idempotency_retention,
            workflow_completion_retention,
            journal_retention,
            inactivity_timeout,
            abort_timeout,
            documentation,
            enable_lazy_state,
            state_preload_policy,
            metadata,
            retry_policy_initial_interval,
            retry_policy_exponentiation_factor,
            retry_policy_max_attempts,
            retry_policy_max_interval,
            retry_policy_on_max_attempts,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, bilrost::Message)]
struct InputRulesLite {
    #[bilrost(tag = 1)]
    input_validation_rules: Vec<InputValidationRuleLite>,
}

impl InputRulesLite {
    fn clone_from(
        InputRules {
            input_validation_rules,
        }: &InputRules,
    ) -> Self {
        Self {
            input_validation_rules: input_validation_rules
                .iter()
                .map(InputValidationRuleLite::clone_from)
                .collect(),
        }
    }
}

impl From<InputRulesLite> for InputRules {
    fn from(
        InputRulesLite {
            input_validation_rules,
        }: InputRulesLite,
    ) -> Self {
        Self {
            input_validation_rules: input_validation_rules.into_iter().map(Into::into).collect(),
        }
    }
}

/// Mirror of [`InputValidationRule`] without the JSON schema (bilrost tag 2 of `JsonValue`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bilrost::Message, bilrost::Oneof)]
enum InputValidationRuleLite {
    NoBodyAndContentType,
    #[bilrost(tag = 1)]
    ContentType {
        content_type: InputContentType,
    },
    #[bilrost(tag = 2, message)]
    JsonValue {
        #[bilrost(tag = 1)]
        content_type: InputContentType,
    },
}

impl InputValidationRuleLite {
    fn clone_from(rule: &InputValidationRule) -> Self {
        match rule {
            InputValidationRule::NoBodyAndContentType => Self::NoBodyAndContentType,
            InputValidationRule::ContentType { content_type } => Self::ContentType {
                content_type: content_type.clone(),
            },
            InputValidationRule::JsonValue {
                content_type,
                schema: _,
            } => Self::JsonValue {
                content_type: content_type.clone(),
            },
        }
    }
}

impl From<InputValidationRuleLite> for InputValidationRule {
    fn from(rule: InputValidationRuleLite) -> Self {
        match rule {
            InputValidationRuleLite::NoBodyAndContentType => Self::NoBodyAndContentType,
            InputValidationRuleLite::ContentType { content_type } => {
                Self::ContentType { content_type }
            }
            InputValidationRuleLite::JsonValue { content_type } => Self::JsonValue {
                content_type,
                schema: None,
            },
        }
    }
}

/// Mirror of [`OutputRules`] without the JSON schema (bilrost tag 2).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, bilrost::Message)]
struct OutputRulesLite {
    #[bilrost(tag = 1)]
    content_type_rule: OutputContentTypeRule,
}

impl OutputRulesLite {
    fn clone_from(
        OutputRules {
            content_type_rule,
            json_schema: _,
        }: &OutputRules,
    ) -> Self {
        Self {
            content_type_rule: content_type_rule.clone(),
        }
    }
}

impl From<OutputRulesLite> for OutputRules {
    fn from(OutputRulesLite { content_type_rule }: OutputRulesLite) -> Self {
        Self {
            content_type_rule,
            json_schema: None,
        }
    }
}

mod storage {
    use bytes::BytesMut;

    use restate_platform::storage::{
        StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError,
    };

    use super::SchemaUnindexedLite;
    use crate::storage;

    // Mirrors the `SchemaUnindexed` storage codec so both types stay wire-compatible.
    impl StorageEncode for SchemaUnindexedLite {
        fn default_codec(&self) -> StorageCodecKind {
            // TODO(v1.9): return `StorageCodecKind::ZstdBilrostDefault` unconditionally.
            if self.bilrost_encoded {
                StorageCodecKind::ZstdBilrostDefault
            } else {
                StorageCodecKind::FlexbuffersSerde
            }
        }

        fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
            match self.default_codec() {
                StorageCodecKind::FlexbuffersSerde => {
                    storage::encode::encode_serde(self, buf, self.default_codec())
                }
                StorageCodecKind::ZstdBilrostDefault => {
                    storage::encode::encode_bilrost_zstd(self, buf)
                }
                _ => unreachable!("unsupported StorageCodecKind"),
            }
        }
    }

    impl StorageDecode for SchemaUnindexedLite {
        fn decode<B: bytes::Buf>(buf: B, kind: StorageCodecKind) -> Result<Self, StorageDecodeError>
        where
            Self: Sized,
        {
            let schema = match kind {
                StorageCodecKind::FlexbuffersSerde => {
                    storage::decode::decode_serde::<SchemaUnindexedLite, _>(buf, kind)?
                }
                StorageCodecKind::ZstdBilrostDefault => {
                    let mut schema =
                        storage::decode::decode_bilrost_zstd::<SchemaUnindexedLite, _>(buf)?;
                    // TODO(v1.10): Remove this assignment together with `bilrost_encoded`.
                    schema.bilrost_encoded = true;
                    schema
                }
                _ => return Err(StorageDecodeError::UnsupportedCodecKind(kind)),
            };

            schema
                .verify()
                .map_err(|err| StorageDecodeError::DecodeValue(err.into()))?;

            Ok(schema)
        }
    }
}
