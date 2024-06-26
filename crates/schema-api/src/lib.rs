// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains all the different APIs for accessing schemas.

#[cfg(feature = "invocation_target")]
pub mod invocation_target;

#[cfg(feature = "deployment")]
pub mod deployment {
    use crate::service::ServiceMetadata;
    use bytestring::ByteString;
    use http::header::{HeaderName, HeaderValue};
    use http::Uri;
    use restate_types::identifiers::{DeploymentId, LambdaARN, ServiceRevision};
    use restate_types::time::MillisSinceEpoch;
    use std::collections::HashMap;
    use std::fmt;
    use std::fmt::{Display, Formatter};
    use std::ops::RangeInclusive;

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum ProtocolType {
        RequestResponse,
        BidiStream,
    }

    #[derive(Debug, Clone, Default)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct DeliveryOptions {
        #[cfg_attr(
            feature = "serde",
            serde(
                with = "serde_with::As::<serde_with::FromInto<restate_serde_util::SerdeableHeaderHashMap>>"
            )
        )]
        #[cfg_attr(feature = "serde_schema", schemars(with = "HashMap<String, String>"))]
        pub additional_headers: HashMap<HeaderName, HeaderValue>,
    }

    impl DeliveryOptions {
        pub fn new(additional_headers: HashMap<HeaderName, HeaderValue>) -> Self {
            Self { additional_headers }
        }
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct Deployment {
        pub id: DeploymentId,
        pub metadata: DeploymentMetadata,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", serde_with::serde_as)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct DeploymentMetadata {
        pub ty: DeploymentType,
        pub delivery_options: DeliveryOptions,
        pub supported_protocol_versions: RangeInclusive<i32>,
        pub created_at: MillisSinceEpoch,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde", serde(from = "DeploymentTypeShadow"))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum DeploymentType {
        Http {
            #[cfg_attr(
                feature = "serde",
                serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
            )]
            #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
            address: Uri,
            protocol_type: ProtocolType,
            #[cfg_attr(
                feature = "serde",
                serde(with = "serde_with::As::<restate_serde_util::VersionSerde>")
            )]
            #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
            http_version: http::Version,
        },
        Lambda {
            arn: LambdaARN,
            #[cfg_attr(feature = "serde_schema", schemars(with = "Option<String>"))]
            assume_role_arn: Option<ByteString>,
        },
    }

    #[cfg_attr(feature = "serde", derive(serde::Deserialize))]
    #[cfg(feature = "serde")]
    enum DeploymentTypeShadow {
        Http {
            #[cfg_attr(
                feature = "serde",
                serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
            )]
            address: Uri,
            protocol_type: ProtocolType,
            #[cfg_attr(
                feature = "serde",
                serde(
                    default,
                    with = "serde_with::As::<Option<restate_serde_util::VersionSerde>>"
                )
            )]
            // this field did not used to be stored, so we must consider it optional when deserialising
            http_version: Option<http::Version>,
        },
        Lambda {
            arn: LambdaARN,
            assume_role_arn: Option<ByteString>,
        },
    }

    #[cfg(feature = "serde")]
    impl From<DeploymentTypeShadow> for DeploymentType {
        fn from(value: DeploymentTypeShadow) -> Self {
            match value {
                DeploymentTypeShadow::Http {
                    address,
                    protocol_type,
                    http_version,
                } => Self::Http {
                    address,
                    protocol_type,
                    http_version: match http_version {
                        Some(v) => v,
                        None => Self::backfill_http_version(protocol_type),
                    },
                },
                DeploymentTypeShadow::Lambda {
                    arn,
                    assume_role_arn,
                } => Self::Lambda {
                    arn,
                    assume_role_arn,
                },
            }
        }
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn can_deserialise_without_http_version() {
            let dt: super::DeploymentType = serde_json::from_str(
                r#"{"Http":{"address":"google.com","protocol_type":"BidiStream"}}"#,
            )
            .unwrap();
            let serialised = serde_json::to_string(&dt).unwrap();
            assert_eq!(
                r#"{"Http":{"address":"google.com","protocol_type":"BidiStream","http_version":"HTTP/2.0"}}"#,
                serialised
            );

            let dt: super::DeploymentType = serde_json::from_str(
                r#"{"Http":{"address":"google.com","protocol_type":"RequestResponse"}}"#,
            )
            .unwrap();
            let serialised = serde_json::to_string(&dt).unwrap();
            assert_eq!(
                r#"{"Http":{"address":"google.com","protocol_type":"RequestResponse","http_version":"HTTP/1.1"}}"#,
                serialised
            );
        }
    }

    impl DeploymentType {
        pub fn backfill_http_version(protocol_type: ProtocolType) -> http::Version {
            match protocol_type {
                ProtocolType::BidiStream => http::Version::HTTP_2,
                ProtocolType::RequestResponse => http::Version::HTTP_11,
            }
        }

        pub fn protocol_type(&self) -> ProtocolType {
            match self {
                DeploymentType::Http { protocol_type, .. } => *protocol_type,
                DeploymentType::Lambda { .. } => ProtocolType::RequestResponse,
            }
        }

        pub fn normalized_address(&self) -> String {
            match self {
                DeploymentType::Http { address, .. } => {
                    // We use only authority and path, as those uniquely identify the deployment.
                    format!(
                        "{}{}",
                        address.authority().expect("Must have authority"),
                        address.path()
                    )
                }
                DeploymentType::Lambda { arn, .. } => arn.to_string(),
            }
        }
    }

    impl DeploymentMetadata {
        pub fn new_http(
            address: Uri,
            protocol_type: ProtocolType,
            http_version: http::Version,
            delivery_options: DeliveryOptions,
            supported_protocol_versions: RangeInclusive<i32>,
        ) -> Self {
            Self {
                ty: DeploymentType::Http {
                    address,
                    protocol_type,
                    http_version,
                },
                delivery_options,
                created_at: MillisSinceEpoch::now(),
                supported_protocol_versions,
            }
        }

        pub fn new_lambda(
            arn: LambdaARN,
            assume_role_arn: Option<ByteString>,
            delivery_options: DeliveryOptions,
            supported_protocol_versions: RangeInclusive<i32>,
        ) -> Self {
            Self {
                ty: DeploymentType::Lambda {
                    arn,
                    assume_role_arn,
                },
                delivery_options,
                created_at: MillisSinceEpoch::now(),
                supported_protocol_versions,
            }
        }

        // address_display returns a Displayable identifier for the endpoint; for http endpoints this is a URI,
        // and for Lambda deployments its the ARN
        pub fn address_display(&self) -> impl Display + '_ {
            struct Wrapper<'a>(&'a DeploymentType);
            impl<'a> Display for Wrapper<'a> {
                fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                    match self {
                        Wrapper(DeploymentType::Http { address, .. }) => address.fmt(f),
                        Wrapper(DeploymentType::Lambda { arn, .. }) => arn.fmt(f),
                    }
                }
            }
            Wrapper(&self.ty)
        }

        pub fn created_at(&self) -> MillisSinceEpoch {
            self.created_at
        }
    }

    pub trait DeploymentResolver {
        fn resolve_latest_deployment_for_service(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<Deployment>;

        fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment>;

        fn get_deployment_and_services(
            &self,
            deployment_id: &DeploymentId,
        ) -> Option<(Deployment, Vec<ServiceMetadata>)>;

        fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)>;
    }

    #[cfg(feature = "test-util")]
    pub mod test_util {
        use super::*;

        use restate_types::service_protocol::MAX_SERVICE_PROTOCOL_VERSION_VALUE;
        use std::collections::HashMap;

        impl Deployment {
            pub fn mock() -> Deployment {
                let id = "dp_15VqmTOnXH3Vv2pl5HOG7UB"
                    .parse()
                    .expect("valid stable deployment id");
                let metadata = DeploymentMetadata::new_http(
                    "http://localhost:9080".parse().unwrap(),
                    ProtocolType::BidiStream,
                    http::Version::HTTP_2,
                    Default::default(),
                    1..=MAX_SERVICE_PROTOCOL_VERSION_VALUE,
                );

                Deployment { id, metadata }
            }

            pub fn mock_with_uri(uri: &str) -> Deployment {
                let id = DeploymentId::new();
                let metadata = DeploymentMetadata::new_http(
                    uri.parse().unwrap(),
                    ProtocolType::BidiStream,
                    http::Version::HTTP_2,
                    Default::default(),
                    1..=MAX_SERVICE_PROTOCOL_VERSION_VALUE,
                );
                Deployment { id, metadata }
            }
        }

        #[derive(Default, Clone, Debug)]
        pub struct MockDeploymentMetadataRegistry {
            pub deployments: HashMap<DeploymentId, DeploymentMetadata>,
            pub latest_deployment: HashMap<String, DeploymentId>,
        }

        impl MockDeploymentMetadataRegistry {
            pub fn mock_service(&mut self, service: &str) {
                self.mock_service_with_metadata(service, Deployment::mock());
            }

            pub fn mock_service_with_metadata(&mut self, service: &str, deployment: Deployment) {
                self.latest_deployment
                    .insert(service.to_string(), deployment.id);
                self.deployments.insert(deployment.id, deployment.metadata);
            }
        }

        impl DeploymentResolver for MockDeploymentMetadataRegistry {
            fn resolve_latest_deployment_for_service(
                &self,
                service_name: impl AsRef<str>,
            ) -> Option<Deployment> {
                self.latest_deployment
                    .get(service_name.as_ref())
                    .and_then(|deployment_id| self.get_deployment(deployment_id))
            }

            fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment> {
                self.deployments
                    .get(deployment_id)
                    .cloned()
                    .map(|metadata| Deployment {
                        id: *deployment_id,
                        metadata,
                    })
            }

            fn get_deployment_and_services(
                &self,
                deployment_id: &DeploymentId,
            ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
                self.deployments
                    .get(deployment_id)
                    .cloned()
                    .map(|metadata| {
                        (
                            Deployment {
                                id: *deployment_id,
                                metadata,
                            },
                            vec![],
                        )
                    })
            }

            fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
                self.deployments
                    .iter()
                    .map(|(id, metadata)| {
                        (
                            Deployment {
                                id: *id,
                                metadata: metadata.clone(),
                            },
                            vec![],
                        )
                    })
                    .collect()
            }
        }
    }
}

#[cfg(feature = "service")]
pub mod service {
    use restate_types::identifiers::{DeploymentId, ServiceRevision};
    use restate_types::invocation::{
        InvocationTargetType, ServiceType, VirtualObjectHandlerType, WorkflowHandlerType,
    };

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct ServiceMetadata {
        /// # Name
        ///
        /// Fully qualified name of the service
        pub name: String,

        pub handlers: Vec<HandlerMetadata>,

        pub ty: ServiceType,

        /// # Deployment Id
        ///
        /// Deployment exposing the latest revision of the service.
        #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
        pub deployment_id: DeploymentId,

        /// # Revision
        ///
        /// Latest revision of the service.
        pub revision: ServiceRevision,

        /// # Public
        ///
        /// If true, the service can be invoked through the ingress.
        /// If false, the service can be invoked only from another Restate service.
        pub public: bool,

        /// # Idempotency retention
        ///
        /// The retention duration of idempotent requests for this service.
        #[cfg_attr(
            feature = "serde",
            serde(with = "serde_with::As::<serde_with::DisplayFromStr>")
        )]
        #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
        pub idempotency_retention: humantime::Duration,

        /// # Workflow completion retention
        ///
        /// The retention duration of workflows. Only available on workflow services.
        #[cfg_attr(
            feature = "serde",
            serde(
                with = "serde_with::As::<Option<serde_with::DisplayFromStr>>",
                skip_serializing_if = "Option::is_none",
                default
            )
        )]
        #[cfg_attr(feature = "serde_schema", schemars(with = "Option<String>"))]
        pub workflow_completion_retention: Option<humantime::Duration>,
    }

    // This type is used only for exposing the handler metadata, and not internally. See [ServiceAndHandlerType].
    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum HandlerMetadataType {
        Exclusive,
        Shared,
        Workflow,
    }

    impl From<InvocationTargetType> for HandlerMetadataType {
        fn from(value: InvocationTargetType) -> Self {
            match value {
                InvocationTargetType::Service => HandlerMetadataType::Shared,
                InvocationTargetType::VirtualObject(h_ty) => match h_ty {
                    VirtualObjectHandlerType::Exclusive => HandlerMetadataType::Exclusive,
                    VirtualObjectHandlerType::Shared => HandlerMetadataType::Shared,
                },
                InvocationTargetType::Workflow(h_ty) => match h_ty {
                    WorkflowHandlerType::Workflow => HandlerMetadataType::Workflow,
                    WorkflowHandlerType::Shared => HandlerMetadataType::Shared,
                },
            }
        }
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct HandlerMetadata {
        pub name: String,

        pub ty: HandlerMetadataType,

        // # Human readable input description
        //
        // If empty, no schema was provided by the user at discovery time.
        pub input_description: String,

        // # Human readable output description
        //
        // If empty, no schema was provided by the user at discovery time.
        pub output_description: String,
    }

    /// This API will return services registered by the user.
    pub trait ServiceMetadataResolver {
        fn resolve_latest_service(&self, service_name: impl AsRef<str>) -> Option<ServiceMetadata>;

        fn resolve_latest_service_type(&self, service_name: impl AsRef<str>)
            -> Option<ServiceType>;

        fn list_services(&self) -> Vec<ServiceMetadata>;
    }

    #[cfg(feature = "test-util")]
    #[allow(dead_code)]
    pub mod test_util {
        use super::*;

        use std::collections::HashMap;

        #[derive(Debug, Default, Clone)]
        pub struct MockServiceMetadataResolver(HashMap<String, ServiceMetadata>);

        impl MockServiceMetadataResolver {
            pub fn add(&mut self, service_metadata: ServiceMetadata) {
                self.0
                    .insert(service_metadata.name.clone(), service_metadata);
            }
        }

        impl ServiceMetadataResolver for MockServiceMetadataResolver {
            fn resolve_latest_service(
                &self,
                service_name: impl AsRef<str>,
            ) -> Option<ServiceMetadata> {
                self.0.get(service_name.as_ref()).cloned()
            }

            fn resolve_latest_service_type(
                &self,
                service_name: impl AsRef<str>,
            ) -> Option<ServiceType> {
                self.0.get(service_name.as_ref()).map(|c| c.ty)
            }

            fn list_services(&self) -> Vec<ServiceMetadata> {
                self.0.values().cloned().collect()
            }
        }

        impl ServiceMetadata {
            pub fn mock_service(
                name: impl AsRef<str>,
                handlers: impl IntoIterator<Item = impl AsRef<str>>,
            ) -> Self {
                Self {
                    name: name.as_ref().to_string(),
                    handlers: handlers
                        .into_iter()
                        .map(|s| HandlerMetadata {
                            name: s.as_ref().to_string(),
                            ty: HandlerMetadataType::Shared,
                            input_description: "any".to_string(),
                            output_description: "any".to_string(),
                        })
                        .collect(),
                    ty: ServiceType::Service,
                    deployment_id: Default::default(),
                    revision: 0,
                    public: true,
                    idempotency_retention: std::time::Duration::from_secs(60).into(),
                    workflow_completion_retention: None,
                }
            }

            pub fn mock_virtual_object(
                name: impl AsRef<str>,
                handlers: impl IntoIterator<Item = impl AsRef<str>>,
            ) -> Self {
                Self {
                    name: name.as_ref().to_string(),
                    handlers: handlers
                        .into_iter()
                        .map(|s| HandlerMetadata {
                            name: s.as_ref().to_string(),
                            ty: HandlerMetadataType::Exclusive,
                            input_description: "any".to_string(),
                            output_description: "any".to_string(),
                        })
                        .collect(),
                    ty: ServiceType::VirtualObject,
                    deployment_id: Default::default(),
                    revision: 0,
                    public: true,
                    idempotency_retention: std::time::Duration::from_secs(60).into(),
                    workflow_completion_retention: None,
                }
            }
        }
    }
}

#[cfg(feature = "subscription")]
pub mod subscription {
    use restate_types::errors::GenericError;
    use std::collections::HashMap;
    use std::fmt;

    use restate_types::config::IngressOptions;
    use restate_types::identifiers::SubscriptionId;
    use tracing::warn;

    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum Source {
        Kafka { cluster: String, topic: String },
    }

    impl fmt::Display for Source {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Source::Kafka { cluster, topic, .. } => {
                    write!(f, "kafka://{}/{}", cluster, topic)
                }
            }
        }
    }

    impl PartialEq<&str> for Source {
        fn eq(&self, other: &&str) -> bool {
            self.to_string().as_str() == *other
        }
    }

    /// Specialized version of [super::service::ServiceType]
    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum EventReceiverServiceType {
        VirtualObject,
        Workflow,
        Service,
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum Sink {
        Service {
            name: String,
            handler: String,
            ty: EventReceiverServiceType,
        },
    }

    impl fmt::Display for Sink {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Sink::Service { name, handler, .. } => {
                    write!(f, "service://{}/{}", name, handler)
                }
            }
        }
    }

    impl PartialEq<&str> for Sink {
        fn eq(&self, other: &&str) -> bool {
            self.to_string().as_str() == *other
        }
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct Subscription {
        id: SubscriptionId,
        source: Source,
        sink: Sink,
        metadata: HashMap<String, String>,
    }

    impl Subscription {
        pub fn new(
            id: SubscriptionId,
            source: Source,
            sink: Sink,
            metadata: HashMap<String, String>,
        ) -> Self {
            Self {
                id,
                source,
                sink,
                metadata,
            }
        }

        pub fn id(&self) -> SubscriptionId {
            self.id
        }

        pub fn source(&self) -> &Source {
            &self.source
        }

        pub fn sink(&self) -> &Sink {
            &self.sink
        }

        pub fn metadata(&self) -> &HashMap<String, String> {
            &self.metadata
        }

        pub fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
            &mut self.metadata
        }
    }

    pub enum ListSubscriptionFilter {
        ExactMatchSink(String),
        ExactMatchSource(String),
    }

    impl ListSubscriptionFilter {
        pub fn matches(&self, sub: &Subscription) -> bool {
            match self {
                ListSubscriptionFilter::ExactMatchSink(sink) => sub.sink == sink.as_str(),
                ListSubscriptionFilter::ExactMatchSource(source) => sub.source == source.as_str(),
            }
        }
    }

    pub trait SubscriptionResolver {
        fn get_subscription(&self, id: SubscriptionId) -> Option<Subscription>;

        fn list_subscriptions(&self, filters: &[ListSubscriptionFilter]) -> Vec<Subscription>;
    }

    pub trait SubscriptionValidator {
        type Error: Into<GenericError>;

        fn validate(&self, subscription: Subscription) -> Result<Subscription, Self::Error>;
    }

    #[derive(Debug, thiserror::Error)]
    #[error("invalid option '{name}'. Reason: {reason}")]
    pub struct ValidationError {
        name: &'static str,
        reason: &'static str,
    }

    impl SubscriptionValidator for IngressOptions {
        type Error = ValidationError;

        fn validate(&self, mut subscription: Subscription) -> Result<Subscription, Self::Error> {
            // Retrieve the cluster option and merge them with subscription metadata
            let Source::Kafka { cluster, .. } = subscription.source();
            let cluster_options = &self.get_kafka_cluster(cluster).ok_or(ValidationError {
            name: "source",
            reason: "specified cluster in the source URI does not exist. Make sure it is defined in the KafkaOptions",
        })?.additional_options;

            if cluster_options.contains_key("enable.auto.commit")
                || subscription.metadata().contains_key("enable.auto.commit")
            {
                warn!("The configuration option enable.auto.commit should not be set and it will be ignored.");
            }
            if cluster_options.contains_key("enable.auto.offset.store")
                || subscription
                    .metadata()
                    .contains_key("enable.auto.offset.store")
            {
                warn!("The configuration option enable.auto.offset.store should not be set and it will be ignored.");
            }

            // Set the group.id if unset
            if !(cluster_options.contains_key("group.id")
                || subscription.metadata().contains_key("group.id"))
            {
                let group_id = subscription.id().to_string();

                subscription
                    .metadata_mut()
                    .insert("group.id".to_string(), group_id);
            }

            // Set client.id if unset
            if !(cluster_options.contains_key("client.id")
                || subscription.metadata().contains_key("client.id"))
            {
                subscription
                    .metadata_mut()
                    .insert("client.id".to_string(), "restate".to_string());
            }

            Ok(subscription)
        }
    }

    #[cfg(feature = "mocks")]
    pub mod mocks {
        use std::str::FromStr;

        use super::*;

        impl Subscription {
            pub fn mock() -> Self {
                let id = SubscriptionId::from_str("sub_15VqmTOnXH3Vv2pl5HOG7Ua")
                    .expect("stable valid subscription id");
                Subscription {
                    id,
                    source: Source::Kafka {
                        cluster: "my-cluster".to_string(),
                        topic: "my-topic".to_string(),
                    },
                    sink: Sink::Service {
                        name: "MySvc".to_string(),
                        handler: "MyMethod".to_string(),
                        ty: EventReceiverServiceType::Service,
                    },
                    metadata: Default::default(),
                }
            }
        }
    }
}
