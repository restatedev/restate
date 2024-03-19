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

#[cfg(feature = "deployment")]
pub mod deployment {
    use crate::component::ComponentMetadata;
    use bytestring::ByteString;
    use http::header::{HeaderName, HeaderValue};
    use http::Uri;
    use restate_types::identifiers::{ComponentRevision, DeploymentId, LambdaARN};
    use restate_types::time::MillisSinceEpoch;
    use std::collections::HashMap;
    use std::fmt;
    use std::fmt::{Display, Formatter};

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
        pub created_at: MillisSinceEpoch,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", serde_with::serde_as)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
        },
        Lambda {
            arn: LambdaARN,
            #[cfg_attr(feature = "serde_schema", schemars(with = "Option<String>"))]
            assume_role_arn: Option<ByteString>,
        },
    }

    impl DeploymentType {
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
            delivery_options: DeliveryOptions,
        ) -> Self {
            Self {
                ty: DeploymentType::Http {
                    address,
                    protocol_type,
                },
                delivery_options,
                created_at: MillisSinceEpoch::now(),
            }
        }

        pub fn new_lambda(
            arn: LambdaARN,
            assume_role_arn: Option<ByteString>,
            delivery_options: DeliveryOptions,
        ) -> Self {
            Self {
                ty: DeploymentType::Lambda {
                    arn,
                    assume_role_arn,
                },
                delivery_options,
                created_at: MillisSinceEpoch::now(),
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
        fn resolve_latest_deployment_for_component(
            &self,
            component_name: impl AsRef<str>,
        ) -> Option<Deployment>;

        fn get_deployment(&self, deployment_id: &DeploymentId) -> Option<Deployment>;

        fn get_deployment_and_components(
            &self,
            deployment_id: &DeploymentId,
        ) -> Option<(Deployment, Vec<ComponentMetadata>)>;

        fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ComponentRevision)>)>;
    }

    #[cfg(feature = "mocks")]
    pub mod mocks {
        use super::*;

        use std::collections::HashMap;

        impl Deployment {
            pub fn mock() -> Deployment {
                let id = "dp_15VqmTOnXH3Vv2pl5HOG7UB"
                    .parse()
                    .expect("valid stable deployment id");
                let metadata = DeploymentMetadata::new_http(
                    "http://localhost:9080".parse().unwrap(),
                    ProtocolType::BidiStream,
                    Default::default(),
                );

                Deployment { id, metadata }
            }

            pub fn mock_with_uri(uri: &str) -> Deployment {
                let id = DeploymentId::new();
                let metadata = DeploymentMetadata::new_http(
                    uri.parse().unwrap(),
                    ProtocolType::BidiStream,
                    Default::default(),
                );
                Deployment { id, metadata }
            }
        }

        #[derive(Default, Clone)]
        pub struct MockDeploymentMetadataRegistry {
            pub deployments: HashMap<DeploymentId, DeploymentMetadata>,
            pub latest_deployment: HashMap<String, DeploymentId>,
        }

        impl MockDeploymentMetadataRegistry {
            pub fn mock_component(&mut self, component: &str) {
                self.mock_component_with_metadata(component, Deployment::mock());
            }

            pub fn mock_component_with_metadata(
                &mut self,
                component: &str,
                deployment: Deployment,
            ) {
                self.latest_deployment
                    .insert(component.to_string(), deployment.id);
                self.deployments.insert(deployment.id, deployment.metadata);
            }
        }

        impl DeploymentResolver for MockDeploymentMetadataRegistry {
            fn resolve_latest_deployment_for_component(
                &self,
                component_name: impl AsRef<str>,
            ) -> Option<Deployment> {
                self.latest_deployment
                    .get(component_name.as_ref())
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

            fn get_deployment_and_components(
                &self,
                deployment_id: &DeploymentId,
            ) -> Option<(Deployment, Vec<ComponentMetadata>)> {
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

            fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ComponentRevision)>)> {
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

#[cfg(feature = "component")]
pub mod component {
    use restate_types::identifiers::{ComponentRevision, DeploymentId};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum ComponentType {
        Service,
        VirtualObject,
    }

    impl ComponentType {
        pub fn requires_key(&self) -> bool {
            matches!(self, ComponentType::VirtualObject)
        }
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct ComponentMetadata {
        /// # Name
        ///
        /// Fully qualified name of the component
        pub name: String,

        pub handlers: Vec<HandlerMetadata>,

        pub ty: ComponentType,

        /// # Deployment Id
        ///
        /// Deployment exposing the latest revision of the component.
        #[cfg_attr(feature = "serde_schema", schemars(with = "String"))]
        pub deployment_id: DeploymentId,

        /// # Revision
        ///
        /// Latest revision of the component.
        pub revision: ComponentRevision,

        /// # Public
        ///
        /// If true, the component can be invoked through the ingress.
        /// If false, the component can be invoked only from another Restate service.
        pub public: bool,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct HandlerMetadata {
        pub name: String,

        // # Human readable input description
        //
        // If empty, no schema was provided by the user at discovery time.
        pub input_description: Option<String>,

        // # Human readable output description
        //
        // If empty, no schema was provided by the user at discovery time.
        pub output_description: Option<String>,
    }

    #[derive(Debug, Clone)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub struct BasicComponentMetadata {
        pub public: bool,
        pub ty: ComponentType,
    }

    /// This API will return components registered by the user.
    pub trait ComponentMetadataResolver {
        /// Returns None if the component handler doesn't exist, Some(basic_component_metadata) otherwise.
        fn resolve_latest_component_handler(
            &self,
            component_name: impl AsRef<str>,
            handler_name: impl AsRef<str>,
        ) -> Option<BasicComponentMetadata>;

        fn resolve_latest_component_type(
            &self,
            component_name: impl AsRef<str>,
        ) -> Option<ComponentType>;

        fn resolve_latest_component(
            &self,
            component_name: impl AsRef<str>,
        ) -> Option<ComponentMetadata>;

        fn list_components(&self) -> Vec<ComponentMetadata>;
    }

    #[cfg(feature = "mocks")]
    #[allow(dead_code)]
    pub mod mocks {
        use super::*;

        use std::collections::HashMap;

        #[derive(Debug, Default, Clone)]
        pub struct MockComponentMetadataResolver(HashMap<String, ComponentMetadata>);

        impl MockComponentMetadataResolver {
            pub fn add(&mut self, component_metadata: ComponentMetadata) {
                self.0
                    .insert(component_metadata.name.clone(), component_metadata);
            }
        }

        impl ComponentMetadataResolver for MockComponentMetadataResolver {
            fn resolve_latest_component_handler(
                &self,
                component_name: impl AsRef<str>,
                handler_name: impl AsRef<str>,
            ) -> Option<BasicComponentMetadata> {
                let component = self.0.get(component_name.as_ref());
                if let Some(meta) = component {
                    if meta
                        .handlers
                        .iter()
                        .any(|m| m.name == handler_name.as_ref())
                    {
                        return Some(BasicComponentMetadata {
                            public: meta.public,
                            ty: meta.ty,
                        });
                    }
                }
                None
            }

            fn resolve_latest_component_type(
                &self,
                component_name: impl AsRef<str>,
            ) -> Option<ComponentType> {
                self.0.get(component_name.as_ref()).map(|c| c.ty)
            }

            fn resolve_latest_component(
                &self,
                component_name: impl AsRef<str>,
            ) -> Option<ComponentMetadata> {
                self.0.get(component_name.as_ref()).cloned()
            }

            fn list_components(&self) -> Vec<ComponentMetadata> {
                self.0.values().cloned().collect()
            }
        }

        impl ComponentMetadata {
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
                            input_description: None,
                            output_description: None,
                        })
                        .collect(),
                    ty: ComponentType::Service,
                    deployment_id: Default::default(),
                    revision: 0,
                    public: true,
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
                            input_description: None,
                            output_description: None,
                        })
                        .collect(),
                    ty: ComponentType::VirtualObject,
                    deployment_id: Default::default(),
                    revision: 0,
                    public: true,
                }
            }
        }
    }
}

#[cfg(feature = "subscription")]
pub mod subscription {
    use std::collections::HashMap;
    use std::fmt;

    use restate_types::identifiers::SubscriptionId;

    #[derive(Debug, Clone, Eq, PartialEq, Default)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum KafkaOrderingKeyFormat {
        #[default]
        ConsumerGroupTopicPartition,
        ConsumerGroupTopicPartitionKey,
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum Source {
        Kafka {
            cluster: String,
            topic: String,
            ordering_key_format: KafkaOrderingKeyFormat,
        },
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

    /// Specialized version of [super::component::ComponentType]
    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum EventReceiverComponentType {
        VirtualObject {
            // If true, event.ordering_key is the key, otherwise event.key is the key
            ordering_key_is_key: bool,
        },
        Service,
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "serde_schema", derive(schemars::JsonSchema))]
    pub enum Sink {
        Component {
            name: String,
            handler: String,
            ty: EventReceiverComponentType,
        },
    }

    impl fmt::Display for Sink {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Sink::Component { name, handler, .. } => {
                    write!(f, "component://{}/{}", name, handler)
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
        type Error: Into<anyhow::Error>;

        fn validate(&self, subscription: Subscription) -> Result<Subscription, Self::Error>;
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
                        ordering_key_format: Default::default(),
                    },
                    sink: Sink::Component {
                        name: "MySvc".to_string(),
                        handler: "MyMethod".to_string(),
                        ty: EventReceiverComponentType::Service,
                    },
                    metadata: Default::default(),
                }
            }
        }
    }
}
