// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use serde::Deserialize;
use serde::Serialize;

use crate::identifiers::SubscriptionId;
use crate::invocation::{VirtualObjectHandlerType, WorkflowHandlerType};
use crate::schema::Redaction;

// Why this is not an enum anymore?
//
// it's because the entire subscription mechanism will be deprecated
// once we merge the Ingestion API. This is only here now for
// backward compatibility and should not be extended.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, bilrost::Message)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(from = "serde_hacks::Source", into = "serde_hacks::Source")]
pub struct KafkaSource {
    #[bilrost(tag(1))]
    pub cluster: String,
    #[bilrost(tag(2))]
    pub topic: String,
}

impl fmt::Display for KafkaSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { cluster, topic } = self;
        write!(f, "kafka://{cluster}/{topic}")
    }
}

impl PartialEq<&str> for KafkaSource {
    fn eq(&self, other: &&str) -> bool {
        self.to_string().as_str() == *other
    }
}

// Why this is not an enum anymore?
//
// it's because the entire subscription mechanism will be deprecated
// once we merge the Ingestion API. This is only here now for
// backward compatibility and should not be extended.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, bilrost::Message)]
#[serde(from = "serde_hacks::Sink", into = "serde_hacks::Sink")]
pub struct Sink {
    #[bilrost(tag(1))]
    pub event_invocation_target_template: EventInvocationTargetTemplate,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, bilrost::Message)]
pub struct ServiceTemplate {
    #[bilrost(tag(1))]
    pub name: String,
    #[bilrost(tag(2))]
    pub handler: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, bilrost::Message)]
pub struct VirtualObjectTemplate {
    #[bilrost(tag(1))]
    pub name: String,
    #[bilrost(tag(2))]
    pub handler: String,
    #[bilrost(tag(3))]
    pub handler_ty: VirtualObjectHandlerType,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, bilrost::Message)]
pub struct WorkflowTemplate {
    #[bilrost(tag(1))]
    pub name: String,
    #[bilrost(tag(2))]
    pub handler: String,
    #[bilrost(tag(3))]
    pub handler_ty: WorkflowHandlerType,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, bilrost::Oneof, bilrost::Message)]
pub enum EventInvocationTargetTemplate {
    Unknown,
    #[bilrost(tag(1))]
    Service(ServiceTemplate),
    #[bilrost(tag(2))]
    VirtualObject(VirtualObjectTemplate),
    #[bilrost(tag(3))]
    Workflow(WorkflowTemplate),
}

impl fmt::Display for Sink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.event_invocation_target_template {
            EventInvocationTargetTemplate::Unknown => {
                write!(f, "unknown")
            }
            EventInvocationTargetTemplate::Service(ServiceTemplate { name, handler, .. })
            | EventInvocationTargetTemplate::VirtualObject(VirtualObjectTemplate {
                name,
                handler,
                ..
            })
            | EventInvocationTargetTemplate::Workflow(WorkflowTemplate { name, handler, .. }) => {
                write!(f, "service://{name}/{handler}")
            }
        }
    }
}

impl PartialEq<&str> for Sink {
    fn eq(&self, other: &&str) -> bool {
        self.to_string().as_str() == *other
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, bilrost::Message)]
pub struct Subscription {
    #[bilrost(tag(1))]
    id: SubscriptionId,
    #[bilrost(tag(2))]
    source: KafkaSource,
    #[bilrost(tag(3))]
    sink: Sink,
    #[bilrost(tag(4))]
    metadata: HashMap<String, String>,
}

impl Subscription {
    pub fn new(
        id: SubscriptionId,
        source: KafkaSource,
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

    pub fn source(&self) -> &KafkaSource {
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
    fn get_subscription(&self, id: SubscriptionId, redaction: Redaction) -> Option<Subscription>;

    fn list_subscriptions(
        &self,
        filters: &[ListSubscriptionFilter],
        redaction: Redaction,
    ) -> Vec<Subscription>;
}

mod serde_hacks {
    use super::*;

    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    pub enum Source {
        Kafka { cluster: String, topic: String },
    }

    impl From<KafkaSource> for Source {
        fn from(value: KafkaSource) -> Self {
            let KafkaSource { cluster, topic } = value;
            Self::Kafka { cluster, topic }
        }
    }

    impl From<Source> for KafkaSource {
        fn from(value: Source) -> Self {
            let Source::Kafka { cluster, topic } = value;
            Self { cluster, topic }
        }
    }

    /// Specialized version of [super::service::ServiceType]
    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    pub enum EventReceiverServiceType {
        VirtualObject,
        Workflow,
        Service,
    }

    // TODO(slinkydeveloper) this migration will be executed in 1.5, together with the new schema registry
    //  we should be able to remove it when we remove the old schema registry migration
    #[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
    pub enum Sink {
        // Could not use the Rust built-in deprecated feature because some macros will fail with it and won't apply the #[allow(deprecated)] :(
        #[serde(rename = "Service")]
        DeprecatedService {
            name: String,
            handler: String,
            ty: EventReceiverServiceType,
        },
        Invocation {
            event_invocation_target_template: EventInvocationTargetTemplate,
        },
    }

    impl From<Sink> for super::Sink {
        fn from(value: Sink) -> Self {
            match value {
                Sink::DeprecatedService {
                    name,
                    handler,
                    ty: EventReceiverServiceType::Service,
                } => Self {
                    event_invocation_target_template: EventInvocationTargetTemplate::Service(
                        ServiceTemplate { name, handler },
                    ),
                },
                Sink::DeprecatedService {
                    name,
                    handler,
                    ty: EventReceiverServiceType::VirtualObject,
                } => Self {
                    event_invocation_target_template: EventInvocationTargetTemplate::VirtualObject(
                        VirtualObjectTemplate {
                            name,
                            handler,
                            handler_ty: VirtualObjectHandlerType::Exclusive,
                        },
                    ),
                },
                Sink::DeprecatedService {
                    name,
                    handler,
                    ty: EventReceiverServiceType::Workflow,
                } => Self {
                    event_invocation_target_template: EventInvocationTargetTemplate::Workflow(
                        WorkflowTemplate {
                            name,
                            handler,
                            handler_ty: WorkflowHandlerType::Workflow,
                        },
                    ),
                },
                Sink::Invocation {
                    event_invocation_target_template,
                    ..
                } => Self {
                    event_invocation_target_template,
                },
            }
        }
    }

    impl From<super::Sink> for Sink {
        fn from(value: super::Sink) -> Self {
            Self::Invocation {
                event_invocation_target_template: value.event_invocation_target_template,
            }
        }
    }
}

#[cfg(feature = "test-util")]
pub mod mocks {
    use std::str::FromStr;

    use super::*;

    impl Subscription {
        pub fn mock() -> Self {
            let id = SubscriptionId::from_str("sub_15VqmTOnXH3Vv2pl5HOG7Ua")
                .expect("stable valid subscription id");
            Subscription {
                id,
                source: KafkaSource {
                    cluster: "my-cluster".to_string(),
                    topic: "my-topic".to_string(),
                },
                sink: Sink {
                    event_invocation_target_template: EventInvocationTargetTemplate::Service(
                        ServiceTemplate {
                            name: "MySvc".to_string(),
                            handler: "MyMethod".to_string(),
                        },
                    ),
                },
                metadata: Default::default(),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};

    use crate::{
        invocation::VirtualObjectHandlerType, schema::subscriptions::VirtualObjectTemplate,
    };

    #[test]
    fn serde_compatibility() {
        #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
        struct OldContainer {
            source: super::serde_hacks::Source,
            sink: super::serde_hacks::Sink,
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
        struct NewContainer {
            source: super::KafkaSource,
            sink: super::Sink,
        }

        let old = OldContainer {
            source: super::serde_hacks::Source::Kafka {
                cluster: "my-cluster".into(),
                topic: "my-topic".into(),
            },
            sink: super::serde_hacks::Sink::Invocation {
                event_invocation_target_template:
                    crate::schema::subscriptions::EventInvocationTargetTemplate::VirtualObject(
                        VirtualObjectTemplate {
                            name: "object".into(),
                            handler: "handler".into(),
                            handler_ty: VirtualObjectHandlerType::Exclusive,
                        },
                    ),
            },
        };

        let new = NewContainer {
            source: super::KafkaSource {
                cluster: "my-cluster".into(),
                topic: "my-topic".into(),
            },
            sink: super::Sink {
                event_invocation_target_template:
                    crate::schema::subscriptions::EventInvocationTargetTemplate::VirtualObject(
                        VirtualObjectTemplate {
                            name: "object".into(),
                            handler: "handler".into(),
                            handler_ty: VirtualObjectHandlerType::Exclusive,
                        },
                    ),
            },
        };

        let buffer = flexbuffers::to_vec(&new).unwrap();
        let loaded: OldContainer = flexbuffers::from_slice(&buffer).unwrap();
        assert_eq!(loaded, old);

        let buffer = flexbuffers::to_vec(&old).unwrap();
        let loaded: NewContainer = flexbuffers::from_slice(&buffer).unwrap();

        assert_eq!(loaded, new);
    }
}
