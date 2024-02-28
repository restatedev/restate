use super::*;
use restate_schema_api::subscription::EventReceiverComponentType;

impl SchemasInner {
    pub(crate) fn compute_add_subscription<V: SubscriptionValidator>(
        &self,
        id: Option<SubscriptionId>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
        validator: &V,
    ) -> Result<(Subscription, SchemasUpdateCommand), SchemasUpdateError> {
        // generate id if not provided
        let id = id.unwrap_or_default();

        if self.subscriptions.contains_key(&id) {
            return Err(SchemasUpdateError::OverrideSubscription(id));
        }

        // TODO This logic to parse source and sink should be moved elsewhere to abstract over the known source/sink providers
        //  Maybe together with the validator?

        // Parse source
        let source = match source.scheme_str() {
            Some("kafka") => {
                let cluster_name = source.authority().ok_or_else(|| SchemasUpdateError::InvalidSubscription(anyhow!(
                    "source URI of Kafka type must have a authority segment containing the cluster name. Was '{}'",
                    source
                )))?.as_str();
                let topic_name = &source.path()[1..];
                Source::Kafka {
                    cluster: cluster_name.to_string(),
                    topic: topic_name.to_string(),
                    ordering_key_format: Default::default(),
                }
            }
            _ => {
                return Err(SchemasUpdateError::InvalidSubscription(anyhow!(
                    "source URI must have a scheme segment, with supported schemes: {:?}. Was '{}'",
                    ["kafka"],
                    source
                )))
            }
        };

        // Parse sink
        let sink = match sink.scheme_str() {
            Some("service") => {
                let service_name = sink.authority().ok_or_else(|| SchemasUpdateError::InvalidSubscription(anyhow!(
                    "sink URI of service type must have a authority segment containing the service name. Was '{}'",
                    sink
                )))?.as_str();
                let method_name = &sink.path()[1..];

                // Retrieve service and method in the schema registry
                let service_schemas = self.services.get(service_name).ok_or_else(|| {
                    SchemasUpdateError::InvalidSubscription(anyhow!(
                        "cannot find service specified in the sink URI. Was '{}'",
                        sink
                    ))
                })?;
                let method_schemas = service_schemas.methods.get(method_name).ok_or_else(|| {
                    SchemasUpdateError::InvalidSubscription(anyhow!(
                        "cannot find service method specified in the sink URI. Was '{}'",
                        sink
                    ))
                })?;

                let input_type = method_schemas.descriptor().input();
                let input_event_remap = if input_type.full_name() == "dev.restate.Event" {
                    // No remapping needed
                    None
                } else {
                    let key = if let Some(index) =
                        method_schemas.input_field_annotated(FieldAnnotation::Key)
                    {
                        let kind = input_type.get_field(index).unwrap().kind();
                        if kind == Kind::String {
                            Some((index, FieldRemapType::String))
                        } else {
                            Some((index, FieldRemapType::Bytes))
                        }
                    } else {
                        None
                    };

                    let payload = if let Some(index) =
                        method_schemas.input_field_annotated(FieldAnnotation::EventPayload)
                    {
                        let kind = input_type.get_field(index).unwrap().kind();
                        if kind == Kind::String {
                            Some((index, FieldRemapType::String))
                        } else {
                            Some((index, FieldRemapType::Bytes))
                        }
                    } else {
                        None
                    };

                    Some(InputEventRemap {
                        key,
                        payload,
                        attributes_index: method_schemas
                            .input_field_annotated(FieldAnnotation::EventMetadata),
                    })
                };

                let instance_type = match service_schemas.instance_type {
                    InstanceTypeMetadata::Keyed { .. } => {
                        // Verify the type is supported!
                        let key_field_kind = method_schemas.descriptor.input().get_field(
                            method_schemas.input_field_annotated(FieldAnnotation::Key).expect("There must be a key field for every method input type")
                        ).unwrap().kind();
                        if key_field_kind != Kind::String && key_field_kind != Kind::Bytes {
                            return Err(SchemasUpdateError::InvalidSubscription(anyhow!(
                                "Key type {:?} for sink {} is invalid, only bytes and string are supported.",
                                key_field_kind, sink
                            )));
                        }

                        EventReceiverServiceInstanceType::Keyed { ordering_key_is_key: false }
                    }
                    InstanceTypeMetadata::Unkeyed => EventReceiverServiceInstanceType::Unkeyed,
                    InstanceTypeMetadata::Singleton => EventReceiverServiceInstanceType::Singleton,
                    InstanceTypeMetadata::Unsupported | InstanceTypeMetadata::Custom { .. } => {
                        return Err(SchemasUpdateError::InvalidSubscription(anyhow!(
                            "trying to use a built-in service as sink {}. This is currently unsupported.",
                            sink
                        )))
                    }
                };

                Sink::Service {
                    name: service_name.to_string(),
                    method: method_name.to_string(),
                    input_event_remap,
                    instance_type,
                }
            }
            Some("component") => {
                let component_name = sink.authority().ok_or_else(|| SchemasUpdateError::InvalidSink(sink.clone(),
                    "sink URI of component type must have a authority segment containing the component name",
                ))?.as_str();
                let handler_name = &sink.path()[1..];

                // Retrieve component and handler in the schema registry
                let component_schemas = self.components.get(component_name).ok_or_else(|| {
                    SchemasUpdateError::InvalidSink(
                        sink.clone(),
                        "cannot find component specified in the sink URI",
                    )
                })?;
                if !component_schemas.handlers.contains_key(handler_name) {
                    return Err(SchemasUpdateError::InvalidSink(
                        sink,
                        "cannot find service method specified in the sink URI",
                    ));
                }

                let ty = match component_schemas.ty {
                    ComponentType::VirtualObject => EventReceiverComponentType::VirtualObject {
                        ordering_key_is_key: false,
                    },
                    ComponentType::Service => EventReceiverComponentType::Service,
                };

                Sink::Component {
                    name: component_name.to_owned(),
                    handler: handler_name.to_owned(),
                    ty,
                }
            }
            _ => return Err(SchemasUpdateError::InvalidSink(
                sink,
                "sink URI must have a scheme segment, with supported schemes: [service, component]",
            )),
        };

        let subscription = validator
            .validate(Subscription::new(
                id,
                source,
                sink,
                metadata.unwrap_or_default(),
            ))
            .map_err(|e| SchemasUpdateError::InvalidSubscription(e.into()))?;

        Ok((
            subscription.clone(),
            SchemasUpdateCommand::AddSubscription(subscription),
        ))
    }

    pub(crate) fn apply_add_subscription(
        &mut self,
        sub: Subscription,
    ) -> Result<(), SchemasUpdateError> {
        self.subscriptions.insert(sub.id(), sub);

        Ok(())
    }

    pub(crate) fn compute_remove_subscription(
        &self,
        id: SubscriptionId,
    ) -> Result<SchemasUpdateCommand, SchemasUpdateError> {
        if !self.subscriptions.contains_key(&id) {
            return Err(SchemasUpdateError::UnknownSubscription(id));
        }

        Ok(SchemasUpdateCommand::RemoveSubscription(id))
    }

    pub(crate) fn apply_remove_subscription(
        &mut self,
        id: SubscriptionId,
    ) -> Result<(), SchemasUpdateError> {
        self.subscriptions.remove(&id);

        Ok(())
    }
}
