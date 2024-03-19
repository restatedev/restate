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
                        "cannot find component handler specified in the sink URI",
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
            _ => {
                return Err(SchemasUpdateError::InvalidSink(
                    sink,
                    "sink URI must have a scheme segment, with supported schemes: [component]",
                ))
            }
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
