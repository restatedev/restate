use super::*;
use restate_schema_api::subscription::EventReceiverComponentType;

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(restate_errors::META0009)]
pub enum SubscriptionErrorKind {
    #[error(
        "invalid source URI '{0}': must have a scheme segment, with supported schemes: [kafka]."
    )]
    InvalidSourceScheme(Uri),
    #[error("invalid source URI '{0}': source URI of Kafka type must have a authority segment containing the cluster name.")]
    InvalidKafkaSourceAuthority(Uri),

    #[error(
        "invalid sink URI '{0}': must have a scheme segment, with supported schemes: [component]."
    )]
    InvalidSinkScheme(Uri),
    #[error("invalid sink URI '{0}': sink URI of component type must have a authority segment containing the component name.")]
    InvalidComponentSinkAuthority(Uri),
    #[error("invalid sink URI '{0}': cannot find component/handler specified in the sink URI.")]
    SinkComponentNotFound(Uri),

    #[error(transparent)]
    #[code(unknown)]
    Validation(anyhow::Error),
}

impl SchemasInner {
    pub(crate) fn compute_add_subscription<V: SubscriptionValidator>(
        &self,
        id: Option<SubscriptionId>,
        source: Uri,
        sink: Uri,
        metadata: Option<HashMap<String, String>>,
        validator: &V,
    ) -> Result<(Subscription, SchemasUpdateCommand), ErrorKind> {
        // generate id if not provided
        let id = id.unwrap_or_default();

        if self.subscriptions.contains_key(&id) {
            return Err(ErrorKind::Override);
        }

        // TODO This logic to parse source and sink should be moved elsewhere to abstract over the known source/sink providers
        //  Maybe together with the validator?

        // Parse source
        let source = match source.scheme_str() {
            Some("kafka") => {
                let cluster_name = source
                    .authority()
                    .ok_or_else(|| {
                        ErrorKind::Subscription(SubscriptionErrorKind::InvalidKafkaSourceAuthority(
                            source.clone(),
                        ))
                    })?
                    .as_str();
                let topic_name = &source.path()[1..];
                Source::Kafka {
                    cluster: cluster_name.to_string(),
                    topic: topic_name.to_string(),
                    ordering_key_format: Default::default(),
                }
            }
            _ => {
                return Err(ErrorKind::Subscription(
                    SubscriptionErrorKind::InvalidSourceScheme(source),
                ))
            }
        };

        // Parse sink
        let sink = match sink.scheme_str() {
            Some("component") => {
                let component_name = sink
                    .authority()
                    .ok_or_else(|| {
                        ErrorKind::Subscription(
                            SubscriptionErrorKind::InvalidComponentSinkAuthority(sink.clone()),
                        )
                    })?
                    .as_str();
                let handler_name = &sink.path()[1..];

                // Retrieve component and handler in the schema registry
                let component_schemas = self.components.get(component_name).ok_or_else(|| {
                    ErrorKind::Subscription(SubscriptionErrorKind::SinkComponentNotFound(
                        sink.clone(),
                    ))
                })?;
                if !component_schemas.handlers.contains_key(handler_name) {
                    return Err(ErrorKind::Subscription(
                        SubscriptionErrorKind::SinkComponentNotFound(sink),
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
                return Err(ErrorKind::Subscription(
                    SubscriptionErrorKind::InvalidSinkScheme(sink),
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
            .map_err(|e| ErrorKind::Subscription(SubscriptionErrorKind::Validation(e.into())))?;

        Ok((
            subscription.clone(),
            SchemasUpdateCommand::AddSubscription(subscription),
        ))
    }

    pub(crate) fn apply_add_subscription(&mut self, sub: Subscription) {
        self.subscriptions.insert(sub.id(), sub);
    }

    pub(crate) fn compute_remove_subscription(
        &self,
        id: SubscriptionId,
    ) -> Result<SchemasUpdateCommand, ErrorKind> {
        if !self.subscriptions.contains_key(&id) {
            return Err(ErrorKind::NotFound);
        }

        Ok(SchemasUpdateCommand::RemoveSubscription(id))
    }

    pub(crate) fn apply_remove_subscription(&mut self, id: SubscriptionId) {
        self.subscriptions.remove(&id);
    }
}
