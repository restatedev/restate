use bytes::Bytes;
use restate_ingress_grpc::{ServiceInvocationFactory, ServiceInvocationFactoryError};
use restate_service_key_extractor::{KeyExtractor, KeyExtractorsRegistry};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{
    ServiceInvocation, ServiceInvocationId, ServiceInvocationResponseSink, SpanRelation,
};
use tracing::Span;

#[derive(Debug, Clone)]
pub(super) struct DefaultServiceInvocationFactory {
    key_extractor_registry: KeyExtractorsRegistry,
}

impl DefaultServiceInvocationFactory {
    pub(super) fn new(key_extractor_registry: KeyExtractorsRegistry) -> Self {
        Self {
            key_extractor_registry,
        }
    }

    fn extract_key(
        &self,
        service_name: impl AsRef<str>,
        method_name: impl AsRef<str>,
        request_payload: Bytes,
    ) -> Result<Bytes, ServiceInvocationFactoryError> {
        self.key_extractor_registry
            .extract(service_name.as_ref(), method_name.as_ref(), request_payload)
            .map_err(|err| match err {
                restate_service_key_extractor::Error::NotFound => {
                    ServiceInvocationFactoryError::unknown_service_method(
                        service_name.as_ref(),
                        method_name.as_ref(),
                    )
                }
                err => ServiceInvocationFactoryError::key_extraction_error(err),
            })
    }
}

impl ServiceInvocationFactory for DefaultServiceInvocationFactory {
    fn create(
        &self,
        service_name: &str,
        method_name: &str,
        request_payload: Bytes,
        response_sink: Option<ServiceInvocationResponseSink>,
        span_relation: SpanRelation,
    ) -> Result<(ServiceInvocation, Span), ServiceInvocationFactoryError> {
        let key = self.extract_key(service_name, method_name, request_payload.clone())?;

        Ok(ServiceInvocation::new(
            ServiceInvocationId::new(service_name, key, InvocationId::now_v7()),
            method_name.into(),
            request_payload,
            response_sink,
            span_relation,
        ))
    }
}
