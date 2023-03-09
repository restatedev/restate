use bytes::Bytes;
use common::traits::ServiceInvocationFactory;
use common::types::{
    InvocationId, ServiceInvocation, ServiceInvocationFactoryError, ServiceInvocationId,
    ServiceInvocationResponseSink, SpanRelation,
};
use service_key_extractor::{KeyExtractor, KeyExtractorsRegistry};

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
                service_key_extractor::Error::NotFound => {
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
        response_sink: ServiceInvocationResponseSink,
        span_relation: SpanRelation,
    ) -> Result<ServiceInvocation, ServiceInvocationFactoryError> {
        let key = self.extract_key(service_name, method_name, request_payload.clone())?;

        let invocation_id = InvocationId::now_v7();
        let id = ServiceInvocationId::new(service_name, key, invocation_id);

        let service_invocation = ServiceInvocation {
            id,
            method_name: method_name.into(),
            response_sink,
            argument: request_payload,
            span_relation,
        };

        Ok(service_invocation)
    }
}
