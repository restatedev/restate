use bytes::Bytes;
use restate_ingress_grpc::{ServiceInvocationFactory, ServiceInvocationFactoryError};
use restate_schema_api::key::KeyExtractor;
use restate_types::identifiers::{InvocationId, ServiceInvocationId};
use restate_types::invocation::{ServiceInvocation, ServiceInvocationResponseSink, SpanRelation};

#[derive(Debug, Clone)]
pub(super) struct DefaultServiceInvocationFactory<K> {
    key_extractor: K,
}
impl<K> DefaultServiceInvocationFactory<K> {
    pub(super) fn new(key_extractor: K) -> Self {
        Self { key_extractor }
    }
}

impl<K: KeyExtractor> DefaultServiceInvocationFactory<K> {
    fn extract_key(
        &self,
        service_name: impl AsRef<str>,
        method_name: impl AsRef<str>,
        request_payload: Bytes,
    ) -> Result<Bytes, ServiceInvocationFactoryError> {
        self.key_extractor
            .extract(service_name.as_ref(), method_name.as_ref(), request_payload)
            .map_err(|err| match err {
                restate_schema_api::key::KeyExtractorError::NotFound => {
                    ServiceInvocationFactoryError::unknown_service_method(
                        service_name.as_ref(),
                        method_name.as_ref(),
                    )
                }
                err => ServiceInvocationFactoryError::key_extraction_error(err),
            })
    }
}

impl<K: KeyExtractor> ServiceInvocationFactory for DefaultServiceInvocationFactory<K> {
    fn create(
        &self,
        service_name: &str,
        method_name: &str,
        request_payload: Bytes,
        response_sink: Option<ServiceInvocationResponseSink>,
        span_relation: SpanRelation,
    ) -> Result<ServiceInvocation, ServiceInvocationFactoryError> {
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
