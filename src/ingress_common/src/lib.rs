#![allow(dead_code)]

mod command;
mod request_response_handler;

pub(crate) use command::*;
pub use request_response_handler::RequestResponseHandler;

use bytes::Bytes;
use common::types::ServiceInvocationId;
use opentelemetry::Context;
use tonic::metadata::MetadataMap;
use tonic::Status;

#[derive(Debug)]
pub struct IngressRequestHeaders {
    service_name: String,
    method_name: String,
    metadata: MetadataMap,
    tracing_context: Context,
}
pub type IngressRequest = (IngressRequestHeaders, Bytes);
pub type IngressResponse = Bytes;
pub type IngressError = Status;
pub type IngressResult = Result<IngressResponse, IngressError>;

// Assert IngressRequest and IngressResponse are Send
const _: () = {
    fn assert_send<T: Send>() {}
    fn assert_all() {
        assert_send::<IngressRequest>();
        assert_send::<IngressResponse>();
    }
};

pub type IngressResponseRequester = UnboundedCommandSender<ServiceInvocationId, IngressResult>;
