use bytes::{Buf, BufMut, BytesMut};
use http::HeaderValue;
use hyper::Body;
use prost::Message;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use serde::Serialize;
use tower::BoxError;

// --- Supported content types

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ConnectContentType {
    Protobuf,
    Json,
}

pub(super) fn resolve_content_type(content_type: &HeaderValue) -> Option<ConnectContentType> {
    if let Ok(ct) = content_type.to_str() {
        return if ct.starts_with("application/json") {
            Some(ConnectContentType::Json)
        } else if ct.starts_with("application/proto") || ct.starts_with("application/protobuf") {
            Some(ConnectContentType::Protobuf)
        } else {
            None
        };
    }
    None
}

pub(super) fn read_message(
    content_type: ConnectContentType,
    msg_desc: MessageDescriptor,
    payload_buf: impl Buf + Sized,
) -> Result<DynamicMessage, BoxError> {
    match content_type {
        ConnectContentType::Json => {
            let mut deser = serde_json::Deserializer::from_reader(payload_buf.reader());
            let dynamic_message = DynamicMessage::deserialize(msg_desc, &mut deser)?;
            deser.end()?;
            Ok(dynamic_message)
        }
        ConnectContentType::Protobuf => Ok(DynamicMessage::decode(msg_desc, payload_buf)?),
    }
}

pub(super) fn write_message(
    content_type: ConnectContentType,
    msg: DynamicMessage,
) -> Result<(HeaderValue, Body), BoxError> {
    match content_type {
        ConnectContentType::Json => {
            let mut ser = serde_json::Serializer::new(BytesMut::new().writer());

            msg.serialize(&mut ser)?;

            Ok((
                HeaderValue::from_static("application/json"),
                ser.into_inner().into_inner().freeze().into(),
            ))
        }
        ConnectContentType::Protobuf => Ok((
            HeaderValue::from_static("application/proto"),
            msg.encode_to_vec().into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::super::mocks::greeter_get_count_method_descriptor;
    use super::*;

    use bytes::Bytes;
    use test_utils::assert_eq;

    #[test]
    fn resolve_json() {
        assert_eq!(
            resolve_content_type(&HeaderValue::from_static("application/json")).unwrap(),
            ConnectContentType::Json
        );
        assert_eq!(
            resolve_content_type(&HeaderValue::from_static("application/json; encoding=utf8"))
                .unwrap(),
            ConnectContentType::Json
        );
    }

    #[test]
    fn read_google_protobuf_empty() {
        read_message(
            ConnectContentType::Json,
            greeter_get_count_method_descriptor().input(),
            Bytes::from("{}"),
        )
        .unwrap()
        .transcode_to::<()>()
        .unwrap();
    }

    #[tokio::test]
    async fn write_google_protobuf_empty() {
        let (_, b) = write_message(
            ConnectContentType::Json,
            DynamicMessage::new(greeter_get_count_method_descriptor().input()),
        )
        .unwrap();

        assert_eq!(
            hyper::body::to_bytes(b).await.unwrap(),
            Bytes::from_static(b"{}")
        )
    }
}
