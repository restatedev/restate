// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::BytesMut;

use restate_encoding::NetSerde;
use restate_memory::EstimatedMemorySize;

use crate::storage::{PolyBytes, StorageCodec, StorageDecode, StorageDecodeError, StorageEncode};
use crate::time::NanosSinceEpoch;

use super::{KeyFilter, Keys, MatchKeyQuery};
pub use record_encoding::CustomRecordEncoding;

#[derive(Debug, Clone, bilrost::Message, NetSerde)]
pub struct Record {
    #[bilrost(tag(1))]
    created_at: NanosSinceEpoch,
    #[bilrost(tag(2))]
    body: PolyBytes,
    #[bilrost(tag(3))]
    keys: Keys,
}

impl Record {
    pub fn from_parts(created_at: NanosSinceEpoch, keys: Keys, body: PolyBytes) -> Self {
        Self {
            created_at,
            keys,
            body,
        }
    }

    pub fn created_at(&self) -> NanosSinceEpoch {
        self.created_at
    }

    pub const fn keys(&self) -> &Keys {
        &self.keys
    }

    pub fn estimated_encode_size(&self) -> usize {
        size_of::<Keys>() + size_of::<NanosSinceEpoch>() + self.body.estimated_encode_size()
    }

    pub fn ensure_encoded(self, buf: &mut BytesMut) -> Self {
        let keys = self.keys;
        let created_at = self.created_at;
        let body = match self.body {
            PolyBytes::Bytes(bytes) => PolyBytes::Bytes(bytes),
            PolyBytes::Both(v, bytes) => PolyBytes::Both(v, bytes),
            PolyBytes::Typed(typed) => {
                StorageCodec::encode(&*typed, buf).expect("serde is infallible");
                PolyBytes::Both(typed, buf.split().freeze())
            }
        };
        Self {
            created_at,
            keys,
            body,
        }
    }

    pub fn body(&self) -> &PolyBytes {
        &self.body
    }

    pub fn dissolve(self) -> (NanosSinceEpoch, PolyBytes, Keys) {
        (self.created_at, self.body, self.keys)
    }

    /// Decode the record body into an owned value T.
    ///
    /// Internally, this will clone the inner value if it's already in record cache, or will move
    /// the value from the underlying Arc delivered from the loglet. Use this approach if you need
    /// to mutate the value in-place and the cost of cloning sections is high. It's generally
    /// recommended to use `decode_arc` whenever possible for large payloads.
    pub fn decode<T: StorageDecode + StorageEncode + Clone>(self) -> Result<T, StorageDecodeError> {
        let decoded = match self.body {
            PolyBytes::Bytes(slice) => {
                let mut buf = std::io::Cursor::new(slice);
                StorageCodec::decode(&mut buf)?
            }
            PolyBytes::Both(value, _) | PolyBytes::Typed(value) => {
                let target_arc: Arc<T> = value.downcast_arc().map_err(|_| {
                StorageDecodeError::DecodeValue(
                    anyhow::anyhow!(
                        "Type mismatch. Original value in PolyBytes::Typed does not match requested type"
                    )
                    .into(),
                )})?;
                // Attempts to move the inner value (T) if this Arc has exactly one strong
                // reference. Otherwise, it clones the inner value.
                match Arc::try_unwrap(target_arc) {
                    Ok(value) => value,
                    Err(value) => value.as_ref().clone(),
                }
            }
        };
        Ok(decoded)
    }

    /// Decode the record body into an Arc<T>. This is the most efficient way to access the entry
    /// if you need read-only access or if it's acceptable to selectively clone inner sections. If
    /// the record is in record cache, this will avoid cloning or deserialization of the value.
    pub fn decode_arc<T: StorageDecode + StorageEncode>(
        self,
    ) -> Result<Arc<T>, StorageDecodeError> {
        let decoded = match self.body {
            PolyBytes::Bytes(slice) => {
                let mut buf = std::io::Cursor::new(slice);
                Arc::new(StorageCodec::decode(&mut buf)?)
            }
            PolyBytes::Typed(value) | PolyBytes::Both(value, _) => {
                value.downcast_arc().map_err(|_| {
                StorageDecodeError::DecodeValue(
                    anyhow::anyhow!(
                        "Type mismatch. Original value in PolyBytes::Typed does not match requested type"
                    )
                    .into(),
                )})?
            },
        };
        Ok(decoded)
    }
}

impl EstimatedMemorySize for Record {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        self.body.estimated_memory_size()
    }
}

impl MatchKeyQuery for Record {
    fn matches_key_query(&self, query: &KeyFilter) -> bool {
        self.keys.matches_key_query(query)
    }
}

impl From<String> for Record {
    fn from(value: String) -> Self {
        Record {
            created_at: NanosSinceEpoch::now(),
            keys: Keys::None,
            body: PolyBytes::Typed(Arc::new(value)),
        }
    }
}

impl From<&str> for Record {
    fn from(value: &str) -> Self {
        Record {
            created_at: NanosSinceEpoch::now(),
            keys: Keys::None,
            body: PolyBytes::Typed(Arc::new(value.to_owned())),
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
impl From<(&str, Keys)> for Record {
    fn from((value, keys): (&str, Keys)) -> Self {
        Record::from_parts(
            NanosSinceEpoch::now(),
            keys,
            PolyBytes::Typed(Arc::new(value.to_owned())),
        )
    }
}

mod record_encoding {
    use bilrost::encoding::Proxiable;
    use bytes::{Bytes, BytesMut};

    use restate_clock::time::NanosSinceEpoch;

    use super::Record;
    use crate::{
        logs::Keys,
        storage::{PolyBytes, StorageCodec},
    };

    pub struct CustomRecordEncoding;

    bilrost::encoding_implemented_via_value_encoding!(CustomRecordEncoding);

    bilrost::encoding_uses_base_empty_state!(CustomRecordEncoding);

    struct RecordTag;

    #[derive(Debug, Clone, bilrost::Message)]
    struct EncodedRecord {
        #[bilrost(tag(1))]
        created_at: NanosSinceEpoch,
        #[bilrost(tag(2))]
        keys: Keys,
        #[bilrost(tag(3))]
        body: Bytes,
    }

    impl Proxiable<RecordTag> for Record {
        type Proxy = EncodedRecord;

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), bilrost::DecodeErrorKind> {
            *self = Self {
                created_at: proxy.created_at,
                keys: proxy.keys,
                body: PolyBytes::Bytes(proxy.body),
            };

            Ok(())
        }

        fn encode_proxy(&self) -> Self::Proxy {
            let body = match self.body() {
                PolyBytes::Bytes(bytes) => bytes.clone(),
                PolyBytes::Both(_, bytes) => bytes.clone(),
                PolyBytes::Typed(typed) => {
                    // It's recommended to call ensure_encoding ahead so we
                    // avoid buffer allocations here.

                    let mut buf = BytesMut::new();
                    StorageCodec::encode(typed.as_ref(), &mut buf).expect("serde is infallible");
                    buf.freeze()
                }
            };

            EncodedRecord {
                created_at: self.created_at,
                keys: self.keys.clone(),
                body: body.clone(),
            }
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::General)
        to encode proxied type (Record) using proxy tag (RecordTag)
        with encoding (CustomRecordEncoding)
    );

    #[cfg(test)]
    mod test {
        use bilrost::{Message, OwnedMessage};
        use restate_clock::time::NanosSinceEpoch;

        use super::CustomRecordEncoding;
        use crate::{
            logs::{Keys, Record, record::record_encoding::EncodedRecord},
            storage::PolyBytes,
        };

        #[derive(bilrost::Message)]
        struct ContainerWithCustomEncoder {
            #[bilrost(tag(1), encoding(CustomRecordEncoding))]
            inner: Record,
        }

        #[derive(bilrost::Message)]
        struct ContainerEncodedRecord {
            #[bilrost(tag(1))]
            inner: EncodedRecord,
        }

        #[test]
        fn wire_compatibility_smoke_test() {
            let inner = Record::from_parts(
                NanosSinceEpoch::now(),
                Keys::None,
                PolyBytes::Bytes("hello world".into()),
            );

            let container = ContainerWithCustomEncoder { inner };

            let data = container.encode_to_bytes();

            let decoded = ContainerEncodedRecord::decode(data).unwrap();

            assert!(
                matches!(container.inner.body(), PolyBytes::Bytes(bytes) if bytes == &decoded.inner.body)
            );
        }
    }
}
