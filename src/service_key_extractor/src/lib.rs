use arc_swap::{ArcSwap, Guard};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ServiceInstanceType {
    Keyed {
        /// The `key_structure` of the key field. Every method in a keyed service MUST have the same key type,
        /// hence the key structure is the same.
        key_structure: KeyStructure,
        /// Each method request message might represent the key with a different field number. E.g.
        ///
        /// ```protobuf
        /// message SayHelloRequest {
        ///   Person person = 1 [(dev.restate.ext.field) = KEY];
        /// }
        ///
        /// message SayByeRequest {
        ///   Person person = 2 [(dev.restate.ext.field) = KEY];
        /// }
        /// ```
        service_methods_key_field_root_number: HashMap<String, u32>,
    },
    Unkeyed,
    Singleton,
}

/// This structure provides the directives to the key parser to parse nested messages.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum KeyStructure {
    Scalar,
    Nested(BTreeMap<u32, KeyStructure>),
}

/// A key extractor provides the logic to extract a key out of a request payload.
pub trait KeyExtractor {
    /// Extract performs key extraction from a request payload, returning the key in a Restate internal format.
    ///
    /// To perform the inverse operation, check the [`KeyExpander`] trait.
    fn extract(
        &self,
        service_name: impl AsRef<str>,
        service_method: impl AsRef<str>,
        payload: Bytes,
    ) -> Result<Bytes, Error>;
}

/// A key expander provides the inverse function of a [`KeyExtractor`].
#[cfg(feature = "expand")]
pub trait KeyExpander {
    /// Expand takes a Restate key and assigns it to the key field of a [`prost_reflect::DynamicMessage`] generated from the given `descriptor`.
    ///
    /// The provided [`descriptor`] MUST be the same descriptor of the request message of the given `service_name` and `service_method`.
    ///
    /// The result of this method is a message matching the provided `descriptor` with only the key field filled.
    ///
    /// This message can be mapped back and forth to JSON using `prost-reflect` `serde` feature.
    fn expand(
        &self,
        service_name: impl AsRef<str>,
        service_method: impl AsRef<str>,
        descriptor: prost_reflect::MessageDescriptor,
        key: Bytes,
    ) -> Result<prost_reflect::DynamicMessage, Error>;
}

/// This struct holds the key extractors for each known method of each known service.
#[derive(Default, Debug, Clone)]
pub struct KeyExtractorsRegistry {
    services: Arc<ArcSwap<HashMap<String, ServiceInstanceType>>>,
}

impl KeyExtractorsRegistry {
    pub fn register(&self, name: String, instance_type: ServiceInstanceType) {
        let services = self.services.load();

        let mut new_services = HashMap::clone(&services);
        new_services.insert(name, instance_type);

        self.services.store(Arc::new(new_services));
    }

    pub fn remove(&self, name: impl AsRef<str>) {
        let services = self.services.load();

        let mut new_services = HashMap::clone(&services);
        new_services.remove(name.as_ref());

        self.services.store(Arc::new(new_services));
    }

    fn resolve_instance_type(
        services: &Guard<Arc<HashMap<String, ServiceInstanceType>>>,
        service_name: impl AsRef<str>,
    ) -> Result<&ServiceInstanceType, Error> {
        let service_instance_type = services
            .get(service_name.as_ref())
            .ok_or_else(|| Error::NotFound)?;

        Ok(service_instance_type)
    }
}

impl KeyExtractor for KeyExtractorsRegistry {
    fn extract(
        &self,
        service_name: impl AsRef<str>,
        service_method: impl AsRef<str>,
        payload: Bytes,
    ) -> Result<Bytes, Error> {
        let services = self.services.load();
        let service_instance_type =
            KeyExtractorsRegistry::resolve_instance_type(&services, service_name.as_ref())?;
        service_instance_type.extract(service_name, service_method.as_ref(), payload)
    }
}

#[cfg(feature = "expand")]
impl KeyExpander for KeyExtractorsRegistry {
    fn expand(
        &self,
        service_name: impl AsRef<str>,
        service_method: impl AsRef<str>,
        descriptor: prost_reflect::MessageDescriptor,
        key: Bytes,
    ) -> Result<prost_reflect::DynamicMessage, Error> {
        let services = self.services.load();
        let service_instance_type =
            KeyExtractorsRegistry::resolve_instance_type(&services, service_name.as_ref())?;
        service_instance_type.expand(service_name, service_method.as_ref(), descriptor, key)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unexpected end of buffer when decoding")]
    UnexpectedEndOfBuffer,
    #[error("unexpected value when parsing the payload. It looks like the message schema and the parser directives don't match")]
    UnexpectedValue,
    #[error("error when decoding the payload to extract the message: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("cannot resolve key extractor")]
    NotFound,
    #[cfg(feature = "expand")]
    #[error("unexpected service instance type to expand the key. Only keys of keyed services can be expanded")]
    UnexpectedServiceInstanceType,
}

mod extract_impls {
    use super::*;

    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use prost::encoding::WireType::*;
    use prost::encoding::{decode_key, decode_varint, skip_field, DecodeContext, WireType};
    use uuid::Uuid;

    fn generate_random_key() -> Bytes {
        Bytes::copy_from_slice(Uuid::now_v7().as_bytes())
    }

    impl KeyExtractor for ServiceInstanceType {
        fn extract(
            &self,
            _service_name: impl AsRef<str>,
            service_method: impl AsRef<str>,
            payload: Bytes,
        ) -> Result<Bytes, Error> {
            match self {
                ServiceInstanceType::Unkeyed => Ok(generate_random_key()),
                ServiceInstanceType::Singleton => Ok(Bytes::default()),
                ServiceInstanceType::Keyed {
                    key_structure,
                    service_methods_key_field_root_number,
                } => root_extract(
                    payload,
                    *service_methods_key_field_root_number
                        .get(service_method.as_ref())
                        .ok_or_else(|| Error::NotFound)?,
                    key_structure,
                ),
            }
        }
    }

    /// This is the start of the key extraction algorithm
    fn root_extract(
        mut buf: Bytes,
        root_key_field_number: u32,
        key_structure: &KeyStructure,
    ) -> Result<Bytes, Error> {
        // Look for the key tag first
        let root_key_field_wire_type = advance_to_field(&mut buf, root_key_field_number)?;

        // No key, we just return empty buffer
        if root_key_field_wire_type.is_none() {
            return Ok(Bytes::new());
        }

        // Start recursive extract
        deep_extract(&mut buf, root_key_field_wire_type.unwrap(), key_structure)
    }

    /// This will move the buffer up to the beginning of the target field.
    fn advance_to_field<B: Buf>(
        buf: &mut B,
        target_field_number: u32,
    ) -> Result<Option<WireType>, Error> {
        while buf.has_remaining() {
            let (field_number, field_wire_type) = decode_key(buf)?;
            if field_number == target_field_number {
                return Ok(Some(field_wire_type));
            }

            skip_field(field_wire_type, field_number, buf, DecodeContext::default())?;
        }
        Ok(None)
    }

    fn check_remaining<B: Buf>(buf: &B, len: usize) -> Result<(), Error> {
        if buf.remaining() < len {
            Err(Error::UnexpectedEndOfBuffer)
        } else {
            Ok(())
        }
    }

    fn deep_extract(
        buf: &mut Bytes,
        current_wire_type: WireType,
        current_parser_directive: &KeyStructure,
    ) -> Result<Bytes, Error> {
        let mut result_buf = BytesMut::new();

        match (current_wire_type, current_parser_directive) {
            // Primitive cases
            (Varint, _) => result_buf.put(slice_varint_bytes(buf)?),
            (ThirtyTwoBit, _) => result_buf.put(slice_const_bytes(buf, 4)?),
            (SixtyFourBit, _) => result_buf.put(slice_const_bytes(buf, 8)?),
            (LengthDelimited, KeyStructure::Scalar) => {
                result_buf.put(slice_length_delimited_bytes(buf)?)
            }

            // Composite cases
            (StartGroup, KeyStructure::Nested(expected_message_fields)) => {
                let mut message_fields = HashMap::with_capacity(expected_message_fields.len());
                loop {
                    let (next_field_number, next_wire_type) = decode_key(buf)?;
                    if next_wire_type == EndGroup {
                        break;
                    }
                    match expected_message_fields.get(&next_field_number) {
                        None => {
                            // Unknown field, just skip it
                            skip_field(
                                next_wire_type,
                                next_field_number,
                                buf,
                                DecodeContext::default(),
                            )?;
                            continue;
                        }
                        Some(next_parser_directive) => {
                            message_fields.insert(
                                next_field_number,
                                deep_extract(buf, next_wire_type, next_parser_directive)?,
                            );
                        }
                    };
                }
                expected_message_fields
                    .keys()
                    .map(|k| {
                        message_fields
                            .remove(k)
                            // Ensure we have defaulting
                            .unwrap_or_default()
                    })
                    .for_each(|b| result_buf.put(b));
            }

            (LengthDelimited, KeyStructure::Nested(expected_message_fields)) => {
                let mut message_fields = HashMap::with_capacity(expected_message_fields.len());
                let inner_message_len = decode_varint(buf)? as usize;
                let mut current_buf = buf.split_to(inner_message_len);
                while current_buf.has_remaining() {
                    let (next_field_number, next_wire_type) = decode_key(&mut current_buf)?;
                    match expected_message_fields.get(&next_field_number) {
                        None => {
                            // Unknown field, just skip it
                            skip_field(
                                next_wire_type,
                                next_field_number,
                                &mut current_buf,
                                DecodeContext::default(),
                            )?;
                        }
                        Some(next_parser_directive) => {
                            message_fields.insert(
                                next_field_number,
                                deep_extract(
                                    &mut current_buf,
                                    next_wire_type,
                                    next_parser_directive,
                                )?,
                            );
                        }
                    };
                }

                expected_message_fields
                    .keys()
                    .map(|k| {
                        message_fields
                            .remove(k)
                            // Ensure we have defaulting
                            .unwrap_or_default()
                    })
                    .for_each(|b| result_buf.put(b));
            }

            // Expecting a primitive message, but got composite -> schema mismatch
            (StartGroup | EndGroup, KeyStructure::Scalar) => return Err(Error::UnexpectedValue),
            // EndGroup is handled by the loop below, so we're not supposed to have a match here
            (EndGroup, _) => return Err(Error::UnexpectedValue),
        };

        Ok(result_buf.freeze())
    }

    /// This behaves similarly to [decode_varint], but without parsing the number, but simply returning the bytes composing it.
    fn slice_varint_bytes(buf: &mut Bytes) -> Result<Bytes, Error> {
        let len = buf.len();
        if len == 0 {
            return Err(Error::UnexpectedEndOfBuffer);
        }

        let mut scanned_bytes = 0;
        let mut end_byte_reached = false;
        while scanned_bytes < len {
            if buf[scanned_bytes] < 0x80 {
                // MSB == 1 means more bytes, == 0 means last byte
                end_byte_reached = true;
                break;
            }
            scanned_bytes += 1;
        }
        if end_byte_reached {
            let res = buf.split_to(scanned_bytes + 1);

            return Ok(res);
        }

        Err(Error::UnexpectedEndOfBuffer)
    }

    fn slice_const_bytes(buf: &mut Bytes, len: usize) -> Result<Bytes, Error> {
        check_remaining(buf, len)?;
        let res = buf.split_to(len);

        Ok(res)
    }

    fn slice_length_delimited_bytes(buf: &mut Bytes) -> Result<Bytes, Error> {
        let length = decode_varint(buf)?;

        slice_const_bytes(buf, length as usize)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        use prost::encoding::{
            encode_key, encode_varint, encoded_len_varint, key_len, DecodeContext,
        };
        use prost::{length_delimiter_len, DecodeError, Message};
        use std::collections::BTreeMap;
        use std::fmt::Write;

        #[derive(Debug)]
        struct MockMessage {
            a: String,
            b: String,
            c: String,
            ordered_encoding: bool,
            nested: bool,
            unknown_field: bool,
        }

        impl Default for MockMessage {
            fn default() -> Self {
                Self {
                    a: "Alfa".to_string(),
                    b: "Beta".to_string(),
                    c: "Delta".to_string(),
                    ordered_encoding: true,
                    nested: false,
                    unknown_field: false,
                }
            }
        }

        impl MockMessage {
            fn fill_expected_buf(&self, buf: &mut BytesMut) {
                buf.write_str(&self.a).unwrap();
                buf.write_str(&self.b).unwrap();
                buf.write_str(&self.c).unwrap();
            }

            fn parser_directive(nested: bool) -> KeyStructure {
                if nested {
                    KeyStructure::Nested(BTreeMap::from([
                        (
                            1,
                            KeyStructure::Nested(BTreeMap::from([(2, KeyStructure::Scalar)])),
                        ),
                        (
                            2,
                            KeyStructure::Nested(BTreeMap::from([(1, KeyStructure::Scalar)])),
                        ),
                        (3, KeyStructure::Scalar),
                    ]))
                } else {
                    KeyStructure::Nested(BTreeMap::from([
                        (1, KeyStructure::Scalar),
                        (2, KeyStructure::Scalar),
                        (3, KeyStructure::Scalar),
                    ]))
                }
            }

            fn write_a<B: BufMut>(&self, buf: &mut B) {
                if self.nested {
                    // Encode a as nested message
                    encode_key(1, LengthDelimited, buf);
                    encode_varint(
                        key_len(2) as u64
                            + encoded_len_varint(self.a.len() as u64) as u64
                            + self.a.len() as u64,
                        buf,
                    );
                    // Nested message
                    encode_key(2, LengthDelimited, buf);
                    encode_varint(self.a.len() as u64, buf);
                    buf.put_slice(self.a.as_bytes());
                } else {
                    prost::encoding::string::encode(1, &self.a, buf);
                }
            }

            fn write_b<B: BufMut>(&self, buf: &mut B) {
                if self.nested {
                    // Encode b as nested group
                    encode_key(2, StartGroup, buf);
                    encode_key(1, LengthDelimited, buf);
                    encode_varint(self.b.len() as u64, buf);
                    buf.put_slice(self.b.as_bytes());
                    encode_key(2, EndGroup, buf);
                } else {
                    prost::encoding::string::encode(2, &self.b, buf);
                }
            }

            fn write_c<B: BufMut>(&self, buf: &mut B) {
                prost::encoding::string::encode(3, &self.c, buf);
            }

            fn write_unknown_field<B: BufMut>(&self, buf: &mut B) {
                if self.unknown_field {
                    prost::encoding::string::encode(10, &self.c, buf);
                }
            }
        }

        impl Message for MockMessage {
            fn encode_raw<B>(&self, buf: &mut B)
            where
                B: BufMut,
                Self: Sized,
            {
                if self.ordered_encoding {
                    self.write_a(buf);
                    self.write_b(buf);
                    self.write_c(buf);
                    self.write_unknown_field(buf);
                } else {
                    self.write_c(buf);
                    self.write_a(buf);
                    self.write_unknown_field(buf);
                    self.write_b(buf);
                }
            }

            fn merge_field<B>(
                &mut self,
                _tag: u32,
                _wire_type: WireType,
                _buf: &mut B,
                _ctx: DecodeContext,
            ) -> Result<(), DecodeError>
            where
                B: Buf,
                Self: Sized,
            {
                unimplemented!()
            }

            fn encoded_len(&self) -> usize {
                let mut strings_len = prost::encoding::string::encoded_len(1, &self.a)
                    + prost::encoding::string::encoded_len(2, &self.b)
                    + prost::encoding::string::encoded_len(3, &self.c);

                if self.unknown_field {
                    strings_len += prost::encoding::string::encoded_len(10, &self.c);
                }

                if self.nested {
                    strings_len +=
                        // Group needs two more keys
                        (2 * key_len(2))
                        // Length encoded Message needs one key + 1 varint for the length
                        + key_len(1) +
                            length_delimiter_len(key_len(2) + length_delimiter_len(self.a.len()) + self.a.len());
                }
                strings_len
            }

            fn clear(&mut self) {
                unimplemented!()
            }
        }

        // This macro generates the various test cases
        macro_rules! extract_tests {
            ($typ:ident, item: $val:expr, fill_expected_buf: $fill_expected_buf_fn:expr) => {
                extract_tests!($typ, mod: $typ, item: $val, fill_expected_buf: $fill_expected_buf_fn, parser: KeyStructure::Scalar);
            };
            ($typ:ident, item: $val:expr, fill_expected_buf: $fill_expected_buf_fn:expr, parser: $parser_directive:expr) => {
                extract_tests!($typ, mod: $typ, item: $val, fill_expected_buf: $fill_expected_buf_fn, parser: $parser_directive);
            };
            ($typ:ident, mod: $mod:ident, item: $val:expr, fill_expected_buf: $fill_expected_buf_fn:expr, parser: $parser_directive:expr) => {
                mod $mod {
                    use super::*;

                    #[test]
                    fn extract_mixed() {
                        let mut input_buf = BytesMut::new();
                        // These are not part of the key
                        prost::encoding::uint32::encode(1, &(352890234 as u32), &mut input_buf);
                        prost::encoding::group::encode(4, &MockMessage::default(), &mut input_buf);
                        prost::encoding::float::encode(6, &(4543.342 as f32), &mut input_buf);
                        prost::encoding::message::encode(
                            7,
                            &MockMessage::default(),
                            &mut input_buf,
                        );
                        prost::encoding::string::encode(5, &"my str".to_string(), &mut input_buf);

                        // The test value to encode
                        prost::encoding::$typ::encode(3, &($val), &mut input_buf);

                        // Another value not part of the key
                        prost::encoding::string::encode(
                            2,
                            &"my other str".to_string(),
                            &mut input_buf,
                        );

                        let mut expected_buf = BytesMut::new();
                        $fill_expected_buf_fn(&mut expected_buf, $val);

                        assert_eq!(
                            root_extract(input_buf.freeze(), 3, &$parser_directive).unwrap(),
                            expected_buf
                        );
                    }

                    #[test]
                    fn extract_first_field() {
                        let mut input_buf = BytesMut::new();
                        prost::encoding::$typ::encode(3, &($val), &mut input_buf);
                        prost::encoding::string::encode(
                            2,
                            &"my other str".to_string(),
                            &mut input_buf,
                        ); // This is not part of the key

                        let mut expected_buf = BytesMut::new();
                        $fill_expected_buf_fn(&mut expected_buf, $val);

                        assert_eq!(
                            root_extract(input_buf.freeze(), 3, &$parser_directive).unwrap(),
                            expected_buf
                        );
                    }

                    #[test]
                    fn extract_last_field() {
                        let mut input_buf = BytesMut::new();
                        prost::encoding::string::encode(5, &"my str".to_string(), &mut input_buf); // This is not part of the key
                        prost::encoding::$typ::encode(3, &($val), &mut input_buf);

                        let mut expected_buf = BytesMut::new();
                        $fill_expected_buf_fn(&mut expected_buf, $val);

                        assert_eq!(
                            root_extract(input_buf.freeze(), 3, &$parser_directive).unwrap(),
                            expected_buf
                        );
                    }

                    #[test]
                    fn extract_only_field() {
                        let mut input_buf = BytesMut::new();
                        prost::encoding::$typ::encode(3, &($val), &mut input_buf);

                        let mut expected_buf = BytesMut::new();
                        $fill_expected_buf_fn(&mut expected_buf, $val);

                        assert_eq!(
                            root_extract(input_buf.freeze(), 3, &$parser_directive).unwrap(),
                            expected_buf
                        );
                    }
                }
            };
        }

        // Note: The encoding from rust types to protobuf has been taken directly from
        // https://github.com/tokio-rs/prost/blob/master/src/encoding.rs

        // Test single varint size type
        extract_tests!(
            bool,
            item: true,
            fill_expected_buf: |buf: &mut BytesMut, val| encode_varint(if val { 1u64 } else { 0u64 }, buf)
        );
        extract_tests!(
            int32,
            item: -21314_i32,
            fill_expected_buf: |buf: &mut BytesMut, val| encode_varint(val as u64, buf)
        );
        extract_tests!(
            int64,
            item: -245361314_i64,
            fill_expected_buf: |buf: &mut BytesMut, val| encode_varint(val as u64, buf)
        );
        extract_tests!(
            uint32,
            item: 21314_u32,
            fill_expected_buf: |buf: &mut BytesMut, val| encode_varint(val as u64, buf)
        );
        extract_tests!(
            uint64,
            item: 245361314_u64,
            fill_expected_buf: |buf: &mut BytesMut, val: u64| encode_varint(val, buf)
        );
        extract_tests!(
            sint32,
            item: -21314_i32,
            fill_expected_buf: |buf: &mut BytesMut, val| encode_varint(((val << 1) ^ (val >> 31)) as u32 as u64, buf)
        );
        extract_tests!(
            sint64,
            item: -245361314_i64,
            fill_expected_buf: |buf: &mut BytesMut, val| encode_varint(((val << 1) ^ (val >> 63)) as u64, buf)
        );

        // Test single 32/64 const size type
        extract_tests!(
            float,
            item: 4543.342_f32,
            fill_expected_buf: |buf: &mut BytesMut, val| buf.put_f32_le(val)
        );
        extract_tests!(
            double,
            item: 4543986.342542_f64,
            fill_expected_buf: |buf: &mut BytesMut, val| buf.put_f64_le(val)
        );
        extract_tests!(
            fixed32,
            item: 4543_u32,
            fill_expected_buf: |buf: &mut BytesMut, val| buf.put_u32_le(val)
        );
        extract_tests!(
            fixed64,
            item: 349320_u64,
            fill_expected_buf: |buf: &mut BytesMut, val| buf.put_u64_le(val)
        );
        extract_tests!(
            sfixed32,
            item: -4543_i32,
            fill_expected_buf: |buf: &mut BytesMut, val| buf.put_i32_le(val)
        );
        extract_tests!(
            sfixed64,
            item: -349320_i64,
            fill_expected_buf: |buf: &mut BytesMut, val| buf.put_i64_le(val)
        );

        // Test single length delimited type
        extract_tests!(
            string,
            item: "my awesome string".to_string(),
            fill_expected_buf: |buf: &mut BytesMut, val: String| buf.put_slice(val.as_bytes())
        );
        extract_tests!(
            bytes,
            item: Bytes::from_static(&[1_u8, 2, 3]),
            fill_expected_buf: |buf: &mut BytesMut, val: Bytes| buf.put_slice(&val)
        );

        // Test message
        // Note: the difference between message and group is that
        // the former encodes using length delimited message encoding,
        // while the latter encodes using the [Start/End]Group markers
        extract_tests!(
            message,
            item: MockMessage::default(),
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(false)
        );
        extract_tests!(
            message,
            mod: message_reverse,
            item: MockMessage {
                ordered_encoding: false,
                ..MockMessage::default()
            },
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(false)
        );
        extract_tests!(
            group,
            item: MockMessage::default(),
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(false)
        );
        extract_tests!(
            group,
            mod: group_reverse,
            item: MockMessage {
                ordered_encoding: false,
                ..MockMessage::default()
            },
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(false)
        );
        extract_tests!(
            message,
            mod: message_nested,
            item: MockMessage {
                nested: true,
                ..MockMessage::default()
            },
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(true)
        );
        extract_tests!(
            message,
            mod: message_nested_reverse,
            item: MockMessage {
                nested: true,
                ordered_encoding: false,
                ..MockMessage::default()
            },
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(true)
        );
        extract_tests!(
            group,
            mod: group_nested,
            item: MockMessage {
                nested: true,
                ..MockMessage::default()
            },
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(true)
        );
        extract_tests!(
            group,
            mod: group_nested_reverse,
            item: MockMessage {
                nested: true,
                ordered_encoding: false,
                ..MockMessage::default()
            },
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(true)
        );

        // Tests with unknown field
        extract_tests!(
            message,
            mod: message_unknown,
            item: MockMessage {
                unknown_field: true,
                ..MockMessage::default()
            },
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(false)
        );
        extract_tests!(
            group,
            mod: group_unknown,
            item: MockMessage {
                unknown_field: true,
                ..MockMessage::default()
            },
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(false)
        );
        extract_tests!(
            message,
            mod: message_reverse_unknown,
            item: MockMessage {
                ordered_encoding: false,
                unknown_field: true,
                ..MockMessage::default()
            },
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(false)
        );
        extract_tests!(
            group,
            mod: group_reverse_unknown,
            item: MockMessage {
                ordered_encoding: false,
                unknown_field: true,
                ..MockMessage::default()
            },
            fill_expected_buf: |buf: &mut BytesMut, val: MockMessage| val.fill_expected_buf(buf),
            parser: MockMessage::parser_directive(false)
        );

        // Additional tests
        #[test]
        fn default_key() {
            let mut input_buf = BytesMut::new();
            // None of these are the key
            prost::encoding::string::encode(5, &"my str".to_string(), &mut input_buf);
            prost::encoding::string::encode(2, &"my other str".to_string(), &mut input_buf);

            assert_eq!(
                root_extract(input_buf.freeze(), 1, &KeyStructure::Scalar).unwrap(),
                Bytes::new()
            );
        }

        #[test]
        fn empty_message() {
            let input_buf = Bytes::new();

            assert_eq!(
                root_extract(input_buf, 1, &KeyStructure::Scalar).unwrap(),
                Bytes::new()
            );
        }
    }
}

#[cfg(feature = "expand")]
mod expand_impls {
    use super::*;
    use bytes::{BufMut, BytesMut};

    use prost::encoding::{encode_key, key_len, WireType};
    use prost_reflect::{DynamicMessage, Kind, MessageDescriptor};

    impl KeyExpander for ServiceInstanceType {
        fn expand(
            &self,
            _service_name: impl AsRef<str>,
            service_method: impl AsRef<str>,
            descriptor: MessageDescriptor,
            restate_key: Bytes,
        ) -> Result<DynamicMessage, Error> {
            if let ServiceInstanceType::Keyed {
                service_methods_key_field_root_number,
                ..
            } = self
            {
                // Find out the root field number and kind
                let root_number = *service_methods_key_field_root_number
                    .get(service_method.as_ref())
                    .ok_or(Error::NotFound)?;
                let field_descriptor_kind = descriptor
                    .get_field(root_number)
                    .ok_or(Error::NotFound)?
                    .kind();

                // Prepare the buffer for the protobuf
                let mut b = BytesMut::with_capacity(key_len(root_number) + restate_key.len());

                // Encode the key of the protobuf field
                encode_key(
                    root_number,
                    kind_to_wire_type(&field_descriptor_kind),
                    &mut b,
                );

                // Append the restate key buffer
                b.put(restate_key);

                // Now this message should be a well formed protobuf message, we can create the DynamicMessage
                Ok(DynamicMessage::decode(descriptor, b.freeze())?)
            } else {
                return Err(Error::UnexpectedServiceInstanceType);
            }
        }
    }

    // Function took from https://github.com/andrewhickman/prost-reflect/blob/a3a8e9cc2373b9b090781f277ba9068c064504d5/prost-reflect/src/descriptor/api.rs#L94
    // Double license Apache-1.0 and MIT
    fn kind_to_wire_type(kind: &Kind) -> WireType {
        match kind {
            Kind::Double | Kind::Fixed64 | Kind::Sfixed64 => WireType::SixtyFourBit,
            Kind::Float | Kind::Fixed32 | Kind::Sfixed32 => WireType::ThirtyTwoBit,
            Kind::Enum(_)
            | Kind::Int32
            | Kind::Int64
            | Kind::Uint32
            | Kind::Uint64
            | Kind::Sint32
            | Kind::Sint64
            | Kind::Bool => WireType::Varint,
            Kind::String | Kind::Bytes | Kind::Message(_) => WireType::LengthDelimited,
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        use prost::Message;
        use prost_reflect::DescriptorPool;

        mod pb {
            #![allow(warnings)]
            #![allow(clippy::all)]
            #![allow(unknown_lints)]
            include!(concat!(env!("OUT_DIR"), "/test.rs"));
        }
        use pb::*;

        static DESCRIPTOR: &[u8] =
            include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
        static METHOD_NAME: &str = "test";

        fn test_descriptor_message() -> MessageDescriptor {
            DescriptorPool::decode(DESCRIPTOR)
                .unwrap()
                .get_message_by_name("test.TestMessage")
                .unwrap()
        }

        fn test_message() -> TestMessage {
            TestMessage {
                string: "my_string".to_string(),
                bytes: b"my_bytes".to_vec().into(),
                number: 5,
                nested_message: Some(NestedKey {
                    a: "A".to_string(),
                    b: "B".to_string(),
                    c: 10,
                    ..Default::default()
                }),
            }
        }

        fn nested_key_structure() -> KeyStructure {
            KeyStructure::Nested(BTreeMap::from([
                (1, KeyStructure::Scalar),
                (2, KeyStructure::Scalar),
                (3, KeyStructure::Scalar),
                (
                    4,
                    KeyStructure::Nested(BTreeMap::from([(1, KeyStructure::Scalar)])),
                ),
            ]))
        }

        fn mock_keyed_service_instance_type(
            key_structure: KeyStructure,
            field_number: u32,
        ) -> ServiceInstanceType {
            ServiceInstanceType::Keyed {
                key_structure,
                service_methods_key_field_root_number: HashMap::from([(
                    METHOD_NAME.to_string(),
                    field_number,
                )]),
            }
        }

        // This macro generates test cases for the above TestMessage that extract the key and then expand it.
        // $field_name indicate which field to use as key.
        // $key_structure specifies the KeyStructure
        // The third variant of the macro allows to specify both test name and test message
        macro_rules! expand_tests {
            ($field_name:ident) => {
                expand_tests!($field_name, KeyStructure::Scalar);
            };
            ($field_name:ident, $key_structure:expr) => {
                expand_tests!(
                    test: $field_name,
                    field_name: $field_name,
                    key_structure: $key_structure,
                    test_message: test_message()
                );
            };
            (test: $test_name:ident, field_name: $field_name:ident, key_structure: $key_structure:expr, test_message: $test_message:expr) => {
                mod $test_name {
                    use super::*;

                    #[test]
                    fn expand() {
                        let test_descriptor_message = test_descriptor_message();
                        let field_number = test_descriptor_message
                            .get_field_by_name(stringify!($field_name))
                            .expect("Field should exist")
                            .number();

                        // Create test message and service instance type
                        let test_message = $test_message;
                        let service_instance_type =
                            mock_keyed_service_instance_type($key_structure, field_number);

                        // Extract the restate key
                        let restate_key = service_instance_type
                            .extract("", METHOD_NAME, test_message.encode_to_vec().into())
                            .expect("successful key extraction");

                        // Now expand the key again into a message
                        let expanded_message = service_instance_type
                            .expand("", METHOD_NAME, test_descriptor_message, restate_key)
                            .expect("successful key expansion");

                        // Transcode back to original message
                        let test_message_expanded = expanded_message
                            .transcode_to::<TestMessage>()
                            .expect("transcoding to TestMessage");

                        // Assert expanded field is equal to the one from the original message
                        assert_eq!(test_message_expanded.$field_name, test_message.$field_name)
                    }
                }
            };
        }

        expand_tests!(string);
        expand_tests!(bytes);
        expand_tests!(number);
        expand_tests!(nested_message, nested_key_structure());
        expand_tests!(
            test: nested_message_with_default,
            field_name: nested_message,
            key_structure: nested_key_structure(),
            test_message: TestMessage {
                nested_message: Some(NestedKey {
                    b: "b".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }
        );
        expand_tests!(
            test: double_nested_message,
            field_name: nested_message,
            key_structure: nested_key_structure(),
            test_message: TestMessage {
                nested_message: Some(NestedKey {
                    b: "b".to_string(),
                    other: Some(OtherMessage {
                        d: "d".to_string()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_add_and_remove_key_extractor() {
        let registry = KeyExtractorsRegistry::default();

        registry.register("MySvc".to_string(), ServiceInstanceType::Unkeyed);

        assert!(matches!(
            registry.extract("MySvc", "MyMethod", Bytes::new()),
            Ok(_)
        ));

        registry.remove("MySvc");

        assert!(matches!(
            registry.extract("MySvc", "MyMethod", Bytes::new()),
            Err(Error::NotFound)
        ));
    }

    #[test]
    fn singleton_key_extractor_always_return_same_key() {
        let registry = KeyExtractorsRegistry::default();

        registry.register("MySvc".to_string(), ServiceInstanceType::Singleton);

        assert_eq!(
            registry
                .extract("MySvc", "MyMethod", Bytes::copy_from_slice(&[1, 2, 3, 4]))
                .unwrap(),
            registry
                .extract("MySvc", "MyMethod", Bytes::copy_from_slice(&[5, 6, 7, 8]))
                .unwrap()
        );

        assert_eq!(
            registry
                .extract("MySvc", "MyMethod", Bytes::copy_from_slice(&[1, 2, 3, 4]))
                .unwrap(),
            registry
                .extract(
                    "MySvc",
                    "OtherMethod",
                    Bytes::copy_from_slice(&[1, 2, 3, 4])
                )
                .unwrap()
        );
    }
}
