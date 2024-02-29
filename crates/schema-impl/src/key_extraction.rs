// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{KeyStructure, Schemas};

use bytes::Bytes;
use restate_schema_api::key::extraction::Error;
use restate_schema_api::key::KeyExtractor;

impl KeyExtractor for Schemas {
    fn extract(
        &self,
        service_name: impl AsRef<str>,
        service_method: impl AsRef<str>,
        payload: Bytes,
    ) -> Result<Bytes, Error> {
        self.use_service_schema(service_name, |schemas| {
            extract_impls::extract(&schemas.instance_type, service_method, payload)
        })
        .ok_or(Error::NotFound)?
    }
}

pub(crate) mod extract_impls {
    use super::*;

    use crate::schemas_impl::InstanceTypeMetadata;
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use prost::encoding::WireType::*;
    use prost::encoding::{
        decode_key, decode_varint, encode_key, skip_field, DecodeContext, WireType,
    };
    use uuid::Uuid;

    fn generate_random_key() -> Bytes {
        Bytes::copy_from_slice(Uuid::now_v7().to_string().as_ref())
    }

    pub(crate) fn extract(
        service_instance_type: &InstanceTypeMetadata,
        service_method: impl AsRef<str>,
        payload: Bytes,
    ) -> Result<Bytes, Error> {
        match service_instance_type {
            InstanceTypeMetadata::Unkeyed => Ok(generate_random_key()),
            InstanceTypeMetadata::Singleton => Ok(Bytes::default()),
            InstanceTypeMetadata::Keyed {
                key_structure,
                service_methods_key_field_root_number,
            } => root_extract(
                payload,
                *service_methods_key_field_root_number
                    .get(service_method.as_ref())
                    .ok_or_else(|| Error::NotFound)?,
                key_structure,
            ),
            InstanceTypeMetadata::Custom {
                structure_per_method,
            } => {
                if let Some((root_key_field_number, structure)) =
                    structure_per_method.get(service_method.as_ref())
                {
                    root_extract(payload, *root_key_field_number, structure)
                } else {
                    Ok(generate_random_key())
                }
            }
        }
    }

    /// This is the start of the key extraction algorithm.
    ///
    /// ## Restate key representation:
    ///
    /// This function manipulates the input message as follows:
    ///
    /// 1. Skips all the fields of the input message until it reaches the key field.
    /// 2. If the field is scalar, slice it and return it.
    /// 3. If the field is a message, this algorithm traverses the field making sure
    ///    the final representation has the fields ordered.
    ///
    /// The final slice won't contain the key (tag and wire_type tuple) of the root field of the key,
    /// as it can be different across methods, but the rest of the field will be encoded as regular protobuf.
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
        deep_extract(
            &mut buf,
            root_key_field_number,
            root_key_field_wire_type.unwrap(),
            key_structure,
            // We don't write the key for the root field,
            // as it might be different across request messages.
            false,
        )
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
        current_field_number: u32,
        current_wire_type: WireType,
        current_parser_directive: &KeyStructure,
        should_write_key: bool,
    ) -> Result<Bytes, Error> {
        let mut result_buf = BytesMut::new();

        if should_write_key {
            encode_key(
                current_field_number,
                match current_wire_type {
                    // We convert group fields to length delimited,
                    // check below the StartGroup/Nested match arm
                    StartGroup => LengthDelimited,
                    wt => wt,
                },
                &mut result_buf,
            );
        }

        match (current_wire_type, current_parser_directive) {
            // Primitive cases
            (LengthDelimited, KeyStructure::Scalar) => {
                let (_length, field_slice) = slice_length_delimited_bytes(buf)?;
                result_buf.put(field_slice)
            }
            // Expecting a string key, but got something else -> schema mismatch
            (_, KeyStructure::Scalar) => return Err(Error::UnexpectedValue),
            (_, _) => panic!(
                "Unsupported key extraction. See https://github.com/restatedev/restate/issues/955"
            ),
        };

        Ok(result_buf.freeze())
    }

    fn slice_const_bytes(buf: &mut Bytes, len: usize) -> Result<Bytes, Error> {
        check_remaining(buf, len)?;
        let res = buf.split_to(len);

        Ok(res)
    }

    fn slice_length_delimited_bytes(buf: &mut Bytes) -> Result<(u64, Bytes), Error> {
        let length = decode_varint(buf)?;

        Ok((length, slice_const_bytes(buf, length as usize)?))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        use prost::encoding::{encode_key, encode_varint, key_len, DecodeContext};
        use prost::{length_delimiter_len, DecodeError, Message};

        #[derive(Debug)]
        struct MockMessage {
            a: String,
            b: String,
            c: String,
            ordered_encoding: bool,
            nested: bool,
            unknown_field: bool,
            skip_b: bool,
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
                    skip_b: false,
                }
            }
        }

        impl MockMessage {
            fn write_a<B: BufMut>(&self, buf: &mut B) {
                if self.nested {
                    Self::write_in_length_delimited(1, &self.a, buf);
                } else {
                    prost::encoding::string::encode(1, &self.a, buf);
                }
            }

            fn write_b<B: BufMut>(&self, nested_as_group: bool, buf: &mut B) {
                if self.skip_b {
                    return;
                }
                if self.nested {
                    if nested_as_group {
                        Self::write_in_nested_group(2, &self.b, buf);
                    } else {
                        Self::write_in_length_delimited(2, &self.b, buf);
                    }
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

            fn write_in_length_delimited<B: BufMut>(tag: u32, str: &String, buf: &mut B) {
                encode_key(tag, LengthDelimited, buf);
                encode_varint(prost::encoding::string::encoded_len(1, str) as u64, buf);
                prost::encoding::string::encode(1, str, buf);
            }

            fn write_in_nested_group<B: BufMut>(tag: u32, str: &String, buf: &mut B) {
                encode_key(tag, StartGroup, buf);
                prost::encoding::string::encode(1, str, buf);
                encode_key(tag, EndGroup, buf);
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
                    self.write_b(true, buf);
                    self.write_c(buf);
                    self.write_unknown_field(buf);
                } else {
                    self.write_c(buf);
                    self.write_a(buf);
                    self.write_unknown_field(buf);
                    self.write_b(true, buf);
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
                    + prost::encoding::string::encoded_len(3, &self.c);

                if !self.skip_b {
                    strings_len += prost::encoding::string::encoded_len(2, &self.b);
                }

                if self.unknown_field {
                    strings_len += prost::encoding::string::encoded_len(10, &self.c);
                }

                if self.nested {
                    strings_len +=
                        // Group needs two more keys
                        (2 * key_len(2))
                            // Length encoded Message needs one key + 1 varint for the length
                            + key_len(1) +
                            length_delimiter_len(key_len(1) + length_delimiter_len(self.a.len()) + self.a.len());
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
                    #[allow(clippy::redundant_closure_call)]
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
                    #[allow(clippy::redundant_closure_call)]
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
                    #[allow(clippy::redundant_closure_call)]
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
                    #[allow(clippy::redundant_closure_call)]
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

        // Test single length delimited type
        extract_tests!(
            string,
            item: "my awesome string".to_string(),
            fill_expected_buf: |buf: &mut BytesMut, val: String| {
                //encode_varint(val.len().try_into().unwrap(), buf);
                buf.put_slice(val.as_bytes());
            }
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
