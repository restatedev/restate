// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Schemas;

use bytes::Bytes;
use prost_reflect::DynamicMessage;
use restate_schema_api::key::expansion::Error;
use restate_schema_api::key::KeyExpander;

impl KeyExpander for Schemas {
    fn expand(
        &self,
        service_name: impl AsRef<str>,
        service_method: impl AsRef<str>,
        key: Bytes,
    ) -> Result<DynamicMessage, Error> {
        self.use_service_schema(service_name, move |service_schema| {
            let input_descriptor = service_schema
                .methods
                .get(service_method.as_ref())
                .ok_or(Error::NotFound)?
                .descriptor()
                .input();
            expand_impls::expand(
                &service_schema.instance_type,
                service_method,
                input_descriptor,
                key,
            )
        })
        .ok_or(Error::NotFound)?
    }
}

pub(crate) mod expand_impls {
    use super::*;
    use bytes::{BufMut, BytesMut};

    use crate::schemas_impl::InstanceTypeMetadata;
    use prost::encoding::{encode_key, encode_varint, key_len};
    use prost_reflect::{DynamicMessage, MessageDescriptor};

    pub(crate) fn expand(
        service_instance_type: &InstanceTypeMetadata,
        service_method: impl AsRef<str>,
        descriptor: MessageDescriptor,
        restate_key: impl AsRef<[u8]>,
    ) -> Result<DynamicMessage, Error> {
        if let InstanceTypeMetadata::Keyed {
            service_methods_key_field_root_number,
            ..
        } = service_instance_type
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
            let mut b = BytesMut::with_capacity(key_len(root_number) + restate_key.as_ref().len());

            // Encode the key of the protobuf field
            // Note: this assumes the root wire type is never a group, per extract algorithm above
            //  which converts groups to nested messages.
            encode_key(root_number, field_descriptor_kind.wire_type(), &mut b);

            encode_varint(restate_key.as_ref().len() as u64, &mut b);

            // Append the restate key buffer
            b.put(restate_key.as_ref());

            // Now this message should be a well formed protobuf message, we can create the DynamicMessage
            Ok(DynamicMessage::decode(descriptor, b.freeze())?)
        } else {
            Err(Error::UnexpectedServiceInstanceType)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        use crate::key_extraction::extract_impls::extract;
        use prost::Message;
        use restate_pb::mocks::test::*;
        use restate_pb::mocks::DESCRIPTOR_POOL;
        use restate_schema_api::discovery::KeyStructure;
        use std::collections::HashMap;

        static METHOD_NAME: &str = "test";

        fn test_descriptor_message() -> MessageDescriptor {
            DESCRIPTOR_POOL
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

        fn mock_keyed_service_instance_type(
            key_structure: KeyStructure,
            field_number: u32,
        ) -> InstanceTypeMetadata {
            InstanceTypeMetadata::Keyed {
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
                    fn test_expand() {
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
                        let restate_key = extract(
                            &service_instance_type,
                            METHOD_NAME,
                            test_message.encode_to_vec().into(),
                        )
                        .expect("successful key extraction");

                        // Now expand the key again into a message
                        let expanded_message = expand(
                            &service_instance_type,
                            METHOD_NAME,
                            test_descriptor_message,
                            restate_key,
                        )
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
    }
}
