use super::*;

use crate::Schemas;
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

    use prost::encoding::{encode_key, key_len};
    use prost_reflect::{DynamicMessage, MessageDescriptor};

    pub(crate) fn expand(
        service_instance_type: &ServiceInstanceType,
        service_method: impl AsRef<str>,
        descriptor: MessageDescriptor,
        restate_key: Bytes,
    ) -> Result<DynamicMessage, Error> {
        if let ServiceInstanceType::Keyed {
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
            let mut b = BytesMut::with_capacity(key_len(root_number) + restate_key.len());

            // Encode the key of the protobuf field
            // Note: this assumes the root wire type is never a group, per extract algorithm above
            //  which converts groups to nested messages.
            encode_key(root_number, field_descriptor_kind.wire_type(), &mut b);

            // Append the restate key buffer
            b.put(restate_key);

            // Now this message should be a well formed protobuf message, we can create the DynamicMessage
            Ok(DynamicMessage::decode(descriptor, b.freeze())?)
        } else {
            Err(Error::UnexpectedServiceInstanceType)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        use key_extraction::extract_impls::extract;
        use prost::Message;
        use restate_pb::mocks::test::*;
        use restate_pb::mocks::DESCRIPTOR_POOL;
        use restate_schema_api::key::KeyStructure;
        use std::collections::{BTreeMap, HashMap};

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
