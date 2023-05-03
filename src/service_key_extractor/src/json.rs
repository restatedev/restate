use super::*;

use prost_reflect::{DynamicMessage, ServiceDescriptor};
use serde::Serialize;
use serde_json::{Map, Value};

trait RestateKeyConverter {
    fn to_json(&self, service_descriptor: ServiceDescriptor, key: Bytes) -> Result<Value, Error>;
}

impl RestateKeyConverter for KeyExtractorsRegistry {
    fn to_json(&self, service_descriptor: ServiceDescriptor, key: Bytes) -> Result<Value, Error> {
        let services = self.services.load();
        let service_instance_type =
            KeyExtractorsRegistry::resolve_instance_type(&services, service_descriptor.name())?;

        service_instance_type.to_json(service_descriptor, key)
    }
}

impl RestateKeyConverter for ServiceInstanceType {
    fn to_json(&self, service_descriptor: ServiceDescriptor, key: Bytes) -> Result<Value, Error> {
        Ok(match self {
            keyed @ ServiceInstanceType::Keyed {
                service_methods_key_field_root_number,
                ..
            } => {
                // TODO I'm not completely certain that all these expect("...") are correct.
                //  There might be some synchronization issues between the provided
                //  `service_descriptor` and what is stored in `self`.
                //  This problem goes away with a unique https://github.com/restatedev/restate/issues/43.
                let method_desc = service_descriptor.methods().next().expect("Must have at least one method. This should have been checked in service discovery. This is a bug, please contact the developers");
                let key_field = method_desc
                    .input()
                    .get_field(
                        *service_methods_key_field_root_number
                            .get(method_desc.name())
                            .expect("Method must exist in the descriptor"),
                    )
                    .expect("Method must exist in the descriptor");
                let extracted_key = keyed.expand(
                    service_descriptor.full_name(),
                    method_desc.name(),
                    method_desc.input(),
                    key,
                )?;

                let json_message = extracted_key.serialize(serde_json::value::Serializer)
                    .expect("Protobuf -> JSON should never fail! This is a bug, please contact the developers");

                match json_message {
                    Value::Object(mut m) => {
                        m.remove(key_field.json_name()).expect("The json serialized message must contain the key field")
                    },
                    _ => panic!("This must be a map because the input schema of the protobuf conversion is always a message! This is a bug, please contact the developers")
                }
            }
            ServiceInstanceType::Unkeyed => Value::String(
                uuid::Builder::from_slice(&key)
                    .unwrap()
                    .into_uuid()
                    .to_string(),
            ),
            ServiceInstanceType::Singleton => Value::Object(Map::new()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use prost::Message;
    use prost_reflect::{DescriptorPool, MessageDescriptor};
    use serde::Serialize;
    use uuid::Uuid;

    mod pb {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/test.rs"));
    }
    use pb::*;

    static DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));
    static METHOD_NAME: &str = "Test";

    fn test_service_descriptor() -> ServiceDescriptor {
        DescriptorPool::decode(DESCRIPTOR)
            .unwrap()
            .get_service_by_name("test.TestService")
            .unwrap()
    }

    fn test_descriptor_message() -> MessageDescriptor {
        test_service_descriptor().methods().next().unwrap().input()
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
    macro_rules! to_json_tests {
        ($field_name:ident) => {
            to_json_tests!($field_name, KeyStructure::Scalar);
        };
        ($field_name:ident, $key_structure:expr) => {
            to_json_tests!(
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
                fn to_json() {
                    let test_descriptor_message = test_descriptor_message();
                    let key_field = test_descriptor_message
                        .get_field_by_name(stringify!($field_name))
                        .expect("Field should exist");
                    let field_number = key_field.number();

                    // Create test message and service instance type
                    let test_message = $test_message;
                    let service_instance_type =
                        mock_keyed_service_instance_type($key_structure, field_number);

                    // Convert the message to json and get the key field
                    let mut dynamic_test_message = DynamicMessage::new(test_descriptor_message);
                    dynamic_test_message.transcode_from(&test_message).unwrap();
                    let expected_json_key = dynamic_test_message
                        .serialize(serde_json::value::Serializer)
                        .unwrap()
                        .as_object()
                        .unwrap()
                        .get(key_field.json_name())
                        .unwrap()
                        .clone();

                    // Extract the restate key
                    let restate_key = service_instance_type
                        .extract("", METHOD_NAME, test_message.encode_to_vec().into())
                        .expect("successful key extraction");

                    // Now convert the key to json
                    let extracted_json_key = service_instance_type
                        .to_json(test_service_descriptor(), restate_key)
                        .unwrap();

                    // Assert expanded field is equal to the one from the original message
                    assert_eq!(extracted_json_key, expected_json_key);
                }
            }
        };
    }

    to_json_tests!(string);
    to_json_tests!(bytes);
    to_json_tests!(number);
    to_json_tests!(nested_message, nested_key_structure());
    to_json_tests!(
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
    to_json_tests!(
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

    #[test]
    fn unkeyed_to_json() {
        let service_instance_type = ServiceInstanceType::Unkeyed;

        // Extract the restate key
        let restate_key = service_instance_type
            .extract("", METHOD_NAME, Bytes::new())
            .expect("successful key extraction");

        // Now convert the key to json
        let extracted_json_key = service_instance_type
            .to_json(test_service_descriptor(), restate_key)
            .unwrap();

        // Parsing uuid as string should work fine
        extracted_json_key
            .as_str()
            .unwrap()
            .parse::<Uuid>()
            .unwrap();
    }

    #[test]
    fn singleton_to_json() {
        let service_instance_type = ServiceInstanceType::Singleton;

        // Extract the restate key
        let restate_key = service_instance_type
            .extract("", METHOD_NAME, Bytes::new())
            .expect("successful key extraction");

        // Now convert the key to json
        let extracted_json_key = service_instance_type
            .to_json(test_service_descriptor(), restate_key)
            .unwrap();

        // Should be an empty object
        assert!(extracted_json_key.as_object().unwrap().is_empty());
    }
}
