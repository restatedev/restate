// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use crate::Schemas;
use bytes::Bytes;
use prost::Message;
use prost_reflect::{DynamicMessage, MethodDescriptor};
use restate_schema_api::key::json_conversion::{Error, RestateKeyConverter};
use restate_serde_util::SerdeableUuid;
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

impl RestateKeyConverter for Schemas {
    fn key_to_json(&self, service_name: impl AsRef<str>, key: Bytes) -> Result<Value, Error> {
        self.use_service_schema(service_name, |service_schemas| {
            let (_, method_desc) = service_schemas.methods
                .iter()
                .next()
                .expect("Must have at least one method. This should have been checked in service discovery. This is a bug, please contact the developers");

            key_to_json(&service_schemas.instance_type, method_desc.clone(), key)
        }).ok_or(Error::NotFound)?
    }

    fn json_to_key(&self, service_name: impl AsRef<str>, key: Value) -> Result<Bytes, Error> {
        self.use_service_schema(service_name, |service_schemas| {
            let (_, method_desc) = service_schemas.methods
                .iter()
                .next()
                .expect("Must have at least one method. This should have been checked in service discovery. This is a bug, please contact the developers");

            json_to_key(&service_schemas.instance_type,method_desc.clone(), key)
        }).ok_or(Error::NotFound)?
    }
}

fn key_to_json(
    service_instance_type: &ServiceInstanceType,
    method_descriptor: MethodDescriptor,
    key: Bytes,
) -> Result<Value, Error> {
    Ok(match service_instance_type {
        keyed @ ServiceInstanceType::Keyed {
            service_methods_key_field_root_number,
            ..
        } => {
            let key_field = method_descriptor
                .input()
                .get_field(
                    *service_methods_key_field_root_number
                        .get(method_descriptor.name())
                        .expect("Method must exist in the descriptor"),
                )
                .expect("Method must exist in the descriptor");
            let extracted_key = key_expansion::expand_impls::expand(
                keyed,
                method_descriptor.name(),
                method_descriptor.input(),
                key,
            )?;

            let json_message = extracted_key
                .serialize(serde_json::value::Serializer)
                .expect(
                "Protobuf -> JSON should never fail! This is a bug, please contact the developers",
            );

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

fn json_to_key(
    service_instance_type: &ServiceInstanceType,
    method_descriptor: MethodDescriptor,
    key: Value,
) -> Result<Bytes, Error> {
    match service_instance_type {
        keyed @ ServiceInstanceType::Keyed {
            service_methods_key_field_root_number,
            ..
        } => {
            let key_field = method_descriptor
                .input()
                .get_field(
                    *service_methods_key_field_root_number
                        .get(method_descriptor.name())
                        .expect("Method must exist in the parsed service methods"),
                )
                .expect("Input key field must exist in the descriptor");

            let mut input_map = serde_json::Map::new();
            input_map.insert(key_field.json_name().to_string(), key);

            let key_message = DynamicMessage::deserialize(
                method_descriptor.input(),
                Value::Object(input_map).into_deserializer(),
            )?;

            Ok(key_extraction::extract_impls::extract(
                keyed,
                method_descriptor.name(),
                key_message.encode_to_vec().into(),
            )?)
        }
        ServiceInstanceType::Unkeyed => {
            let parse_result: Uuid = SerdeableUuid::deserialize(key.into_deserializer())?.into();

            Ok(parse_result.as_bytes().to_vec().into())
        }
        ServiceInstanceType::Singleton if key.is_null() => Ok(Bytes::default()),
        ServiceInstanceType::Singleton => Err(Error::UnexpectedNonNullSingletonKey),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use key_extraction::extract_impls::extract;
    use prost::Message;
    use prost_reflect::{MessageDescriptor, ServiceDescriptor};
    use restate_pb::mocks::test::*;
    use restate_schema_api::key::KeyStructure;
    use serde::Serialize;
    use std::collections::{BTreeMap, HashMap};
    use uuid::Uuid;

    static METHOD_NAME: &str = "Test";

    fn test_service_descriptor() -> ServiceDescriptor {
        restate_pb::mocks::DESCRIPTOR_POOL
            .get_service_by_name("test.TestService")
            .unwrap()
    }

    fn test_method_descriptor() -> MethodDescriptor {
        test_service_descriptor().methods().next().unwrap()
    }

    fn test_descriptor_message() -> MessageDescriptor {
        test_method_descriptor().input()
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
    macro_rules! json_tests {
        ($field_name:ident) => {
            json_tests!($field_name, KeyStructure::Scalar);
        };
        ($field_name:ident, $key_structure:expr) => {
            json_tests!(
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
                fn test_key_to_json() {
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
                    let restate_key = extract(
                        &service_instance_type,
                        METHOD_NAME,
                        test_message.encode_to_vec().into(),
                    )
                    .expect("successful key extraction");

                    // Now convert the key to json
                    let actual_json_key = key_to_json(
                        &service_instance_type,
                        test_method_descriptor(),
                        restate_key,
                    )
                    .unwrap();

                    // Assert expanded field is equal to the one from the original message
                    assert_eq!(actual_json_key, expected_json_key);
                }

                #[test]
                fn test_json_to_key() {
                    let test_descriptor_message = test_descriptor_message();
                    let key_field = test_descriptor_message
                        .get_field_by_name(stringify!($field_name))
                        .expect("Field should exist");
                    let field_number = key_field.number();

                    // Create test message and service instance type
                    let test_message = $test_message;
                    let service_instance_type =
                        mock_keyed_service_instance_type($key_structure, field_number);

                    // Build the expected restate key
                    let expected_restate_key = extract(
                        &service_instance_type,
                        METHOD_NAME,
                        test_message.encode_to_vec().into(),
                    )
                    .expect("successful key extraction");

                    // Convert the message to json and get the key field
                    let mut dynamic_test_message = DynamicMessage::new(test_descriptor_message);
                    dynamic_test_message.transcode_from(&test_message).unwrap();
                    let input_json_key = dynamic_test_message
                        .serialize(serde_json::value::Serializer)
                        .unwrap()
                        .as_object()
                        .unwrap()
                        .get(key_field.json_name())
                        .unwrap()
                        .clone();

                    // Now convert the key from json
                    let actual_restate_key = json_to_key(
                        &service_instance_type,
                        test_method_descriptor(),
                        input_json_key,
                    )
                    .unwrap();

                    // Assert extracted field is equal to the one from the original message
                    assert_eq!(actual_restate_key, expected_restate_key);
                }
            }
        };
    }

    json_tests!(string);
    json_tests!(bytes);
    json_tests!(number);
    json_tests!(nested_message, nested_key_structure());
    json_tests!(
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
    json_tests!(
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
    fn unkeyed_convert_key_to_json() {
        let service_instance_type = ServiceInstanceType::Unkeyed;

        // Extract the restate key
        let restate_key = extract(&service_instance_type, METHOD_NAME, Bytes::new())
            .expect("successful key extraction");

        // Now convert the key to json
        let extracted_json_key = key_to_json(
            &service_instance_type,
            test_method_descriptor(),
            restate_key,
        )
        .unwrap();

        // Parsing uuid as string should work fine
        extracted_json_key
            .as_str()
            .unwrap()
            .parse::<Uuid>()
            .unwrap();
    }

    #[test]
    fn singleton_convert_key_to_json() {
        let service_instance_type = ServiceInstanceType::Singleton;

        // Extract the restate key
        let restate_key = extract(&service_instance_type, METHOD_NAME, Bytes::new())
            .expect("successful key extraction");

        // Now convert the key to json
        let actual_json_key = key_to_json(
            &service_instance_type,
            test_method_descriptor(),
            restate_key,
        )
        .unwrap();

        // Should be an empty object
        assert!(actual_json_key.as_object().unwrap().is_empty());
    }

    #[test]
    fn unkeyed_generate_key_from_json() {
        let service_instance_type = ServiceInstanceType::Unkeyed;

        // Extract the restate key
        let expected_restate_key = extract(&service_instance_type, METHOD_NAME, Bytes::new())
            .expect("successful key extraction");

        // Parse this as uuid
        let uuid = Uuid::from_slice(&expected_restate_key).unwrap();

        // Now convert the key to json
        let actual_restate_key = json_to_key(
            &service_instance_type,
            test_method_descriptor(),
            Value::String(uuid.as_simple().to_string()),
        )
        .unwrap();

        assert_eq!(actual_restate_key, expected_restate_key);
    }

    #[test]
    fn singleton_generate_key_from_json() {
        let service_instance_type = ServiceInstanceType::Singleton;

        // Now convert the key to json
        let actual_json_key = json_to_key(
            &service_instance_type,
            test_method_descriptor(),
            Value::Null,
        )
        .unwrap();

        // Should be an empty object
        assert!(actual_json_key.is_empty());
    }
}
