// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use jsonptr::Pointer;
use std::env;
use std::fs::File;
use std::path::PathBuf;
use typify::{TypeSpace, TypeSpaceSettings};

fn main() -> std::io::Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    prost_build::Config::new()
        .bytes(["."])
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &["../../service-protocol/dev/restate/service/protocol.proto"],
            &["../../service-protocol"],
        )?;

    let mut parsed_content: serde_json::Value = serde_json::from_reader(
        File::open("../../service-protocol/endpoint_manifest_schema.json").unwrap(),
    )
    .unwrap();

    // Patch schema for https://github.com/oxidecomputer/typify/issues/531
    // We can get rid of this once the issue in typify is solved.
    Pointer::from_static(
        "/properties/services/items/properties/handlers/items/properties/input/default",
    )
    .delete(&mut parsed_content);
    Pointer::from_static(
        "/properties/services/items/properties/handlers/items/properties/input/examples",
    )
    .delete(&mut parsed_content);
    Pointer::from_static(
        "/properties/services/items/properties/handlers/items/properties/output/default",
    )
    .delete(&mut parsed_content);
    Pointer::from_static(
        "/properties/services/items/properties/handlers/items/properties/output/examples",
    )
    .delete(&mut parsed_content);

    // Instantiate type space and run code-generation
    let mut type_space =
        TypeSpace::new(TypeSpaceSettings::default().with_derive("Clone".to_owned()));
    type_space
        .add_root_schema(serde_json::from_value(parsed_content).unwrap())
        .unwrap();

    let contents = format!(
        "{}\n{}",
        "use serde::{Deserialize, Serialize};",
        prettyplease::unparse(&syn::parse2::<syn::File>(type_space.to_stream()).unwrap())
    );

    std::fs::write(out_dir.join("endpoint_manifest.rs"), contents)
}
