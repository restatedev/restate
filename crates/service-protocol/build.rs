// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::path::Path;
use typify::{TypeSpace, TypeSpaceSettings};

fn main() -> std::io::Result<()> {
    prost_build::Config::new()
        .bytes(["."])
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &["service-protocol/dev/restate/service/protocol.proto"],
            &["service-protocol"],
        )?;

    let content =
        std::fs::read_to_string("./service-protocol/deployment_manifest_schema.json").unwrap();
    let schema = serde_json::from_str::<schemars::schema::RootSchema>(&content).unwrap();

    let mut type_space =
        TypeSpace::new(TypeSpaceSettings::default().with_derive("Clone".to_owned()));
    type_space.add_root_schema(schema).unwrap();

    let contents = format!(
        "{}\n{}",
        "use serde::{Deserialize, Serialize};",
        prettyplease::unparse(&syn::parse2::<syn::File>(type_space.to_stream()).unwrap())
    );

    let mut out_file = Path::new(&env::var("OUT_DIR").unwrap()).to_path_buf();
    out_file.push("deployment.rs");
    std::fs::write(out_file, contents)
}
