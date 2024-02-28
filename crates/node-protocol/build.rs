// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    prost_build::Config::new()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("common_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["./proto/common.proto"], &["proto"])?;

    prost_build::Config::new()
        .enum_attribute(
            "MessageKind",
            "#[derive(::enum_map::Enum, ::strum_macros::EnumIs)]",
        )
        .enum_attribute("Message.body", "#[derive(::strum_macros::EnumIs)]")
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("node_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["./proto/node.proto"], &["proto"])?;

    Ok(())
}
