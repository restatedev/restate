// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

fn main() -> std::io::Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    prost_build::Config::new()
        .bytes(["."])
        // .enum_attribute(
        //     "ServiceTag",
        //     "#[derive(::enum_map::Enum, ::derive_more::IsVariant, ::derive_more::Display)]",
        // )
        .enum_attribute(
            "ServiceTag",
            "#[derive(::derive_more::IsVariant, ::derive_more::Display)]",
        )
        .enum_attribute(
            "NodeStatus",
            "#[derive(::serde::Serialize, ::derive_more::IsVariant)]",
        )
        .enum_attribute("AdminStatus", "#[derive(::serde::Serialize)]")
        .enum_attribute("LogServerStatus", "#[derive(::serde::Serialize)]")
        .enum_attribute("WorkerStatus", "#[derive(::serde::Serialize)]")
        .enum_attribute("MetadataServerStatus", "#[derive(::serde::Serialize)]")
        .file_descriptor_set_path(out_dir.join("common_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "./protobuf/restate/common.proto",
            ],
            &["protobuf"],
        )?;
    Ok(())
}
