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
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("node_ctrl_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["./proto/node_ctrl.proto"], &["proto", "../pb/proto"])?;

    tonic_build::configure()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("cluster_controller_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(
            &["./proto/cluster_controller.proto"],
            &["proto", "../pb/proto"],
        )?;

    tonic_build::configure()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("worker_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["./proto/worker.proto"], &["proto", "../pb/proto"])?;

    tonic_build::configure()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("schema_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile(&["./proto/schema.proto"], &["proto", "../pb/proto"])?;

    Ok(())
}
