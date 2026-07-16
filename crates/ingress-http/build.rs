// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Only re-run when proto files or this build script change
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=protobuf/");

    tonic_prost_build::configure()
        .bytes(".")
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["./protobuf/ingestion_svc.proto"], &["protobuf"])?;

    Ok(())
}
