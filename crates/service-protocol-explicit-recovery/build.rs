// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!(
        "cargo:rerun-if-changed=../../service-protocol/dev/restate/service/explicit_recovery.proto"
    );

    prost_build::Config::new()
        .bytes(["."])
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                Path::new("../../service-protocol/dev/restate/service/explicit_recovery.proto")
                    .canonicalize()
                    .unwrap(),
            ],
            &[Path::new("../../service-protocol").canonicalize().unwrap()],
        )
}
