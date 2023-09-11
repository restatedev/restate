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

fn main() -> std::io::Result<()> {
    prost_build::Config::new()
        .bytes(["."])
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set.bin"),
        )
        .service_generator(
            tonic_build::configure()
                .build_client(false)
                .build_transport(false)
                .service_generator(),
        )
        .compile_protos(
            &[
                "proto/grpc/health/v1/health.proto",
                "proto/grpc/reflection/v1alpha/reflection.proto",
                "proto/dev/restate/ext.proto",
                "proto/dev/restate/services.proto",
                "proto/dev/restate/events.proto",
            ],
            &["proto"],
        )?;

    prost_build::Config::new()
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set_test.bin"),
        )
        .bytes(["."])
        .service_generator(tonic_build::configure().service_generator())
        .extern_path(".dev.restate", "crate::restate")
        .compile_protos(
            &[
                "tests/proto/test.proto",
                "tests/proto/greeter.proto",
                "tests/proto/event_handler.proto",
            ],
            &["proto", "tests/proto"],
        )?;

    Ok(())
}
