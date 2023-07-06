use std::env;
use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    prost_build::Config::new()
        .bytes(["."])
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set.bin"),
        )
        .compile_protos(
            &[
                "proto/grpc/reflection/v1alpha/reflection.proto",
                "proto/dev/restate/services.proto",
            ],
            &["proto/grpc/reflection/v1alpha", "proto/dev/restate"],
        )?;

    // I need to run this twice to make sure I split the file descriptor set between prod and test
    prost_build::Config::new()
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set_test.bin"),
        )
        .bytes(["."])
        .compile_protos(&["tests/proto/test.proto"], &["tests/proto"])?;

    Ok(())
}
