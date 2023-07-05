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
                "proto/grpc/reflection/v1alpha/reflection.proto",
                "proto/dev/restate/services.proto",
            ],
            &["proto/grpc/reflection/v1alpha", "proto/dev/restate"],
        )?;

    prost_build::Config::new()
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set_test.bin"),
        )
        .bytes(["."])
        .service_generator(tonic_build::configure().service_generator())
        .compile_protos(
            &["tests/proto/test.proto", "tests/proto/greeter.proto"],
            &["tests/proto"],
        )?;

    Ok(())
}
