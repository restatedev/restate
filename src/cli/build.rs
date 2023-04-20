fn main() -> std::io::Result<()> {
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    tonic_build::configure()
        .file_descriptor_set_path(
            std::path::PathBuf::from(
                std::env::var("OUT_DIR").expect("OUT_DIR environment variable not set"),
            )
            .join("file_descriptor_set.bin"),
        )
        .build_server(false)
        .compile_with_config(
            config,
            &[
                "proto/dev/restate/storage/v1/scan.proto",
                "proto/dev/restate/storage/v1/domain.proto",
            ],
            &["proto"],
        )
}
