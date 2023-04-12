fn main() -> std::io::Result<()> {
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    tonic_build::configure().compile_with_config(
        config,
        &["proto/dev/restate/storage/v1/scan.proto"],
        &["proto"],
    )
}
