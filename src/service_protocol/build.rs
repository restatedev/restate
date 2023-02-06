fn main() -> std::io::Result<()> {
    prost_build::Config::new().bytes(["."]).compile_protos(
        &["service-protocol/dev/restate/service/protocol.proto"],
        &["service-protocol"],
    )
}
