fn main() -> std::io::Result<()> {
    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(&["proto/dev/restate/storage/v1/storage.proto"], &["proto"])
}
