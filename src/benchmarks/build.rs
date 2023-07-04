fn main() -> std::io::Result<()> {
    prost_build::Config::new()
        .bytes(["."])
        .service_generator(tonic_build::configure().service_generator())
        .compile_protos(&["proto/counter.proto"], &["proto"])?;
    Ok(())
}
