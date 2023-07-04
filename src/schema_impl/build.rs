use std::env;
use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    prost_build::Config::new()
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set_test.bin"),
        )
        .bytes(["."])
        .compile_protos(&["tests/proto/test.proto"], &["tests/proto"])?;

    Ok(())
}
