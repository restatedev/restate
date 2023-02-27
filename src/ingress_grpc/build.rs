use std::env;
use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    tonic_build::configure()
        // Remove this comment to compile to a local dir, for checking the protoc output
        //.out_dir("proto-test")
        .file_descriptor_set_path(
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join("file_descriptor_set.bin"),
        )
        .build_server(false)
        .compile(
            &[PathBuf::from("tests/proto/greeter.proto")],
            &[PathBuf::from("tests/proto")],
        )?;
    Ok(())
}
