use prost_build::protoc_from_env;
use std::process::Command;

fn main() -> std::io::Result<()> {
    if let Err(err) = check_protoc_version() {
        println!(
            "cargo:warning={}",
            format!(
                "Failed to verify protoc version: {}. Make sure that your protoc version >= 3.15",
                err
            )
        );
    }
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    tonic_build::configure().compile_with_config(
        config,
        &["proto/dev/restate/storage/v1/scan.proto"],
        &["proto"],
    )
}

fn check_protoc_version() -> Result<(), String> {
    let protoc_path = protoc_from_env();
    let mut cmd = Command::new(protoc_path);
    cmd.arg("--version");

    let output = cmd
        .output()
        .map_err(|_| "could not run protoc --version".to_string())?;

    let stdout = String::from_utf8(output.stdout)
        .map_err(|_| "could not parse protoc --version output".to_string())?;

    let version_str = stdout
        .strip_prefix("libprotoc")
        .map(|str| str.trim())
        .unwrap_or(&stdout);

    let version = semver::Version::parse(version_str)
        .map_err(|_| format!("could not parse protoc version from '{version_str}'"))?;
    let version_req =
        semver::VersionReq::parse(">=3.15").expect("version requirement must be valid");

    if version_req.matches(&version) {
        Ok(())
    } else {
        panic!("restate_storage_grpc requires protoc version {} to support optional fields but found version {}", version_req, version);
    }
}
