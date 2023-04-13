use anyhow::bail;
use schemars::schema_for;
use std::env;

fn generate_config_schema() -> anyhow::Result<()> {
    let schema = schema_for!(restate::Configuration);
    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(())
}

fn print_help() {
    println!(
        "
Usage: Run with `cargo xtask <task>`, eg. `cargo xtask generate-config-schema`.
Tasks:
    generate-config-schema: Generate config schema for restate configuration.
"
    );
}

fn main() -> anyhow::Result<()> {
    let task = env::args().nth(1);
    match task {
        None => print_help(),
        Some(t) => match t.as_str() {
            "generate-config-schema" => generate_config_schema()?,
            invalid => {
                print_help();
                bail!("Invalid task name: {}", invalid)
            }
        },
    };
    Ok(())
}
