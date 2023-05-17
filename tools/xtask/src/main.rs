use anyhow::bail;
use restate_common::retry_policy::RetryPolicy;
use restate_common::worker_command::WorkerCommandSender;
use schemars::gen::SchemaSettings;
use std::env;
use std::time::Duration;
use tokio::sync::mpsc;

fn generate_config_schema() -> anyhow::Result<()> {
    let schema = SchemaSettings::draft2019_09()
        .into_generator()
        .into_root_schema_for::<restate::Configuration>();
    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(())
}

fn generate_default_config() -> anyhow::Result<()> {
    println!(
        "{}",
        serde_yaml::to_string(&restate::Configuration::default())?
    );
    Ok(())
}

async fn generate_rest_api_doc() -> anyhow::Result<()> {
    let meta_options = restate_meta::Options::default();
    let rest_address = meta_options.rest_address();
    let openapi_address = format!("http://localhost:{}/openapi", rest_address.port());
    let mut meta_service = meta_options.build();
    meta_service.init().await.unwrap();

    // We start the Meta component, then download the openapi schema generated
    let (shutdown_signal, shutdown_watch) = drain::channel();
    let (command_tx, _command_rx) = mpsc::channel(1);
    let worker_command_tx = WorkerCommandSender::new(command_tx);
    let join_handle = tokio::spawn(meta_service.run(shutdown_watch, worker_command_tx));

    let res = RetryPolicy::fixed_delay(Duration::from_millis(100), 20)
        .retry_operation(|| async { reqwest::get(openapi_address.clone()).await?.text().await })
        .await
        .unwrap();

    println!("{}", res);

    shutdown_signal.drain().await;
    join_handle.await.unwrap().unwrap();

    Ok(())
}

fn print_help() {
    println!(
        "
Usage: Run with `cargo xtask <task>`, eg. `cargo xtask generate-config-schema`.
Tasks:
    generate-config-schema: Generate config schema for restate configuration.
    generate-default-config: Generate default configuration.
    generate-rest-api-doc: Generate Rest API documentation. Make sure to have the port 8081 open.
"
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let task = env::args().nth(1);
    match task {
        None => print_help(),
        Some(t) => match t.as_str() {
            "generate-config-schema" => generate_config_schema()?,
            "generate-default-config" => generate_default_config()?,
            "generate-rest-api-doc" => generate_rest_api_doc().await?,
            invalid => {
                print_help();
                bail!("Invalid task name: {}", invalid)
            }
        },
    };
    Ok(())
}
