use std::{path::PathBuf, time::Duration};

use clap::{builder::OsStr, Parser};
use clap_verbosity_flag::InfoLevel;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use restate_types::config::Configuration;
use tracing::{error, info};

use restate_local_cluster_runner::{cluster::Cluster, shutdown};

#[derive(Debug, Clone, clap::Parser)]
#[command(author, version, about)]
pub struct Arguments {
    #[arg(short, long = "cluster-file", value_name = "FILE", global = true)]
    cluster_file: Option<PathBuf>,
    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity<InfoLevel>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    let arguments = Arguments::parse();

    let cluster_file = match arguments.cluster_file {
        None => {
            eprintln!("--cluster-file must be provided",);
            std::process::exit(1);
        }
        Some(f) if !f.exists() => {
            eprintln!("Cluster file {} does not exist", f.display());
            std::process::exit(1);
        }
        Some(f) => f,
    };

    let mut figment = Figment::new();

    let data = if cluster_file.extension() == Some(&OsStr::from("pkl")) {
        let output = match std::process::Command::new("pkl")
            .args([&OsStr::from("eval"), cluster_file.as_os_str()])
            .output()
        {
            Ok(output) => output,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("pkl must be installed and available on PATH; `brew install pkl` or https://pkl-lang.org/main/current/pkl-cli/index.html#installation");
                std::process::exit(1);
            }
            Err(err) => {
                eprintln!("Failed to execute pkl: {err}");
                std::process::exit(1);
            }
        };
        if !output.status.success() {
            eprintln!(
                "pkl did not execute successfully: {}",
                String::from_utf8(output.stderr).expect("pkl stderr to be utf8")
            );
            std::process::exit(1);
        }
        String::from_utf8(output.stdout).expect("pkl output to be utf8")
    } else {
        std::fs::read_to_string(&cluster_file).expect("to read file")
    };

    let toml = Toml::string(&data);

    for (node_name, _) in Figment::new()
        .merge(&toml)
        .find_value("nodes")
        .expect("to find nodes")
        .as_dict()
        .unwrap()
        .iter()
    {
        figment = figment.merge(Serialized::default(
            &format!("nodes.{node_name}.base_config"),
            Configuration::default(),
        ))
    }

    let cluster: Cluster = figment.merge(toml).extract().expect("config to load");

    // start capturing signals
    let shutdown_fut = shutdown();

    let mut cluster = cluster.start().await.unwrap();

    cluster.wait_healthy(Duration::from_secs(30)).await;

    shutdown_fut.await;

    match cluster.graceful_shutdown(Duration::from_secs(30)).await {
        Ok(_) => {
            info!("All nodes have exited")
        }
        Err(err) => {
            error!("Failed to observe cluster status: {err}");
            std::process::exit(1)
        }
    }
}
