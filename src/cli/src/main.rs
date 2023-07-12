mod encoder;
mod field;
mod table;

use clap::Parser;
use datafusion::arrow::array::{Array, AsArray, BinaryArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::{create_udf, Volatility};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::SessionContext;
use datafusion_cli::catalog::DynamicFileCatalog;
use datafusion_cli::{
    exec, print_format::PrintFormat, print_options::PrintOptions, DATAFUSION_CLI_VERSION,
};
use std::sync::Arc;
use tokio::runtime;

pub(crate) mod storage {
    pub(crate) mod v1 {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/dev.restate.storage.scan.v1.rs"));
    }
}

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short,
        long,
        num_args(0..),
        help = "Execute commands from file(s), then exit",
    )]
    file: Vec<String>,

    #[clap(
        short,
        long,
        help = "Storage gRPC server (defaults to http://0.0.0.0:9091)"
    )]
    server: Option<String>,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    if !args.quiet {
        println!("Restate CLI v{}", DATAFUSION_CLI_VERSION);
    }

    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("restate")
        .build()
        .expect("failed to build Tokio runtime!");

    runtime.block_on(async move {
        let opt: restate_tracing_instrumentation::Options = Default::default();
        let tracing_guard = opt
            .init("Restate CLI", std::process::id())
            .expect("failed to set up tracing and logging");

        let mut ctx = SessionContext::with_config_rt(
            SessionConfig::from_env()?
                .with_information_schema(true)
                .with_default_catalog_and_schema("restate", "public"),
            Arc::new(create_runtime_env()?),
        );

        // add this convenience function
        ctx.register_udf(create_udf(
            "from_hex",
            vec![DataType::Utf8],
            Arc::new(DataType::Binary),
            Volatility::Immutable,
            make_scalar_function(|args| match args[0].data_type() {
                DataType::Utf8 => {
                    let mut results = Vec::with_capacity(args[0].len());
                    for string in args[0].as_string::<i32>() {
                        results.push(match string {
                            None => None,
                            Some(string) => Some(hex::decode(string).map_err(|err| {
                                DataFusionError::Execution(format!(
                                    "from_hex decoding error: {err}"
                                ))
                            })?),
                        })
                    }
                    Ok(Arc::new(results.into_iter().collect::<BinaryArray>()))
                }
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for function power"
                ))),
            }),
        ));

        let server = match args.server {
            Some(server) => server,
            None => "http://0.0.0.0:9091".to_string(),
        };

        let client = crate::storage::v1::storage_client::StorageClient::connect(server)
            .await
            .unwrap();

        table::register(&ctx, client).unwrap();

        ctx.refresh_catalogs().await?;
        // install dynamic catalog provider that knows how to open files
        ctx.register_catalog_list(Arc::new(DynamicFileCatalog::new(
            ctx.state().catalog_list(),
            ctx.state_weak_ref(),
        )));

        let mut print_options = PrintOptions {
            format: PrintFormat::Table,
            quiet: args.quiet,
        };

        let files = args.file;

        let result = if !files.is_empty() {
            exec::exec_from_files(files, &mut ctx, &print_options).await;
            Ok(())
        } else {
            exec::exec_from_repl(&mut ctx, &mut print_options)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))
        };

        tracing_guard.async_shutdown().await;
        result
    })
}

fn create_runtime_env() -> Result<RuntimeEnv> {
    let rn_config = RuntimeConfig::new();
    RuntimeEnv::new(rn_config)
}
