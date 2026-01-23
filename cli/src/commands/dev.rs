// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use cling::prelude::*;
use comfy_table::{Cell, Table};
use tokio_util::sync::CancellationToken;

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::stylesheet;
use restate_cli_util::{CliContext, c_indent_table, c_println, c_warn};
use restate_lite::{AddressMeta, Options, Restate};
use restate_types::art::render_restate_logo;
use restate_types::net::address::{AdminPort, HttpIngressPort, ListenerPort};

use crate::build_info;
use crate::cli_env::CliEnv;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run")]
pub struct Dev {
    /// Start restate on a set of random ports
    #[clap(long, short = 'r')]
    use_random_ports: bool,

    /// Start restate on unix sockets only
    #[clap(long, short = 'u')]
    use_unix_sockets: bool,

    /// Do not delete the temporary data directory after exiting
    #[clap(long)]
    retain: bool,
}

pub async fn run(State(_env): State<CliEnv>, opts: &Dev) -> Result<()> {
    let cancellation = CancellationToken::new();
    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().to_path_buf();

    let options = Options {
        enable_tcp: !opts.use_unix_sockets,
        use_random_ports: opts.use_random_ports,
        data_dir: Some(data_dir.clone()),
        ..Default::default()
    };

    // Commented out until we put a replacement for the mock service endpoint
    //
    // let listener = TcpListener::bind("127.0.0.1:0").await?;
    // let mock_svc_addr = format!("http://{}/", listener.local_addr()?);
    // let (running_tx, running_rx) = oneshot::channel();
    // tokio::spawn({
    //     let cancellation = cancellation.clone();
    //     async move {
    //         cancellation
    //             .run_until_cancelled(mock_service_endpoint::listener::run_listener(
    //                 listener,
    //                 || {
    //                     let _ = running_tx.send(());
    //                 },
    //             ))
    //             .await
    //             .map(|result| result.map_err(|err| anyhow!("mock service endpoint failed: {err}")))
    //             .unwrap_or(Ok(()))
    //     }
    // });
    // let counter_service_running = running_rx.await;

    if opts.retain {
        c_println!(
            "{} Data directory will be retained after exit",
            stylesheet::HANDSHAKE_ICON
        );
        let _ = temp_dir.keep();
    }

    {
        let cancellation = cancellation.clone();
        let boxed: Box<dyn Fn() + Send> = Box::new(move || cancellation.cancel());
        *crate::EXIT_HANDLER.lock().unwrap() = Some(boxed);
    }

    let restate = Restate::create(options).await?;
    // register mock service
    // if let Err(err) = restate.discover_deployment(&mock_svc_addr).await {
    //     // we'll print this but we'll continue anyway since this is not a catastrophic error
    //     // for the user.
    //     eprintln!("Failed to discover the example `Counter` service deployment: {err:#?}");
    // }

    let addresses = restate.get_advertised_addresses();

    let admin_url = addresses
        .iter()
        .find_map(|address| {
            if address.name == AdminPort::NAME {
                Some(address.address.clone())
            } else {
                None
            }
        })
        .expect("Admin port is always set");
    c_println!(">> Using data dir: {}", data_dir.display());
    render(&addresses);
    c_println!();
    // if counter_service_running.is_ok() {
    //     c_println!("✅ `Counter` service endpoint is running on {mock_svc_addr}");
    // }

    if let Err(_err) = open::that(&admin_url) {
        c_println!("Failed to open browser automatically. Please open {admin_url} manually.")
    }

    c_println!(
        "{} Do NOT use this tool to run restate in production.\
        This is a test/dev environment. For production, please use `restate-server` instead.",
        stylesheet::TIP_ICON
    );

    if !opts.retain {
        c_warn!(
            "Data directory will be deleted after exit, use --retain to change this behaviour."
        );
    }

    c_println!();
    c_println!(
        "{} Restate is running - Press Ctrl-C to exit",
        stylesheet::TIP_ICON
    );
    c_println!();
    // spawn checking latest release
    tokio::spawn(build_info::check_if_latest_version());
    cancellation.cancelled().await;

    restate.stop().await?;
    Ok(())
}

fn render(addresses: &[AddressMeta]) {
    let mut table = Table::new_styled();
    let logo = render_restate_logo(CliContext::get().colors_enabled());
    c_println!("{}", logo);

    for address in addresses {
        if [HttpIngressPort::NAME, AdminPort::NAME].contains(&address.name.as_str()) {
            table.add_row(vec![
                Cell::new(address.name.to_string()).add_attribute(comfy_table::Attribute::Bold),
                Cell::new(address.address.to_string()),
            ]);
        }
    }

    c_indent_table!(0, table);
}
