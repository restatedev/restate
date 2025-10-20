// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::IsTerminal;
use std::io::Write as _;

use restate_lite::Options;
use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;

use restate_lite::AddressMeta;
use restate_lite::Restate;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let temp_dir = tempfile::tempdir()?;
    let options = Options {
        data_dir: Some(temp_dir.path().to_path_buf()),
        ..Default::default()
    };

    println!(
        "Using data dir: {}",
        options.data_dir.as_ref().unwrap().display()
    );

    let restate = Restate::create(options).await?;
    println!("** Bound addresses **");
    print_addresses(&restate.get_bound_addresses());

    println!("** Advertised addresses **");
    print_addresses(&restate.get_advertised_addresses());

    shutdown(restate).await?;
    eprintln!("Bye!");

    Ok(())
}

fn print_addresses(addresses: &[AddressMeta]) {
    if !std::io::stdout().is_terminal() {
        return;
    }

    let mut stdout = std::io::stdout().lock();
    for address in addresses {
        let _ = writeln!(&mut stdout, "[{}]: {}", address.name, address.address);
    }

    let _ = writeln!(&mut stdout);
}

async fn shutdown(restate: Restate) -> anyhow::Result<()> {
    let signal = tokio::select! {
        () = await_signal(SignalKind::interrupt()) => "SIGINT",
        () = await_signal(SignalKind::terminate()) => "SIGTERM",
    };
    eprintln!("Received {signal}, starting shutdown.");

    restate.stop().await
}

async fn await_signal(kind: SignalKind) {
    signal(kind)
        .expect("failed to register signal handler")
        .recv()
        .await;
}
