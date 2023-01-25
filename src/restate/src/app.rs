use crate::future_util::TryProcessAbortFuture;
use meta::Meta;
use std::fmt::Debug;
use worker::Worker;

#[derive(Debug, clap::Parser)]
#[group(skip)]
pub(crate) struct Options {
    #[command(flatten)]
    meta_options: meta::Options,
}

pub(crate) struct Application {
    meta_service: Meta,
    worker: Worker,
}

impl Options {
    pub(crate) fn build(self) -> Application {
        let meta_service = self.meta_options.build();
        let worker = Worker::new();

        Application {
            meta_service,
            worker,
        }
    }
}

impl Application {
    pub(crate) fn run(self) -> drain::Signal {
        let (signal, watch) = drain::channel();

        tokio::spawn(
            self.meta_service
                .run(watch.clone())
                .abort_on_err(Some("Meta")),
        );

        tokio::spawn(self.worker.run(watch));

        signal
    }
}
