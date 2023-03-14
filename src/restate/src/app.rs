use meta::Meta;
use std::fmt::Debug;
use worker::Worker;

#[derive(Debug, clap::Parser)]
#[group(skip)]
pub(crate) struct Options {
    #[command(flatten)]
    meta_options: meta::Options,

    #[command(flatten)]
    worker_options: worker::Options,
}

pub(crate) struct Application {
    meta: Meta,
    worker: Worker,
}

impl Options {
    pub(crate) fn build(self) -> Application {
        let meta = self.meta_options.build();
        let worker = self.worker_options.build(
            meta.method_descriptor_registry(),
            meta.key_extractors_registry(),
            meta.service_endpoint_registry(),
        );

        Application { meta, worker }
    }
}

impl Application {
    pub(crate) fn run(self) -> drain::Signal {
        let (signal, watch) = drain::channel();

        tokio::spawn(self.meta.run(watch.clone()));

        tokio::spawn(self.worker.run(watch));

        signal
    }
}
