use meta::Meta;
use worker::Worker;

pub struct Application {
    meta: Meta,
    worker: Worker,
}

impl Application {
    pub fn new(meta: meta::Options, worker: worker::Options) -> Self {
        let meta = meta.build();
        let worker = worker.build(
            meta.method_descriptor_registry(),
            meta.key_extractors_registry(),
            meta.reflections_registry(),
            meta.service_endpoint_registry(),
        );

        Self { meta, worker }
    }

    pub fn run(self) -> drain::Signal {
        let (signal, watch) = drain::channel();

        tokio::spawn(self.meta.run(watch.clone()));

        tokio::spawn(self.worker.run(watch));

        signal
    }
}
