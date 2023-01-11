use meta::Meta;

#[derive(Debug, clap::Parser)]
#[group(skip)]
pub(crate) struct Options {
    #[command(flatten)]
    meta_options: meta::Options,
}

pub(crate) struct Application {
    meta_service: Meta,
}

impl Options {
    pub(crate) fn build(self) -> Application {
        let meta_service = self.meta_options.build();

        Application { meta_service }
    }
}

impl Application {
    pub(crate) fn run(self) -> drain::Signal {
        let (signal, watch) = drain::channel();

        tokio::spawn(self.meta_service.run(watch));

        signal
    }
}
