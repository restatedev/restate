#[derive(Default)]
pub(crate) struct Application {}

impl Application {
    pub(crate) fn run(self) -> drain::Signal {
        let (signal, _) = drain::channel();
        signal
    }
}
