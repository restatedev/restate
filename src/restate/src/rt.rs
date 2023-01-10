use tokio::runtime::{Builder, Runtime};

pub(super) fn build_runtime() -> Result<Runtime, std::io::Error> {
    Builder::new_multi_thread()
        .enable_all()
        .thread_name("restate")
        .build()
}
