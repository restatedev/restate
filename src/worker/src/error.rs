#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("internal error")]
    Internal,
}