pub use crate::schemas_impl::component::ComponentErrorKind;
pub use crate::schemas_impl::deployment::DeploymentErrorKind;
pub use crate::schemas_impl::subscription::SubscriptionErrorKind;

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[error("error when trying to {op} '{id}': {inner:?}")]
pub struct Error {
    op: &'static str,
    id: String,
    #[code]
    #[source]
    inner: ErrorKind,
}

impl Error {
    pub fn new(op: &'static str, id: impl ToString, inner: impl Into<ErrorKind>) -> Error {
        Self {
            op,
            id: id.to_string(),
            inner: inner.into(),
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner
    }
}

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum ErrorKind {
    // Those are generic and used by all schema resources
    #[error("not found in the schema registry")]
    #[code(unknown)]
    NotFound,
    #[error("already exists in the schema registry")]
    #[code(unknown)]
    Override,

    // Specific resources errors
    #[error(transparent)]
    Component(
        #[from]
        #[code]
        ComponentErrorKind,
    ),
    #[error(transparent)]
    Deployment(
        #[from]
        #[code]
        DeploymentErrorKind,
    ),
    #[error(transparent)]
    Subscription(
        #[from]
        #[code]
        SubscriptionErrorKind,
    ),
}
