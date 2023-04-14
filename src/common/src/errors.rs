use crate::utils::GenericError;

/// Error type for conversion related problems (e.g. Rust <-> Protobuf)
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error("missing field '{0}'")]
    MissingField(&'static str),
    #[error("invalid data: {0}")]
    InvalidData(GenericError),
}

impl ConversionError {
    pub fn invalid_data(source: impl Into<GenericError>) -> Self {
        ConversionError::InvalidData(source.into())
    }

    pub fn missing_field(field: &'static str) -> Self {
        ConversionError::MissingField(field)
    }
}
