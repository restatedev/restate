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

/// Error codes used to communicate failures of invocations. Currently, this is tied to
/// grpc's error codes. See https://github.com/grpc/grpc/blob/master/doc/statuscodes.md#status-codes-and-their-use-in-grpc
/// for more details.
pub enum ErrorCode {
    Aborted = 10,
}

impl From<ErrorCode> for i32 {
    fn from(value: ErrorCode) -> Self {
        value as i32
    }
}
