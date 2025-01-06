use crate::errors::GenericError;
use crate::journal_v2::raw::RawEntry;
use crate::journal_v2::Entry;

#[derive(Debug, thiserror::Error)]
#[error("encoding error: {0:?}")]
pub struct EncodingError(#[from] GenericError);

/// The decoder is the abstraction encapsulating how serialized data represents entries.
///
/// This is typically depending on the concrete service protocol implementation/format/version.
pub trait Decoder {
    fn decode_entry(entry: &RawEntry) -> Result<Entry, EncodingError>;
}

/// The encoder is the abstraction encapsulating how to represent entries as serialized data.
///
/// This is typically depending on the concrete service protocol implementation/format/version.
pub trait Encoder {
    fn encode_entry(entry: &Entry) -> Result<RawEntry, EncodingError>;
}
