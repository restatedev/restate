// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(feature = "grpc-client")]
use restate_types::net::connect_opts::GrpcConnectionOptions;

tonic::include_proto!("restate.metadata_server_svc");
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("metadata_server_svc");

/// Creates a new MetadataServerSvcClient with appropriate configuration
#[cfg(feature = "grpc-client")]
pub fn new_metadata_server_client<O>(
    channel: tonic::transport::Channel,
    connection_options: &O,
) -> metadata_server_svc_client::MetadataServerSvcClient<tonic::transport::Channel>
where
    O: GrpcConnectionOptions + Send + Sync + ?Sized,
{
    /// Default send compression for grpc clients
    use tonic::codec::CompressionEncoding;
    pub const DEFAULT_GRPC_COMPRESSION: CompressionEncoding = CompressionEncoding::Zstd;

    metadata_server_svc_client::MetadataServerSvcClient::new(channel)
        .max_decoding_message_size(connection_options.message_size_limit().get())
        // note: the order of those calls defines the priority
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(DEFAULT_GRPC_COMPRESSION)
}

pub mod pb_conversions {
    use bytestring::ByteString;

    use restate_types::Version;
    use restate_types::errors::ConversionError;
    use restate_types::metadata::VersionedValue;

    use super::{GetResponse, GetVersionResponse, KvEntry, Ulid};

    impl TryFrom<GetResponse> for Option<VersionedValue> {
        type Error = ConversionError;

        fn try_from(value: GetResponse) -> Result<Self, Self::Error> {
            if let Some(versioned_value) = value.value {
                Ok(Some(VersionedValue::try_from(versioned_value)?))
            } else {
                Ok(None)
            }
        }
    }

    impl From<GetVersionResponse> for Option<Version> {
        fn from(value: GetVersionResponse) -> Self {
            value.version.map(Into::into)
        }
    }

    impl TryFrom<KvEntry> for (ByteString, VersionedValue) {
        type Error = ConversionError;

        fn try_from(kv_entry: KvEntry) -> Result<Self, Self::Error> {
            Ok((
                ByteString::try_from(kv_entry.key)
                    .map_err(|_| ConversionError::invalid_data("key"))?,
                kv_entry
                    .value
                    .ok_or(ConversionError::missing_field("value"))?
                    .try_into()?,
            ))
        }
    }

    impl From<ulid::Ulid> for Ulid {
        fn from(value: ulid::Ulid) -> Self {
            Self {
                high: (value.0 >> 64) as u64,
                low: value.0 as u64,
            }
        }
    }

    impl From<Ulid> for ulid::Ulid {
        fn from(value: Ulid) -> Self {
            Self::from((u128::from(value.high) << 64) | value.low as u128)
        }
    }
}

#[cfg(test)]
mod test {
    use super::Ulid;

    #[test]
    fn test_ulid_encoding() {
        let id = ulid::Ulid::new();
        let encoded = Ulid::from(id);
        let decoded = ulid::Ulid::from(encoded);

        assert_eq!(id, decoded);
    }
}
