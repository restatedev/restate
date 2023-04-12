use super::*;

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// # Storage GRPC options
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "StorageGrpcOptions"))]
pub struct Options {
    /// # Bind address
    ///
    /// The address to bind for the storage grpc service.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_bind_address")
    )]
    pub bind_address: SocketAddr,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: Options::default_bind_address(),
        }
    }
}

impl Options {
    fn default_bind_address() -> SocketAddr {
        "0.0.0.0:9091".parse().unwrap()
    }

    pub fn build(self, rocksdb: RocksDBStorage) -> StorageService {
        let Options { bind_address } = self;

        StorageService::new(rocksdb, bind_address)
    }
}
