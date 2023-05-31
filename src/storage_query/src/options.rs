use crate::service::PostgresQueryService;
use restate_storage_rocksdb::RocksDBStorage;
use std::net::SocketAddr;

/// # Storage query options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "StorageQueryOptions"))]
pub struct Options {
    /// # Bind address
    ///
    /// The address to bind for the psql service.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_bind_address")
    )]
    pub bind_address: SocketAddr,

    /// # Memory limit
    ///
    /// The total memory in bytes that can be used to preform sql queries
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_memory_limit")
    )]
    pub memory_limit: Option<usize>,

    /// # Temp folder to use for spill
    ///
    /// The path to spill to
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_temp_folder")
    )]
    pub temp_folder: Option<String>,

    /// # Default query parallelism
    ///
    /// The number of parallel partitions to use for a query execution
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_query_parallelism")
    )]
    pub query_parallelism: Option<usize>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind_address: Options::default_bind_address(),
            memory_limit: Options::default_memory_limit(),
            temp_folder: Options::default_temp_folder(),
            query_parallelism: Options::default_query_parallelism(),
        }
    }
}

impl Options {
    fn default_bind_address() -> SocketAddr {
        "0.0.0.0:5432".parse().unwrap()
    }

    fn default_memory_limit() -> Option<usize> {
        None
    }

    fn default_temp_folder() -> Option<String> {
        None
    }

    fn default_query_parallelism() -> Option<usize> {
        None
    }

    pub fn build(self, rocksdb: RocksDBStorage) -> PostgresQueryService {
        let Options {
            bind_address,
            memory_limit,
            temp_folder,
            query_parallelism,
        } = self;

        PostgresQueryService {
            bind_address,
            rocksdb,
            memory_limit,
            temp_folder,
            query_parallelism,
        }
    }
}
