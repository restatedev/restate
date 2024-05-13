mod client;
mod interface;

pub use self::client::CloudClient;
pub use self::interface::CloudClientInterface;

pub mod generated {
    use serde::{Deserialize, Serialize};
    typify::import_types!(schema = "src/clients/cloud/schema.json");
}
