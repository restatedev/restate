use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use aws_sdk_lambda::config::Region;
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;

static AWS_CLIENT_CACHE: OnceLock<
    RwLock<HashMap<String, Shared<BoxFuture<'static, aws_sdk_lambda::Client>>>>,
> = OnceLock::new();

pub fn get(region: &str) -> Shared<BoxFuture<'static, aws_sdk_lambda::Client>> {
    let cache = AWS_CLIENT_CACHE.get_or_init(|| RwLock::new(HashMap::new()));

    match cache.read().unwrap().get(region) {
        Some(client) => client.clone(),
        None => {
            let mut handle = cache.write().unwrap();
            if let Some(client_fut) = handle.get(region) {
                client_fut.clone()
            } else {
                let region = region.to_string();
                let client_fut = aws_config::from_env()
                    .region(Region::new(region.clone()))
                    .load()
                    .map(|config| aws_sdk_lambda::Client::new(&config))
                    .boxed()
                    .shared();

                handle.insert(region, client_fut.clone());
                client_fut
            }
        }
    }
}
