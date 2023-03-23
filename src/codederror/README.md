# CodedError

An extension of dtolnay's [thiserror](https://docs.rs/thiserror/latest/thiserror/) to add code and hints metadata to errors.

## Example

```rust,ignore
// Define the error:
#[derive(thiserror::Error, codederror::CodedError, Debug)]
#[hint("Please contact the developers!")]
pub enum Error {
    #[error("partition {partition_id} error: {source}")]
    #[hint("do this to fix it")]
    Partition {
        partition_id: u64,
        #[source]
        #[code]
        #[hint]
        source: OtherError,
    },
    #[code(1)]
    #[hint("fix the config")]
    #[hint("ask developers help")]
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error(transparent)]
    #[code(unknown)]
    Other(#[from] GenericError),
}

// And then use it:
let e = Error::Configuration("this config error happened".to_string());

assert_eq!(
    format!("{}", e.decorate()),
    r"[APP-0001] configuration error: this config error happened

Hints:
* fix the config
* ask developers help
* Please contact the developers!

For more details, look at the docs with http://mydocs.com/APP-0001"
);
```

## Thanks to

* dtolnay's [thiserror](https://docs.rs/thiserror/latest/thiserror/), where we borrowed some code from.
