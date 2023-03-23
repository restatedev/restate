use std::io;

use codederror::CodedError;
use thiserror::Error;

pub type GenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Error, CodedError, Debug)]
#[code(unknown)]
#[error("some error")]
pub struct NoCodeNoHintsError {}

#[derive(Error, CodedError, Debug)]
#[code(3)]
#[error("some error")]
pub struct NoHintsError {}

#[derive(Error, CodedError, Debug)]
#[code(2)]
#[error("some error from partition")]
#[hint("whatever")]
pub struct OtherError {}

#[derive(Error, CodedError, Debug)]
#[hint("Please contact the developers!")]
pub enum Error {
    #[error("partition {partition_id} error: {source}")]
    #[hint("do this to fix it")]
    Partition {
        partition_id: u64,
        #[source]
        #[hint]
        #[code]
        source: OtherError,
    },

    #[code(1)]
    #[hint("fix the config")]
    #[hint("ask developers help")]
    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("other coded: {0:?}")]
    OtherCoded(
        #[from]
        #[hint]
        #[code]
        OtherError,
    ),

    #[error(transparent)]
    #[code(unknown)]
    Other(#[from] GenericError),
}

#[derive(thiserror::Error, codederror::CodedError, Debug)]
#[code(3)]
pub enum TopLevelCodeError {
    #[error("io error when accessing to {file}: {source:?}")]
    #[hint("Check the path is correct and the file/directory can be read by the current user")]
    Io {
        file: String,
        #[source]
        source: io::Error,
    },
}

#[test]
fn code_and_hint() {
    let e = Error::Configuration("this config error happened".to_string());

    assert_eq!(
        format!("{}", e.decorate()),
        r"[RT-0001] configuration error: this config error happened

Hints:
* fix the config
* ask developers help
* Please contact the developers!

For more details, look at the docs with https://restate.dev/doc/errors/RT-0001"
    );
}

#[test]
fn no_hints() {
    let e = NoHintsError {};

    assert_eq!(
        format!("{}", e.decorate()),
        r"[RT-0003] some error

For more details, look at the docs with https://restate.dev/doc/errors/RT-0003"
    );
}

#[test]
fn no_code() {
    let e = Error::Other("some other error happened".to_string().into());

    assert_eq!(
        format!("{}", e.decorate()),
        r"some other error happened

Hints:
* Please contact the developers!"
    );
}

#[test]
fn no_code_no_hints() {
    let e = NoCodeNoHintsError {};

    assert_eq!(format!("{}", e.decorate()), "some error");
}

#[test]
fn source_code_hint_propagation_named_field() {
    let e = Error::Partition {
        partition_id: 1,
        source: OtherError {},
    };

    assert_eq!(
        format!("{}", e.decorate()),
        r"[RT-0002] partition 1 error: some error from partition

Hints:
* whatever
* do this to fix it
* Please contact the developers!

For more details, look at the docs with https://restate.dev/doc/errors/RT-0002"
    );
}

#[test]
fn source_code_hint_propagation_unnamed_field() {
    let e: Error = OtherError {}.into();

    assert_eq!(
        format!("{}", e.decorate()),
        r"[RT-0002] other coded: OtherError

Hints:
* whatever
* Please contact the developers!

For more details, look at the docs with https://restate.dev/doc/errors/RT-0002"
    );
}

#[test]
fn top_level_enum_code() {
    let e = TopLevelCodeError::Io {
        file: "myfile.txt".to_string(),
        source: io::Error::from(io::ErrorKind::NotFound),
    };

    assert_eq!(
        format!("{}", e.decorate()),
        r"[RT-0003] io error when accessing to myfile.txt: Kind(NotFound)

Hints:
* Check the path is correct and the file/directory can be read by the current user

For more details, look at the docs with https://restate.dev/doc/errors/RT-0003"
    );
}
