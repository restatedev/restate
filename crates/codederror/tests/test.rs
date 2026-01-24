// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io;

use codederror::*;
use thiserror::Error;

pub type GenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub const E0001: Code = error_code!(
    "E0001",
    help = "Ask developers for help about error code E0001",
    description = "long description 1"
);
pub const E0002: Code = error_code!(
    "E0002",
    help = "Ask on the street about E0002",
    description = "long description 2"
);
pub const E0003: Code = error_code!(
    "E0003",
    help = "I know a friend who knows a friend that knows about E0003",
    description = "long description 3"
);

#[derive(Error, CodedError, Debug)]
#[code(unknown)]
#[error("some error")]
pub struct NoCodeError {}

#[derive(Error, CodedError, Debug)]
#[code(E0002)]
#[error("some error from partition")]
pub struct OtherError {}

#[derive(Error, CodedError, Debug)]
pub enum Error {
    #[error("partition {partition_id} error: {source}")]
    Partition {
        partition_id: u64,
        #[source]
        #[code]
        source: OtherError,
    },

    #[code(E0001)]
    #[error("configuration error: {0}")]
    Configuration(String),

    #[error("other coded: {0:?}")]
    OtherCoded(
        #[from]
        #[code]
        OtherError,
    ),

    #[error(transparent)]
    #[code(unknown)]
    Other(#[from] GenericError),
}

#[derive(thiserror::Error, codederror::CodedError, Debug)]
#[code(E0003)]
pub enum TopLevelCodeError {
    #[error("io error when accessing to {file}: {source:?}")]
    Io {
        file: String,
        #[source]
        source: io::Error,
    },
    #[code(E0002)]
    #[error("specific error code")]
    Specific,
}

#[test]
fn code() {
    let e = Error::Configuration("this config error happened".to_string());

    assert_eq!(
        format!("{}", e.decorate()),
        r"[E0001] configuration error: this config error happened. Ask developers for help about error code E0001"
    );
}

#[test]
fn code_and_description() {
    let e = Error::Configuration("this config error happened".to_string());

    assert_eq!(
        format!("{:#}", e.decorate()),
        r"[E0001] configuration error: this config error happened

long description 1

Ask developers for help about error code E0001"
    );
}

#[test]
fn no_code() {
    let e = Error::Other("some other error happened".to_string().into());

    assert_eq!(format!("{}", e.decorate()), r"some other error happened");
}

#[test]
fn source_code_propagation_named_field() {
    let e = Error::Partition {
        partition_id: 1,
        source: OtherError {},
    };

    assert_eq!(
        format!("{}", e.decorate()),
        r"[E0002] partition 1 error: some error from partition. Ask on the street about E0002"
    );
}

#[test]
fn source_code_propagation_unnamed_field() {
    let e: Error = OtherError {}.into();

    assert_eq!(
        format!("{}", e.decorate()),
        r"[E0002] other coded: OtherError. Ask on the street about E0002"
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
        r"[E0003] io error when accessing to myfile.txt: Kind(NotFound). I know a friend who knows a friend that knows about E0003"
    );
}

#[test]
fn top_level_enum_code_with_variant_specific_code() {
    let e = TopLevelCodeError::Specific;

    assert_eq!(
        format!("{}", e.decorate()),
        r"[E0002] specific error code. Ask on the street about E0002"
    );
}
