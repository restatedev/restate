// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{fmt::Display, num::ParseIntError, ops::RangeInclusive, str::FromStr};

use bilrost::{Message, OwnedMessage};
use bytes::BytesMut;
use restate_encoding::{BilrostAs, BilrostDisplayFromStr};

#[derive(Default, BilrostAs)]
#[bilrost_as(BilrostDisplayFromStr)]
struct Stringer {
    inner: u64,
}

impl Display for Stringer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl FromStr for Stringer {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner: u64 = s.parse()?;
        Ok(Self { inner })
    }
}

#[derive(Debug, Default, Clone, BilrostAs)]
#[bilrost_as(AMessage)]
struct NotAMessage {
    range: Option<RangeInclusive<u64>>,
}

#[derive(bilrost::Message)]
struct AMessage {
    inner: Option<(u64, u64)>,
}

impl From<&NotAMessage> for AMessage {
    fn from(value: &NotAMessage) -> Self {
        Self {
            inner: value.range.clone().map(|r| (*r.start(), *r.end())),
        }
    }
}

impl From<AMessage> for NotAMessage {
    fn from(value: AMessage) -> Self {
        Self {
            range: value.inner.map(|v| v.0..=v.1),
        }
    }
}

#[test]
fn test_as_string() {
    #[derive(bilrost::Message)]
    struct Container {
        #[bilrost(1)]
        stringer: Stringer,
    }

    #[derive(bilrost::Message)]
    struct ContainerButWithString {
        #[bilrost(1)]
        stringer: String,
    }

    let src = Container {
        stringer: Stringer { inner: 42 },
    };

    let mut buf = BytesMut::new();
    src.encode(&mut buf).expect("encodes");

    let dst = ContainerButWithString::decode(buf.freeze()).expect("decodes");

    assert_eq!(dst.stringer, "42");
}

#[test]
fn test_as_from_into() {
    #[derive(bilrost::Message)]
    struct Container {
        #[bilrost(1)]
        inner: NotAMessage,
    }

    let src = Container {
        inner: NotAMessage {
            range: Some(0..=100),
        },
    };

    let mut buf = BytesMut::new();
    src.encode(&mut buf).expect("encodes");

    let dst = Container::decode(buf.freeze()).expect("decodes");

    assert!(matches!(dst.inner.range, Some(range) if *range.start() == 0 && *range.end() == 100));
}
