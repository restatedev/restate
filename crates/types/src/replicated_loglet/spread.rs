// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::PlainNodeId;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Deref,
    derive_more::DerefMut,
    derive_more::IntoIterator,
    derive_more::From,
    derive_more::Index,
    derive_more::IndexMut,
)]
pub struct Spread(Box<[PlainNodeId]>);

impl From<Vec<PlainNodeId>> for Spread {
    fn from(v: Vec<PlainNodeId>) -> Self {
        Self(v.into_boxed_slice())
    }
}

impl From<Vec<u32>> for Spread {
    fn from(v: Vec<u32>) -> Self {
        Self(v.into_iter().map(PlainNodeId::from).collect())
    }
}

impl<const N: usize> From<[PlainNodeId; N]> for Spread {
    fn from(value: [PlainNodeId; N]) -> Self {
        Self(From::from(value))
    }
}

impl<const N: usize> From<[u32; N]> for Spread {
    fn from(value: [u32; N]) -> Self {
        Self(value.into_iter().map(PlainNodeId::from).collect())
    }
}
