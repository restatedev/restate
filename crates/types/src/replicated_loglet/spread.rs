// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

use crate::PlainNodeId;
use crate::replication::NodeSet;

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

impl Display for Spread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (i, id) in self.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{id}")?;
        }
        write!(f, "]")
    }
}
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

impl From<NodeSet> for Spread {
    fn from(v: NodeSet) -> Self {
        Self(v.into_iter().collect())
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

impl<'a> IntoIterator for &'a Spread {
    type Item = &'a PlainNodeId;

    type IntoIter = <&'a Box<[PlainNodeId]> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
