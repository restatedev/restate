// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{BufMut, Bytes, BytesMut};
use restate_storage_api::Result;

use crate::StorageIterator;

pub struct OwnedIterator<I> {
    iter: I,
    arena: BytesMut,
    done: bool,
}

impl<I: StorageIterator> OwnedIterator<I> {
    pub(crate) fn new(iter: I) -> Self {
        Self {
            iter,
            arena: BytesMut::new(),
            done: false,
        }
    }
}

impl<I: StorageIterator> Iterator for OwnedIterator<I> {
    type Item = Result<(Bytes, Bytes)>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            None
        } else {
            match self.iter.item() {
                Ok(Some((k, v))) => {
                    self.arena.reserve(k.len() + v.len());
                    self.arena.put_slice(k);
                    let key = self.arena.split().freeze();
                    self.arena.put_slice(v);
                    let value = self.arena.split().freeze();
                    self.iter.next();
                    Some(Ok((key, value)))
                }
                Ok(None) => {
                    self.done = true;
                    None
                }
                Err(err) => {
                    self.done = true;
                    Some(Err(err))
                }
            }
        }
    }
}

impl<I: StorageIterator> std::iter::FusedIterator for OwnedIterator<I> {}
