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
use rocksdb::{DBAccess, DBRawIteratorWithThreadMode};

pub struct OwnedIterator<'a, DB: DBAccess> {
    iter: DBRawIteratorWithThreadMode<'a, DB>,
    arena: BytesMut,
}

impl<'a, DB: DBAccess> OwnedIterator<'a, DB> {
    pub(crate) fn new(iter: DBRawIteratorWithThreadMode<'a, DB>) -> Self {
        Self {
            iter,
            arena: BytesMut::new(),
        }
    }
}

impl<DB: DBAccess> Iterator for OwnedIterator<'_, DB> {
    type Item = (Bytes, Bytes);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.iter.item() {
            self.arena.reserve(k.len() + v.len());
            self.arena.put_slice(k);
            let key = self.arena.split().freeze();
            self.arena.put_slice(v);
            let value = self.arena.split().freeze();
            self.iter.next();
            Some((key, value))
        } else {
            None
        }
    }
}
