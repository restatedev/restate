// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::sync::Arc;

use restate_types::logs::Header;
use restate_types::storage::{StorageCodec, StorageDecode};

pub struct BifrostPayload {
    header: Header,
    inner: Body,
    cached: Option<Box<dyn Any>>,
}

enum Body {
    Bytes(bytes::Bytes),
    RockdbSlice {
        slice: rocksdb::DBPinnableSlice<'static>,
        db: Arc<rocksdb::DB>,
    },
    // only this one can't be used to get byte&
    Concrete(Box<dyn Any>),
}

impl BifrostPayload {
    fn deserialize<T: StorageDecode>(&self) -> Result<T, std::io::Error> {
        // let's assume we deserialize
        //

        let buf1 = [0u8; 1024];
        let mut buf2 = std::io::Cursor::new(&buf1);
        Ok(StorageCodec::decode(&mut buf2).unwrap())
    }

    fn ref_as<T: StorageDecode + 'static>(&mut self) -> Result<&T, std::io::Error> {
        let value = self.cached.get_or_insert_with(|| {
            let buf1 = [0u8; 1024];
            let mut buf2 = std::io::Cursor::new(&buf1);
            let v: T = StorageCodec::decode(&mut buf2).unwrap();
            Box::new(v)
        });
        value
            .downcast_ref::<T>()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "type mismatch"))
    }

    fn match_keys(&self) -> &[u64] {
        &[0, 10]
    }
}

// the flow. on append, we take the type T by reference, we keep it by ref until last minute.
// We now need to serialize and convert into bytes... that's the trick I guess. is that on read we
// can reuse the same bytes memory without copy.
