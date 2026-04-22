// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod bilrost_as;
pub mod bilrost_encodings;
mod common;

pub use bilrost_as::BilrostAsAdaptor;
pub use bilrost_encodings::{Arced, ArcedSlice, RestateEncoding};
pub use common::U128;
pub use restate_encoding_derive::{BilrostAs, BilrostNewType, NetSerde};

#[cfg(test)]
mod test {
    use bilrost::{Message, OwnedMessage};
    use restate_encoding_derive::BilrostNewType;

    #[derive(BilrostNewType)]
    struct MyId(u64);

    #[derive(bilrost::Message)]
    struct Nested {
        id: MyId,
    }

    #[derive(bilrost::Message)]
    struct Flattened {
        id: u64,
    }

    #[test]
    fn test_new_type() {
        let x = Nested { id: MyId(10) };

        let bytes = x.encode_to_bytes();

        let y = Flattened::decode(bytes).expect("decodes");

        assert_eq!(x.id.0, y.id);
    }
}
