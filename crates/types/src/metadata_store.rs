// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod keys {
    //! Keys of values stored in the metadata store

    use bytestring::ByteString;

    pub static NODES_CONFIG_KEY: ByteString = ByteString::from_static("nodes_config");
    pub static BIFROST_CONFIG_KEY: ByteString = ByteString::from_static("bifrost_config");
    pub static PARTITION_TABLE_KEY: ByteString = ByteString::from_static("partition_table");
    pub static EPOCH_TABLE_KEY: ByteString = ByteString::from_static("epoch_table");
}
