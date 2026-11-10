// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod row;
pub(crate) mod schema;

use datafusion::arrow::datatypes::SchemaRef;

use restate_storage_api::blob_table::BlobEntry;
use restate_types::blob::BlobKind;

use super::table::BlobTable;
use schema::{SysBlobOutputBuilder, SysBlobOutputTable};

/// `sys_blob_output`: output blobs with their `ResponseResult` payload decoded.
impl BlobTable for SysBlobOutputTable {
    const KIND: Option<BlobKind> = Some(BlobKind::Output);

    type Builder = SysBlobOutputBuilder;

    fn schema() -> SchemaRef {
        SysBlobOutputBuilder::schema()
    }

    fn append_row(builder: &mut Self::Builder, entry: BlobEntry<'_>) {
        row::append_output_row(builder, entry)
    }
}
