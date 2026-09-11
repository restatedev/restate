// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::MetadataWriter;

pub async fn migrate_metadata(_writer: &MetadataWriter) -> anyhow::Result<()> {
    // nothing to do here.
    // This scaffolding only exists now for future metadata migrations.
    Ok(())
}
