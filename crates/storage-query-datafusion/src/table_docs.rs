// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

pub trait TableDocs {
    fn name(&self) -> &str;

    fn description(&self) -> &str;

    fn columns(&self) -> &[TableColumn];
}

#[derive(Debug, Copy, Clone)]
pub struct TableColumn {
    pub name: &'static str,
    pub column_type: &'static str,
    pub description: &'static str,
}

#[derive(Debug, Copy, Clone)]
pub struct StaticTableDocs {
    pub name: &'static str,
    pub description: &'static str,
    pub columns: &'static [TableColumn],
}

impl TableDocs for StaticTableDocs {
    fn name(&self) -> &str {
        self.name
    }

    fn description(&self) -> &str {
        self.description
    }

    fn columns(&self) -> &[TableColumn] {
        self.columns
    }
}

#[derive(Debug)]
pub struct OwnedTableDocs {
    pub name: Cow<'static, str>,
    pub description: Cow<'static, str>,
    pub columns: Vec<TableColumn>,
}

impl TableDocs for OwnedTableDocs {
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn columns(&self) -> &[TableColumn] {
        &self.columns
    }
}
