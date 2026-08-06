// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[cfg_attr(feature = "clap", clap(rename_all = "kebab-case"))]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum BottommostLevelCompaction {
    /// Do not rewrite files already in the bottommost level.
    Skip,
    /// Rewrite bottommost files only when a compaction filter is configured. This is RocksDB's
    /// default.
    #[default]
    IfHaveCompactionFilter,
    /// Always rewrite the bottommost level, including files created earlier in this compaction.
    Force,
    /// Always rewrite existing bottommost files, but avoid rewriting files created earlier in this
    /// compaction.
    ForceOptimized,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ManualCompactionOptions {
    pub bottommost_level_compaction: BottommostLevelCompaction,
    pub recalculate_level: bool,
}
