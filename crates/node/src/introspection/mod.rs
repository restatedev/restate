// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Node-level introspection scanners for DataFusion tables.
//!
//! Each sub-module provides a local scanner implementation that reads
//! in-memory state from a specific node role and produces Arrow record
//! batches for fan-out SQL queries.

pub(crate) mod loglet_workers;
