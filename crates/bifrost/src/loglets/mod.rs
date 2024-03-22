// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(any(test, feature = "local_loglet"))]
pub mod local_loglet;
#[cfg(any(test, feature = "memory_loglet"))]
pub mod memory_loglet;
