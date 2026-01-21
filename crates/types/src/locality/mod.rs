// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod location_scope;
mod node_location;
// This module is currently shared only with `replication` until its shape is redesigned. The
// future of topology will be more generic, lives longer, and reusable.
pub(crate) mod topology;

pub use location_scope::LocationScope;
pub use node_location::NodeLocation;
