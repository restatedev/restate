// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod get;
mod patch;

use cling::prelude::*;
use restate_types::{flexbuffers_storage_encode_decode, Version, Versioned};

#[derive(Run, Subcommand, Clone)]
pub enum Metadata {
    /// Get a single key's value from the metadata store
    Get(get::GetValueOpts),
    /// Patch a value stored in the metadata store
    Patch(patch::PatchValueOpts),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GenericMetadataValue {
    // We assume that the concrete serialized type's encoded version field is called "version".
    version: Version,

    #[serde(flatten)]
    data: serde_json::Map<String, serde_json::Value>,
}

flexbuffers_storage_encode_decode!(GenericMetadataValue);

impl GenericMetadataValue {
    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::Value::Object(self.data.clone())
    }
}

impl Versioned for GenericMetadataValue {
    fn version(&self) -> Version {
        self.version
    }
}
