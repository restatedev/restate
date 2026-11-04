// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dynamic Protobuf Schema Registry backed by Restate `ServiceMetadata` discovery.
//!
//! Services propagate compiled `google.protobuf.FileDescriptorSet` bytes (base64-encoded)
//! via service metadata key `proto.descriptor_set` (or `proto.fds`) and map state keys
//! to Protobuf message names via `proto.state.<state_key>` (or `proto.state.*`).

pub mod table;
#[cfg(test)]
mod tests;
pub mod udf;
pub mod wire;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use base64::Engine;
use dashmap::DashMap;
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use restate_types::live::Live;
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};

pub const PROTO_DESCRIPTOR_SET_KEY: &str = "proto.descriptor_set";
pub const PROTO_FDS_SHORT_KEY: &str = "proto.fds";
pub const PROTO_STATE_KEY_PREFIX: &str = "proto.state.";
pub const PROTO_STATE_WILDCARD_KEY: &str = "proto.state.*";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldKind {
    Double,
    Float,
    Int64,
    Uint64,
    Int32,
    Fixed64,
    Fixed32,
    Bool,
    String,
    Bytes,
    Uint32,
    Sfixed32,
    Sfixed64,
    Sint32,
    Sint64,
    Enum(String),
    Message(String),
}

impl FieldKind {
    pub fn type_name(&self) -> &str {
        match self {
            FieldKind::Double => "double",
            FieldKind::Float => "float",
            FieldKind::Int64 => "int64",
            FieldKind::Uint64 => "uint64",
            FieldKind::Int32 => "int32",
            FieldKind::Fixed64 => "fixed64",
            FieldKind::Fixed32 => "fixed32",
            FieldKind::Bool => "bool",
            FieldKind::String => "string",
            FieldKind::Bytes => "bytes",
            FieldKind::Uint32 => "uint32",
            FieldKind::Sfixed32 => "sfixed32",
            FieldKind::Sfixed64 => "sfixed64",
            FieldKind::Sint32 => "sint32",
            FieldKind::Sint64 => "sint64",
            FieldKind::Enum(name) => name.as_str(),
            FieldKind::Message(name) => name.as_str(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CompiledField {
    pub name: String,
    pub number: u32,
    pub kind: FieldKind,
    pub is_repeated: bool,
    pub map_entry: Option<(FieldKind, FieldKind)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathStep {
    SubMessage {
        tag: u32,
    },
    Field {
        tag: u32,
        kind: FieldKind,
    },
    MapLookup {
        tag: u32,
        key_str: String,
        value_kind: FieldKind,
    },
}

#[derive(Debug)]
pub struct CompiledMessageSchema {
    pub full_name: String,
    pub fields_by_name: HashMap<String, CompiledField>,
    pub fields_by_tag: HashMap<u32, CompiledField>,
    pub is_map_entry: bool,
    path_cache: DashMap<String, Option<Arc<Vec<PathStep>>>>,
}

impl CompiledMessageSchema {
    pub fn resolve_path(
        &self,
        field_path: &str,
        pool: &CompiledDescriptorPool,
    ) -> Option<Arc<Vec<PathStep>>> {
        if let Some(cached) = self.path_cache.get(field_path) {
            return cached.clone();
        }
        let compiled = self.compile_path_uncached(field_path, pool).map(Arc::new);
        self.path_cache
            .insert(field_path.to_string(), compiled.clone());
        compiled
    }

    fn compile_path_uncached(
        &self,
        field_path: &str,
        pool: &CompiledDescriptorPool,
    ) -> Option<Vec<PathStep>> {
        let segments: Vec<&str> = field_path.split('.').filter(|s| !s.is_empty()).collect();
        if segments.is_empty() {
            return None;
        }
        let mut steps = Vec::with_capacity(segments.len());
        let mut current_msg = self;

        for (idx, &seg) in segments.iter().enumerate() {
            let is_last = idx + 1 == segments.len();
            let field = current_msg.fields_by_name.get(seg)?;

            if is_last {
                steps.push(PathStep::Field {
                    tag: field.number,
                    kind: field.kind.clone(),
                });
                return Some(steps);
            }

            // Check if this field is a `map<string, V>` and the next segment is the map key
            if let Some((_k_kind, v_kind)) = &field.map_entry {
                if idx + 2 != segments.len() {
                    return None;
                }
                let map_key = segments[idx + 1];
                steps.push(PathStep::MapLookup {
                    tag: field.number,
                    key_str: map_key.to_string(),
                    value_kind: v_kind.clone(),
                });
                return Some(steps);
            }

            let FieldKind::Message(sub_name) = &field.kind else {
                return None;
            };
            steps.push(PathStep::SubMessage { tag: field.number });
            current_msg = pool.resolve_message(sub_name)?;
        }

        Some(steps)
    }

    pub fn fields_summary_json(&self) -> String {
        let mut ordered: Vec<&CompiledField> = self.fields_by_tag.values().collect();
        ordered.sort_by_key(|f| f.number);
        let items: Vec<serde_json::Value> = ordered
            .into_iter()
            .map(|f| {
                serde_json::json!({
                    "name": f.name,
                    "number": f.number,
                    "type": f.kind.type_name(),
                    "repeated": f.is_repeated,
                    "map": f.map_entry.is_some(),
                })
            })
            .collect();
        serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
    }
}

#[derive(Debug, Default)]
pub struct CompiledDescriptorPool {
    messages: HashMap<String, Arc<CompiledMessageSchema>>,
    short_names: HashMap<String, String>,
    enums: HashMap<String, HashMap<i32, String>>,
}

impl CompiledDescriptorPool {
    pub fn from_file_descriptor_set(fds: &FileDescriptorSet) -> Self {
        let mut raw_map_entries: HashMap<String, (FieldKind, FieldKind)> = HashMap::new();
        let mut enums: HashMap<String, HashMap<i32, String>> = HashMap::new();

        for file in &fds.file {
            let pkg = file.package.as_deref().unwrap_or("");
            for en in &file.enum_type {
                collect_enum(pkg, en, &mut enums);
            }
            for msg in &file.message_type {
                collect_map_entries_and_enums(pkg, msg, &mut raw_map_entries, &mut enums);
            }
        }

        let mut messages = HashMap::new();
        let mut short_names = HashMap::new();
        for file in &fds.file {
            let pkg = file.package.as_deref().unwrap_or("");
            for msg in &file.message_type {
                compile_message_recursive(
                    pkg,
                    msg,
                    &raw_map_entries,
                    &mut messages,
                    &mut short_names,
                );
            }
        }

        Self {
            messages,
            short_names,
            enums,
        }
    }

    pub fn resolve_message(&self, name: &str) -> Option<&Arc<CompiledMessageSchema>> {
        let trimmed = name.trim_start_matches('.');
        if let Some(m) = self.messages.get(trimmed) {
            return Some(m);
        }
        if let Some(full) = self.short_names.get(trimmed) {
            return self.messages.get(full);
        }
        None
    }

    pub fn resolve_enum_name(&self, enum_type: &str, number: i32) -> Option<&str> {
        let trimmed = enum_type.trim_start_matches('.');
        self.enums.get(trimmed)?.get(&number).map(String::as_str)
    }
}

fn qualify_name(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

fn collect_enum(
    prefix: &str,
    en: &EnumDescriptorProto,
    enums: &mut HashMap<String, HashMap<i32, String>>,
) {
    let Some(name) = en.name.as_deref() else {
        return;
    };
    let full = qualify_name(prefix, name);
    let mut vals = HashMap::new();
    for v in &en.value {
        if let (Some(vname), Some(vnum)) = (v.name.as_deref(), v.number) {
            vals.insert(vnum, vname.to_string());
        }
    }
    enums.insert(full, vals);
}

fn collect_map_entries_and_enums(
    prefix: &str,
    msg: &DescriptorProto,
    map_entries: &mut HashMap<String, (FieldKind, FieldKind)>,
    enums: &mut HashMap<String, HashMap<i32, String>>,
) {
    let Some(name) = msg.name.as_deref() else {
        return;
    };
    let full = qualify_name(prefix, name);
    let is_map = msg
        .options
        .as_ref()
        .and_then(|o| o.map_entry)
        .unwrap_or(false);
    if is_map {
        let mut k_kind = FieldKind::String;
        let mut v_kind = FieldKind::String;
        for f in &msg.field {
            if f.number == Some(1) {
                k_kind = convert_field_kind(f);
            } else if f.number == Some(2) {
                v_kind = convert_field_kind(f);
            }
        }
        map_entries.insert(full.clone(), (k_kind, v_kind));
    }
    for en in &msg.enum_type {
        collect_enum(&full, en, enums);
    }
    for nested in &msg.nested_type {
        collect_map_entries_and_enums(&full, nested, map_entries, enums);
    }
}

fn compile_message_recursive(
    prefix: &str,
    msg: &DescriptorProto,
    map_entries: &HashMap<String, (FieldKind, FieldKind)>,
    messages: &mut HashMap<String, Arc<CompiledMessageSchema>>,
    short_names: &mut HashMap<String, String>,
) {
    let Some(name) = msg.name.as_deref() else {
        return;
    };
    let full = qualify_name(prefix, name);
    let is_map = msg
        .options
        .as_ref()
        .and_then(|o| o.map_entry)
        .unwrap_or(false);

    let mut fields_by_name = HashMap::new();
    let mut fields_by_tag = HashMap::new();

    for f in &msg.field {
        let Some(fname) = f.name.as_deref() else {
            continue;
        };
        let Some(fnum) = f.number else {
            continue;
        };
        if fnum <= 0 {
            continue;
        }
        let kind = convert_field_kind(f);
        let is_repeated = f.label == Some(Label::Repeated as i32);
        let map_entry = if let FieldKind::Message(type_name) = &kind {
            map_entries.get(type_name.trim_start_matches('.')).cloned()
        } else {
            None
        };

        let compiled_field = CompiledField {
            name: fname.to_string(),
            number: fnum as u32,
            kind,
            is_repeated,
            map_entry,
        };
        fields_by_name.insert(fname.to_string(), compiled_field.clone());
        fields_by_tag.insert(fnum as u32, compiled_field);
    }

    let schema = Arc::new(CompiledMessageSchema {
        full_name: full.clone(),
        fields_by_name,
        fields_by_tag,
        is_map_entry: is_map,
        path_cache: DashMap::new(),
    });
    if !is_map {
        short_names.insert(name.to_string(), full.clone());
    }
    messages.insert(full.clone(), schema);

    for nested in &msg.nested_type {
        compile_message_recursive(&full, nested, map_entries, messages, short_names);
    }
}

fn convert_field_kind(f: &FieldDescriptorProto) -> FieldKind {
    let raw_type = f.r#type.and_then(|t| Type::try_from(t).ok());
    match raw_type {
        Some(Type::Double) => FieldKind::Double,
        Some(Type::Float) => FieldKind::Float,
        Some(Type::Int64) => FieldKind::Int64,
        Some(Type::Uint64) => FieldKind::Uint64,
        Some(Type::Int32) => FieldKind::Int32,
        Some(Type::Fixed64) => FieldKind::Fixed64,
        Some(Type::Fixed32) => FieldKind::Fixed32,
        Some(Type::Bool) => FieldKind::Bool,
        Some(Type::String) => FieldKind::String,
        Some(Type::Bytes) => FieldKind::Bytes,
        Some(Type::Uint32) => FieldKind::Uint32,
        Some(Type::Sfixed32) => FieldKind::Sfixed32,
        Some(Type::Sfixed64) => FieldKind::Sfixed64,
        Some(Type::Sint32) => FieldKind::Sint32,
        Some(Type::Sint64) => FieldKind::Sint64,
        Some(Type::Enum) => FieldKind::Enum(
            f.type_name
                .as_deref()
                .unwrap_or("")
                .trim_start_matches('.')
                .to_string(),
        ),
        Some(Type::Message | Type::Group) => FieldKind::Message(
            f.type_name
                .as_deref()
                .unwrap_or("")
                .trim_start_matches('.')
                .to_string(),
        ),
        None => FieldKind::Bytes,
    }
}

#[derive(Debug)]
pub struct ServiceProtoSchemas {
    pub service_name: String,
    pub revision: u32,
    pub pool: Arc<CompiledDescriptorPool>,
    pub state_key_messages: HashMap<String, Arc<CompiledMessageSchema>>,
    pub wildcard_message: Option<Arc<CompiledMessageSchema>>,
}

impl ServiceProtoSchemas {
    pub fn from_service_metadata(svc: &ServiceMetadata) -> Option<Self> {
        let b64 = svc
            .metadata
            .get(PROTO_DESCRIPTOR_SET_KEY)
            .or_else(|| svc.metadata.get(PROTO_FDS_SHORT_KEY))?;
        let raw_bytes = base64::engine::general_purpose::STANDARD
            .decode(b64.trim())
            .ok()?;
        let fds = FileDescriptorSet::decode(raw_bytes.as_slice()).ok()?;
        let pool = Arc::new(CompiledDescriptorPool::from_file_descriptor_set(&fds));

        let mut state_key_messages = HashMap::new();
        let mut wildcard_message = None;

        for (meta_k, meta_v) in &svc.metadata {
            if meta_k == PROTO_STATE_WILDCARD_KEY {
                if let Some(m) = pool.resolve_message(meta_v.trim()) {
                    wildcard_message = Some(m.clone());
                }
            } else if let Some(state_key) = meta_k.strip_prefix(PROTO_STATE_KEY_PREFIX)
                && !state_key.is_empty()
                && let Some(m) = pool.resolve_message(meta_v.trim())
            {
                state_key_messages.insert(state_key.to_string(), m.clone());
            }
        }

        Some(Self {
            service_name: svc.name.clone(),
            revision: svc.revision,
            pool,
            state_key_messages,
            wildcard_message,
        })
    }

    pub fn resolve_state_key(&self, state_key: &str) -> Option<&Arc<CompiledMessageSchema>> {
        self.state_key_messages
            .get(state_key)
            .or(self.wildcard_message.as_ref())
    }
}

type ServiceRevisionCacheKey = (String, u32);
type CachedServiceProtoSchemas = Option<Arc<ServiceProtoSchemas>>;

/// Live-updating cache of compiled Protobuf schemas resolved from Restate's `ServiceMetadataResolver`.
#[derive(Clone, derive_more::Debug)]
#[debug("DynamicProtoSchemaRegistry")]
pub struct DynamicProtoSchemaRegistry<SMR> {
    resolver: Live<SMR>,
    by_name: Arc<DashMap<String, CachedServiceProtoSchemas>>,
    cache: Arc<DashMap<ServiceRevisionCacheKey, CachedServiceProtoSchemas>>,
}

impl<SMR: ServiceMetadataResolver + Send + Sync + 'static> DynamicProtoSchemaRegistry<SMR> {
    pub fn new(resolver: Live<SMR>) -> Self {
        Self {
            resolver,
            by_name: Arc::new(DashMap::new()),
            cache: Arc::new(DashMap::new()),
        }
    }

    pub fn resolve_service_schemas(&self, service_name: &str) -> Option<Arc<ServiceProtoSchemas>> {
        if let Some(entry) = self.by_name.get(service_name) {
            return entry.clone();
        }
        let pinned = self.resolver.pinned();
        let Some(svc) = pinned.resolve_latest_service(service_name) else {
            self.by_name.insert(service_name.to_string(), None);
            return None;
        };
        let cache_key = (svc.name.clone(), svc.revision);
        let compiled = if let Some(entry) = self.cache.get(&cache_key) {
            entry.clone()
        } else {
            let c = ServiceProtoSchemas::from_service_metadata(&svc).map(Arc::new);
            self.cache.insert(cache_key, c.clone());
            c
        };
        self.by_name
            .insert(service_name.to_string(), compiled.clone());
        compiled
    }

    pub fn list_all_service_schemas(&self) -> Vec<Arc<ServiceProtoSchemas>> {
        let pinned = self.resolver.pinned();
        let services = pinned.list_services();
        let mut out = Vec::new();
        for svc in services {
            let cache_key = (svc.name.clone(), svc.revision);
            let compiled = if let Some(entry) = self.cache.get(&cache_key) {
                entry.clone()
            } else {
                let c = ServiceProtoSchemas::from_service_metadata(&svc).map(Arc::new);
                self.cache.insert(cache_key, c.clone());
                c
            };
            if let Some(schema) = compiled {
                out.push(schema);
            }
        }
        out
    }
}
