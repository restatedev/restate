// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UpdateVersion,
};
use tokio::sync::{RwLock, RwLockWriteGuard};

/// An in-memory store that, like GCS, matches conditional updates on the object version
/// alone and rejects them without one, so tests catch a tag that loses the version.
#[derive(Debug, Default)]
pub(super) struct VersionMatchingStore {
    objects: InMemory,
    write_gate: RwLock<()>,
}

impl VersionMatchingStore {
    /// Holds all writes until the returned guard is dropped.
    pub(super) async fn pause_writes(&self) -> RwLockWriteGuard<'_, ()> {
        self.write_gate.write().await
    }
}

impl std::fmt::Display for VersionMatchingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VersionMatchingStore")
    }
}

#[async_trait]
impl ObjectStore for VersionMatchingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if let PutMode::Update(UpdateVersion { version, .. }) = &opts.mode {
            let Some(version) = version.clone() else {
                return Err(Error::Generic {
                    store: "VersionMatchingStore",
                    source: "conditional update without a version".into(),
                });
            };
            // The in-memory store matches on the ETag, which doubles as the version here.
            opts.mode = PutMode::Update(UpdateVersion {
                e_tag: Some(version),
                version: None,
            });
        }
        let _writes_allowed = self.write_gate.read().await;
        let result = self.objects.put_opts(location, payload, opts).await?;
        Ok(PutResult {
            version: result.e_tag.clone(),
            ..result
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.objects.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut result = self.objects.get_opts(location, options).await?;
        result.meta.version = result.meta.e_tag.clone();
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.objects.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.objects.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.objects.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.objects.copy_opts(from, to, options).await
    }
}
