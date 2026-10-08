// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutPayload};
use tokio::io::AsyncReadExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tracing::debug;

use restate_types::config::SnapshotsOptions;

use super::InFlightGauge;
use crate::metric_definitions::{SNAPSHOT_UPLOAD_BYTES, SNAPSHOT_UPLOAD_REQUESTS_ACTIVE};

/// A local snapshot file to upload.
pub(crate) struct UploadFile {
    /// The file's name in the snapshot metadata; reported back once its object exists.
    pub name: String,
    pub path: PathBuf,
    pub key: ObjectPath,
    /// Size recorded at export; paces reads before the file is opened.
    pub size: u64,
}

/// The node-wide budget that all snapshot uploads share.
///
/// Every upload request - one multipart part, or one whole file smaller than a part - holds a
/// permit from a single pool for as long as its part-sized buffer is alive, which bounds both
/// object store concurrency and upload memory per node. Each upload waits for at most one permit at
/// a time and the pool serves waiters in order, so concurrent snapshots take turns instead of the
/// largest one occupying every slot. An optional rate limit paces reads from local disk.
#[derive(Clone)]
pub(crate) struct UploadBudget {
    requests: Arc<Semaphore>,
    part_size: usize,
    rate_limiter: Option<Arc<RateLimiter>>,
}

impl UploadBudget {
    pub fn new(options: &SnapshotsOptions) -> Self {
        Self {
            requests: Arc::new(Semaphore::new(options.upload_parallelism())),
            part_size: options.upload_part_size(),
            rate_limiter: options
                .upload_max_rate_per_second
                .map(|rate| Arc::new(RateLimiter::new(rate.as_u64()))),
        }
    }

    /// Uploads `files`, calling `on_uploaded` with the name of each file whose object now exists.
    ///
    /// After the first failure no further file or part is started. Requests already in flight run
    /// to completion, and every multipart upload that was started is then completed or aborted,
    /// never dropped: S3 and GCS cannot reclaim the parts of a dropped upload, and no listing
    /// finds them. Only cancelling the returned future abandons uploads in progress; buckets
    /// should expire incomplete multipart uploads for that case.
    pub async fn upload(
        &self,
        object_store: &Arc<dyn ObjectStore>,
        files: Vec<UploadFile>,
        mut on_uploaded: impl FnMut(String),
    ) -> anyhow::Result<()> {
        let mut uploader = Uploader {
            part_size: self.part_size as u64,
            object_store: Arc::clone(object_store),
            queued: files.into_iter(),
            reading: None,
            open_uploads: HashMap::new(),
            next_upload_id: 0,
            requests: FuturesUnordered::new(),
            finishing: FuturesUnordered::new(),
            failure: None,
        };

        // Kept across iterations so that this upload holds its place in the permit queue.
        let mut admission: Option<BoxFuture<'static, OwnedSemaphorePermit>> = None;
        loop {
            if uploader.failure.is_some() {
                admission = None;
            } else if admission.is_none()
                && let Some(len) = uploader.next_request_len()
            {
                admission = Some(self.admit(len));
            }

            tokio::select! {
                Some((request, result)) = uploader.requests.next() => {
                    uploader.on_request_done(request, result, &mut on_uploaded);
                }
                Some(finished) = uploader.finishing.next() => {
                    uploader.on_finished(finished, &mut on_uploaded);
                }
                permit = async { admission.as_mut().expect("checked by the branch condition").await },
                    if admission.is_some() =>
                {
                    admission = None;
                    uploader.start_request(permit).await;
                }
                else => break,
            }
        }

        debug_assert!(uploader.open_uploads.is_empty());
        uploader.failure.map_or(Ok(()), Err)
    }

    fn admit(&self, len: u64) -> BoxFuture<'static, OwnedSemaphorePermit> {
        let requests = Arc::clone(&self.requests);
        let rate_limiter = self.rate_limiter.clone();
        async move {
            let permit = requests
                .acquire_owned()
                .await
                .expect("the upload request semaphore is never closed");
            if let Some(rate_limiter) = rate_limiter {
                rate_limiter.reserve(len).await;
            }
            permit
        }
        .boxed()
    }
}

/// The state of a single [`UploadBudget::upload`] call.
struct Uploader {
    part_size: u64,
    object_store: Arc<dyn ObjectStore>,
    queued: std::vec::IntoIter<UploadFile>,
    /// The multipart file being read; its parts are handed to the object store in order.
    reading: Option<Reading>,
    open_uploads: HashMap<usize, OpenUpload>,
    next_upload_id: usize,
    requests: FuturesUnordered<BoxFuture<'static, (Request, object_store::Result<()>)>>,
    finishing: FuturesUnordered<BoxFuture<'static, Finished>>,
    failure: Option<anyhow::Error>,
}

struct Reading {
    upload_id: usize,
    file: tokio::fs::File,
    remaining: u64,
}

struct OpenUpload {
    name: String,
    upload: Box<dyn MultipartUpload>,
    parts_in_flight: usize,
    /// Every part of the file has been handed to the object store.
    submitted: bool,
}

enum Started {
    Whole {
        name: String,
        key: ObjectPath,
        data: Bytes,
    },
    Multipart(Reading),
}

enum Request {
    Part { upload_id: usize, len: u64 },
    File { name: String, len: u64 },
}

enum Finished {
    Completed(String),
    Aborted,
    Failed(anyhow::Error),
}

impl Uploader {
    /// Length of the next request, or `None` once every file has been started and fully read.
    fn next_request_len(&self) -> Option<u64> {
        match &self.reading {
            Some(reading) => Some(reading.remaining.min(self.part_size)),
            None => self
                .queued
                .as_slice()
                .first()
                .map(|file| file.size.min(self.part_size)),
        }
    }

    async fn start_request(&mut self, permit: OwnedSemaphorePermit) {
        if let Err(err) = self.try_start_request(permit).await {
            self.fail(err);
        }
    }

    async fn try_start_request(&mut self, permit: OwnedSemaphorePermit) -> anyhow::Result<()> {
        let mut reading = match self.reading.take() {
            Some(reading) => reading,
            None => {
                let file = self
                    .queued
                    .next()
                    .expect("a request is only admitted while files remain");
                match self.start_file(file).await? {
                    Started::Multipart(reading) => reading,
                    Started::Whole { name, key, data } => {
                        let object_store = Arc::clone(&self.object_store);
                        let len = data.len() as u64;
                        self.push_request(Request::File { name, len }, permit, async move {
                            object_store
                                .put(&key, PutPayload::from(data))
                                .await
                                .map(|_| ())
                        });
                        return Ok(());
                    }
                }
            }
        };

        self.put_next_part(&mut reading, permit).await?;
        if reading.remaining > 0 {
            self.reading = Some(reading);
        }
        Ok(())
    }

    /// Opens `file`, reading it whole if it is smaller than a part, and otherwise starting its
    /// multipart upload.
    async fn start_file(&mut self, file: UploadFile) -> anyhow::Result<Started> {
        let mut handle = tokio::fs::File::open(&file.path)
            .await
            .with_context(|| format!("failed opening snapshot file {}", file.path.display()))?;
        let size = handle.metadata().await?.len();

        if size < self.part_size {
            let mut data = Vec::with_capacity(size as usize);
            handle.read_to_end(&mut data).await?;
            return Ok(Started::Whole {
                name: file.name,
                key: file.key,
                data: Bytes::from(data),
            });
        }

        debug!(key = %file.key, "Starting multipart upload of snapshot file");
        let upload = self.object_store.put_multipart(&file.key).await?;
        let upload_id = self.next_upload_id;
        self.next_upload_id += 1;
        self.open_uploads.insert(
            upload_id,
            OpenUpload {
                name: file.name,
                upload,
                parts_in_flight: 0,
                submitted: false,
            },
        );
        Ok(Started::Multipart(Reading {
            upload_id,
            file: handle,
            remaining: size,
        }))
    }

    async fn put_next_part(
        &mut self,
        reading: &mut Reading,
        permit: OwnedSemaphorePermit,
    ) -> anyhow::Result<()> {
        let len = reading.remaining.min(self.part_size);
        let mut buf = Vec::with_capacity(len as usize);
        let read = (&mut reading.file)
            .take(len)
            .read_to_end(&mut buf)
            .await
            .context("failed reading snapshot file")?;
        anyhow::ensure!(
            read as u64 == len,
            "snapshot file is shorter than its size at open"
        );

        let open = self
            .open_uploads
            .get_mut(&reading.upload_id)
            .expect("the file being read has an open upload");
        let part = open.upload.put_part(PutPayload::from(Bytes::from(buf)));
        open.parts_in_flight += 1;
        reading.remaining -= len;
        open.submitted = reading.remaining == 0;

        self.push_request(
            Request::Part {
                upload_id: reading.upload_id,
                len,
            },
            permit,
            part,
        );
        Ok(())
    }

    fn push_request(
        &mut self,
        request: Request,
        permit: OwnedSemaphorePermit,
        put: impl Future<Output = object_store::Result<()>> + Send + 'static,
    ) {
        let active = InFlightGauge::enter(SNAPSHOT_UPLOAD_REQUESTS_ACTIVE);
        self.requests.push(
            async move {
                let result = put.await;
                drop((permit, active));
                (request, result)
            }
            .boxed(),
        );
    }

    fn on_request_done(
        &mut self,
        request: Request,
        result: object_store::Result<()>,
        on_uploaded: &mut impl FnMut(String),
    ) {
        match request {
            Request::File { name, len } => match result {
                Ok(()) => {
                    metrics::counter!(SNAPSHOT_UPLOAD_BYTES).increment(len);
                    on_uploaded(name);
                }
                Err(err) => self.fail(err.into()),
            },
            Request::Part { upload_id, len } => {
                self.open_uploads
                    .get_mut(&upload_id)
                    .expect("a part in flight keeps its upload open")
                    .parts_in_flight -= 1;
                match result {
                    Ok(()) => metrics::counter!(SNAPSHOT_UPLOAD_BYTES).increment(len),
                    Err(err) => self.fail(err.into()),
                }
                self.finish_if_idle(upload_id);
            }
        }
    }

    fn on_finished(&mut self, finished: Finished, on_uploaded: &mut impl FnMut(String)) {
        match finished {
            Finished::Completed(name) => on_uploaded(name),
            Finished::Aborted => {}
            Finished::Failed(err) => self.fail(err),
        }
    }

    /// Records the first failure, stops reading, and finishes every upload with nothing in flight.
    fn fail(&mut self, err: anyhow::Error) {
        if self.failure.is_some() {
            debug!(%err, "Further snapshot upload failure");
            return;
        }
        self.failure = Some(err);
        self.reading = None;
        let upload_ids: Vec<_> = self.open_uploads.keys().copied().collect();
        for upload_id in upload_ids {
            self.finish_if_idle(upload_id);
        }
    }

    /// Completes the upload once all of its parts are in, or aborts it once nothing is in flight
    /// after a failure.
    fn finish_if_idle(&mut self, upload_id: usize) {
        let abort = self.failure.is_some();
        let Some(open) = self.open_uploads.get(&upload_id) else {
            return;
        };
        if open.parts_in_flight > 0 || !(open.submitted || abort) {
            return;
        }

        let OpenUpload {
            name, mut upload, ..
        } = self.open_uploads.remove(&upload_id).expect("checked above");
        self.finishing.push(
            async move {
                if abort {
                    if let Err(err) = upload.abort().await {
                        debug!(%err, "Failed to abort snapshot file multipart upload");
                    }
                    return Finished::Aborted;
                }
                match upload.complete().await {
                    Ok(_) => Finished::Completed(name),
                    Err(err) => {
                        if let Err(abort_err) = upload.abort().await {
                            debug!(%abort_err, "Failed to abort snapshot file multipart upload");
                        }
                        Finished::Failed(err.into())
                    }
                }
            }
            .boxed(),
        );
    }
}

/// Paces reads to a fixed byte rate. Each reservation starts where the previous one ends, so
/// callers are served in the order they reserve and idle time does not accumulate into a burst.
struct RateLimiter {
    bytes_per_second: f64,
    next_free: parking_lot::Mutex<Instant>,
}

impl RateLimiter {
    fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second: bytes_per_second as f64,
            next_free: parking_lot::Mutex::new(Instant::now()),
        }
    }

    async fn reserve(&self, bytes: u64) {
        let start = {
            let mut next_free = self.next_free.lock();
            let start = (*next_free).max(Instant::now());
            *next_free = start + Duration::from_secs_f64(bytes as f64 / self.bytes_per_second);
            start
        };
        tokio::time::sleep_until(start).await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, ObjectMeta, PutMultipartOptions,
        PutOptions, PutResult, UploadPart,
    };
    use tempfile::TempDir;

    use restate_util_bytecount::NonZeroByteCount;

    use super::*;

    const PART_SIZE: usize = 5 * 1024 * 1024;

    /// In-memory store that fails the `fail_on`-th multipart part and records how every multipart
    /// upload ended.
    #[derive(Debug)]
    struct FaultyStore {
        inner: InMemory,
        fail_on: usize,
        parts: Arc<AtomicUsize>,
        endings: Arc<parking_lot::Mutex<Endings>>,
    }

    #[derive(Debug, Default)]
    struct Endings {
        started: HashSet<String>,
        completed: HashSet<String>,
        aborted: HashSet<String>,
    }

    #[derive(Debug)]
    struct FaultyUpload {
        key: String,
        inner: Box<dyn MultipartUpload>,
        fail_on: usize,
        parts: Arc<AtomicUsize>,
        endings: Arc<parking_lot::Mutex<Endings>>,
    }

    #[async_trait]
    impl MultipartUpload for FaultyUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            if self.parts.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_on {
                return async {
                    Err(object_store::Error::Generic {
                        store: "faulty",
                        source: "injected part failure".into(),
                    })
                }
                .boxed();
            }
            self.inner.put_part(data)
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.endings.lock().completed.insert(self.key.clone());
            self.inner.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.endings.lock().aborted.insert(self.key.clone());
            self.inner.abort().await
        }
    }

    impl std::fmt::Display for FaultyStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FaultyStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FaultyStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.endings.lock().started.insert(location.to_string());
            Ok(Box::new(FaultyUpload {
                key: location.to_string(),
                inner: self.inner.put_multipart_opts(location, opts).await?,
                fail_on: self.fail_on,
                parts: Arc::clone(&self.parts),
                endings: Arc::clone(&self.endings),
            }))
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn budget(parallelism: u32) -> UploadBudget {
        UploadBudget::new(&SnapshotsOptions {
            upload_parallelism: NonZeroU32::new(parallelism),
            upload_part_size: Some(NonZeroByteCount::new(NonZeroUsize::new(PART_SIZE).unwrap())),
            ..SnapshotsOptions::default()
        })
    }

    fn write_files(dir: &TempDir, files: &[(&str, usize)]) -> Vec<UploadFile> {
        files
            .iter()
            .map(|(name, size)| {
                let path = dir.path().join(name);
                let data: Vec<u8> = (0..*size).map(|i| (i % 251) as u8).collect();
                std::fs::write(&path, data).unwrap();
                UploadFile {
                    name: (*name).to_owned(),
                    path,
                    key: ObjectPath::from(*name),
                    size: *size as u64,
                }
            })
            .collect()
    }

    /// A failed part must stop the upload from starting anything new, and every multipart upload
    /// it did start must end in `complete` or `abort` rather than being dropped, which S3 and GCS
    /// never clean up. Only files whose objects exist are reported as uploaded.
    #[tokio::test]
    async fn failed_part_finishes_every_started_upload() {
        let dir = TempDir::new().unwrap();
        let files = write_files(
            &dir,
            &[
                ("a.sst", PART_SIZE * 3),
                ("b.sst", PART_SIZE * 3),
                ("c.sst", PART_SIZE * 3),
                ("d.sst", 1024),
            ],
        );
        let endings = Arc::new(parking_lot::Mutex::new(Endings::default()));
        let store: Arc<dyn ObjectStore> = Arc::new(FaultyStore {
            inner: InMemory::new(),
            fail_on: 5,
            parts: Arc::new(AtomicUsize::new(0)),
            endings: Arc::clone(&endings),
        });

        let mut uploaded = Vec::new();
        let result = budget(2)
            .upload(&store, files, |name| uploaded.push(name))
            .await;
        assert!(
            result.is_err(),
            "the injected part failure must fail the upload"
        );

        {
            let endings = endings.lock();
            assert!(endings.started.contains("a.sst") && endings.started.contains("b.sst"));
            assert!(
                endings.aborted.contains("b.sst"),
                "the upload whose part failed must be aborted"
            );
            for key in &endings.started {
                assert!(
                    endings.completed.contains(key) != endings.aborted.contains(key),
                    "{key} must be completed or aborted exactly once, got {endings:?}"
                );
            }
            assert!(
                !endings.started.contains("c.sst"),
                "no file may start after the failure"
            );
        }
        for name in ["a.sst", "b.sst", "c.sst", "d.sst"] {
            let exists = store.head(&ObjectPath::from(name)).await.is_ok();
            assert_eq!(
                exists,
                uploaded.iter().any(|uploaded| uploaded == name),
                "{name}: reported uploads must match the objects that exist"
            );
        }
    }

    /// Files spanning several parts, and files below a part, arrive intact while uploads share a
    /// pool smaller than the number of parts.
    #[tokio::test]
    async fn uploads_files_intact_through_shared_pool() {
        let dir = TempDir::new().unwrap();
        let sizes = [
            ("large.sst", PART_SIZE * 2 + 1024),
            ("exact.sst", PART_SIZE * 2),
            ("small.sst", 7),
            ("empty.sst", 0),
        ];
        let files = write_files(&dir, &sizes);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut uploaded = Vec::new();
        budget(3)
            .upload(&store, files, |name| uploaded.push(name))
            .await
            .unwrap();

        uploaded.sort();
        assert_eq!(
            uploaded,
            ["empty.sst", "exact.sst", "large.sst", "small.sst"]
        );
        for (name, size) in sizes {
            let data = store
                .get(&ObjectPath::from(name))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let expected: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            assert!(data == expected, "{name} content mismatch");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_paces_reservations() {
        let limiter = RateLimiter::new(100);
        let start = Instant::now();

        limiter.reserve(100).await;
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "the first reservation is immediate"
        );
        limiter.reserve(50).await;
        limiter.reserve(50).await;
        assert_eq!(start.elapsed(), Duration::from_millis(1500));

        // Idle time does not accumulate into a burst.
        tokio::time::sleep(Duration::from_secs(10)).await;
        let resumed = Instant::now();
        limiter.reserve(100).await;
        limiter.reserve(100).await;
        assert_eq!(resumed.elapsed(), Duration::from_secs(1));
    }
}
