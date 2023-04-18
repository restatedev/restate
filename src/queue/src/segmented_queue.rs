use crate::io;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::mem;
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;

use crate::segmented_queue::Segment::{
    LoadedFromDisk, LoadingFromDisk, Mutable, OnDisk, StoringToDisk,
};

///
/// This is a FIFO queue that has a predefined capacity (number of elements) and can spill to disk
/// once this capacity is reached.
///
/// # Basic usage example:
///
/// ```
/// use std::collections::VecDeque;
/// use restate_queue::SegmentQueue;
///
/// async fn demo() {
///     let mut queue = SegmentQueue::new("./", 1024);
///
///     queue.enqueue(1).await;
///     queue.enqueue(2).await;
///
///     assert_eq!(queue.dequeue().await, Some(1));
///     assert_eq!(queue.dequeue().await, Some(2));
/// }
///
/// ```
pub struct SegmentQueue<T> {
    segments: VecDeque<Segment<T>>,
    in_memory_element_threshold: usize,
    next_segment_id: u64,
    spillable_base_path: PathBuf,
}

impl<T: Serialize + DeserializeOwned + Send + 'static> SegmentQueue<T> {
    /// creates a new spillable segment queue.
    ///
    /// # Arguments
    /// * spillable_base_path - the base path to spill the segments there. Please note that the directory is expected to exists and be writable.
    /// * in_memory_element_threshold - the number of elements to hold in memory, before spilling.
    pub fn new(spillable_base_path: impl AsRef<Path>, in_memory_element_threshold: usize) -> Self {
        assert!(in_memory_element_threshold > 0);
        Self {
            segments: Default::default(),
            in_memory_element_threshold,
            next_segment_id: 0,
            spillable_base_path: spillable_base_path.as_ref().into(),
        }
    }

    /// enqueues an element of type T to the queue.
    /// Please note, that if the number of elements that are currently enqueued is greater than the
    /// threshold provided, this operation might take some time to complete, as it has to flush the queue to disk.
    pub async fn enqueue(&mut self, element: T) {
        if self.enqueue_internal(element) < self.in_memory_element_threshold {
            return;
        }
        let background_flush = self.has_previous_store_completed();
        // SAFETY: enqueue_internal will always make sure that the last segment in
        // SAFETY: self.segments is mutable, therefore it can not be empty.
        debug_assert!(!self.segments.is_empty());
        let id = self.next_segment_id;
        let segment = self.segments.back_mut().unwrap();
        // Good luck storing more than 2^64 segments on disk.
        self.next_segment_id = self.next_segment_id.wrapping_add_signed(1);
        segment
            .store_to_disk(self.spillable_base_path.clone(), id, background_flush)
            .await;
    }

    fn has_previous_store_completed(&mut self) -> bool {
        if self.segments.len() < 2 {
            return true;
        }
        let last_non_mutable = self.segments.len() - 2;
        match self.segments.get(last_non_mutable) {
            Some(StoringToDisk { handle, .. }) => handle.is_finished(),
            _ => true,
        }
    }

    /// dequeues an element of type T that was previously encoded.
    /// note that this operation might take a while to complete as it might have to load a previously
    /// serialized element from disk.
    pub async fn dequeue(&mut self) -> Option<T> {
        match self.segments.front_mut() {
            Some(segment) => {
                if segment.is_on_disk() {
                    segment
                        .load_from_disk(self.spillable_base_path.clone())
                        .await;
                }
                let head = segment.dequeue();
                let len = segment.len();
                if self.should_preload(len) {
                    self.try_preload_next_segment();
                }
                if len == 0 {
                    self.segments.pop_front();
                }
                head
            }
            None => None,
        }
    }

    /// preload if the current segment has less than a half of the in memory threshold.
    #[inline]
    fn should_preload(&self, len: usize) -> bool {
        2 * len < self.in_memory_element_threshold
    }

    fn try_preload_next_segment(&mut self) {
        if let Some(next_segment) = self.segments.get_mut(1) {
            next_segment.pre_load_from_disk(self.spillable_base_path.clone())
        }
    }

    /// enqueue an element. This function would make sure that at there exists a mutable
    /// segment at the back (.back() is a mutable segment) and enqueue this element to that
    /// segment.
    /// If the last segment isn't mutable or does not exists, it would create one.
    /// This function returns the size of the mutable segment after insertion.
    ///
    fn enqueue_internal(&mut self, element: T) -> usize {
        match self.segments.back_mut() {
            Some(segment) if segment.is_mutable() => {
                segment.enqueue(element);
                segment.len()
            }
            _ => {
                // None or Some of OnDisk/LoadedFromDisk
                let mut segment = Mutable {
                    buffer: VecDeque::with_capacity(self.in_memory_element_threshold),
                };
                segment.enqueue(element);
                self.segments.push_back(segment);

                1
            }
        }
    }
}
/// Mutable -> StoringToDisk -> OnDisk -> LoadingFromDisk -> LoadedFromDisk
#[derive(Debug)]
enum Segment<T> {
    Mutable {
        buffer: VecDeque<T>,
    },
    StoringToDisk {
        id: u64,
        len: usize,
        handle: JoinHandle<()>,
    },
    OnDisk {
        id: u64,
        len: usize,
    },
    LoadingFromDisk {
        len: usize,
        handle: JoinHandle<VecDeque<T>>,
    },
    LoadedFromDisk {
        buffer: VecDeque<T>,
    },
}

impl<T: Serialize + DeserializeOwned + Send + 'static> Segment<T> {
    fn is_mutable(&self) -> bool {
        matches!(self, Segment::Mutable { .. })
    }

    fn is_on_disk(&self) -> bool {
        match self {
            StoringToDisk { .. } => true,
            OnDisk { .. } => true,
            LoadingFromDisk { .. } => true,
            LoadedFromDisk { .. } => false,
            Mutable { .. } => false,
        }
    }

    fn enqueue(&mut self, element: T) {
        match self {
            Mutable { buffer } => buffer.push_back(element),
            _ => panic!("This is a bug, it is only possible to enqueue into a mutable segment. Please contact the restate developers."),
        }
    }

    fn dequeue(&mut self) -> Option<T> {
        match self {
            Mutable { buffer } => buffer.pop_front(),
            LoadedFromDisk {  buffer } => buffer.pop_front(),
            _ => panic!("This is a bug, it is only possible to dequeue from a mutable or in memory segment. Please contact the restate developers."),
        }
    }

    fn len(&self) -> usize {
        match self {
            StoringToDisk { len, .. } => *len,
            OnDisk { len, .. } => *len,
            LoadingFromDisk { len, .. } => *len,
            LoadedFromDisk { buffer, .. } => buffer.len(),
            Mutable { buffer } => buffer.len(),
        }
    }

    async fn store_to_disk(&mut self, base_dir: PathBuf, id: u64, background: bool) {
        match self {
            Mutable { buffer } => {
                let len = buffer.len();
                let buffer = mem::take(buffer);
                let handle = tokio::spawn(io::create_segment_infalliable(base_dir, id, buffer));
                if background {
                    *self = StoringToDisk { id, len, handle };
                } else {
                    handle.await.expect("Unable to store a segment.");
                    *self = OnDisk {id, len};
                }
            }
            _ => panic!("We can only store a mutable segment. This is a bug, please contact the restate developers."),
        }
    }

    async fn load_from_disk(&mut self, path: PathBuf) {
        let path = &path;
        loop {
            match self {
                Mutable { .. } => return,
                StoringToDisk { id, len, handle } => {
                    //
                    // first make sure that the buffer was successfully persisted to disk
                    //
                    handle.await.expect("Was not able to store to disk");
                    *self = OnDisk { id: *id, len: *len };
                }
                OnDisk { id, .. } => {
                    let path = path.clone();
                    let buffer = io::consume_segment_infallible(path, *id).await;

                    *self = LoadedFromDisk { buffer };
                }
                LoadingFromDisk { handle, .. } => {
                    let buffer = handle.await.expect("Unable to load a segment");
                    *self = LoadedFromDisk { buffer }
                }
                LoadedFromDisk { .. } => return,
            }
        }
    }

    fn pre_load_from_disk(&mut self, path: PathBuf) {
        if let StoringToDisk { id, len, handle } = self {
            if handle.is_finished() {
                *self = OnDisk { id: *id, len: *len }
            }
        }
        if let OnDisk { id, len } = self {
            let load_handle = tokio::spawn(io::consume_segment_infallible(path, *id));

            *self = LoadingFromDisk {
                len: *len,
                handle: load_handle,
            };
        }
    }
}
