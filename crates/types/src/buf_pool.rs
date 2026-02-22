// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

use heapless::Deque;

use bytes::BytesMut;

const OVERSIZE_THRESHOLD: usize = 2 * 1024 * 1024; // 2MiB
const NUM_BUFFERS: usize = 4;

thread_local! {
    static POOL: RefCell<Pool> = const { RefCell::new(Pool::new()) };
}

pub struct Buffers;

impl Buffers {
    /// Fetch or allocate a buffer that has at least `min_size` capacity
    pub fn reserve(min_size: usize) -> Buffer {
        POOL.with_borrow_mut(|pool| pool.reserve(min_size))
    }

    /// The total capacity of all buffers in the pool
    pub fn capacity() -> usize {
        POOL.with_borrow_mut(|pool| pool.capacity())
    }

    /// The number of buffers currently available in the pool
    pub fn len() -> usize {
        POOL.with_borrow(|pool| pool.len())
    }

    /// Reset the buffer pool to its initial state.
    pub fn reset() {
        POOL.replace(Pool::new());
    }
}

struct Pool(Deque<BytesMut, NUM_BUFFERS>);

impl Pool {
    const fn new() -> Self {
        Self(Deque::new())
    }

    fn reserve(&mut self, min_size: usize) -> Buffer {
        if let Some(mut buf) = self.0.pop_front() {
            buf.reserve(min_size);
            Buffer {
                buffer: ManuallyDrop::new(buf),
            }
        } else {
            // no buffer available, create a new one
            let buffer = BytesMut::with_capacity(min_size);
            Buffer {
                buffer: ManuallyDrop::new(buffer),
            }
        }
    }

    fn return_buffer(&mut self, mut buffer: BytesMut) {
        let _ = buffer.try_reclaim(1);
        buffer.clear();
        if self.0.len() < NUM_BUFFERS && buffer.capacity() <= OVERSIZE_THRESHOLD {
            self.0.push_back(buffer).unwrap();
        }
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn capacity(&mut self) -> usize {
        let mut sum = 0;
        for buf in self.0.iter_mut() {
            // reclaiming with 1 bytes will re-acquire as much as possible of the capacity
            // allocated in the original buffer if it was released.
            let _ = buf.try_reclaim(1);
            sum += buf.capacity();
        }
        sum
    }
}

pub struct Buffer {
    buffer: ManuallyDrop<BytesMut>,
}

impl Drop for Buffer {
    fn drop(&mut self) {
        POOL.with_borrow_mut(|pool| {
            // Safety: buffer is exclusives "taken" at drop time, so we know that it's
            // still valid. We don't provide &mut access to db, and it's not a public field.
            let buffer = unsafe { ManuallyDrop::take(&mut self.buffer) };
            pool.return_buffer(buffer);
        });
    }
}

impl Deref for Buffer {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;

    #[test]
    fn test_arena_basic() {
        Buffers::reset();
        let buf1 = Buffers::reserve(1024);
        assert_eq!(Buffers::len(), 0);
        let buf2 = Buffers::reserve(1024);
        assert_eq!(Buffers::len(), 0);
        let buf3 = Buffers::reserve(1024);
        assert_eq!(Buffers::len(), 0);
        let buf4 = Buffers::reserve(1024);
        assert_eq!(Buffers::len(), 0);
        let buf5 = Buffers::reserve(1024);
        assert_eq!(Buffers::len(), 0);

        drop(buf1);
        assert_eq!(Buffers::len(), 1);
        assert_eq!(Buffers::capacity(), 1024);

        drop(buf2);
        assert_eq!(Buffers::len(), 2);
        assert_eq!(Buffers::capacity(), 2048);

        drop(buf3);
        assert_eq!(Buffers::len(), 3);
        assert_eq!(Buffers::capacity(), 3072);
        drop(buf4);
        assert_eq!(Buffers::len(), 4);
        assert_eq!(Buffers::capacity(), 4096);

        // not returned
        drop(buf5);
        assert_eq!(Buffers::len(), 4);
        assert_eq!(Buffers::capacity(), 4096);
    }

    #[test]
    fn test_arena_return() {
        Buffers::reset();
        let buf1 = Buffers::reserve(1024);
        assert_eq!(Buffers::len(), 0);
        let buf2 = Buffers::reserve(10);
        assert_eq!(Buffers::len(), 0);

        drop(buf1);
        drop(buf2);

        // I should get the same buffer that had 1024 bytes capacity (buf1) since it was returned
        // first.
        let buf1 = Buffers::reserve(50);
        assert_eq!(buf1.capacity(), 1024);

        let buf2 = Buffers::reserve(50);
        assert_eq!(buf2.capacity(), 50);
    }

    #[test]
    fn test_arena_and_alloc() {
        Buffers::reset();
        let mut buf1 = Buffers::reserve(100);
        assert_eq!(Buffers::len(), 0);
        // extend, causes reallocation
        buf1.put_slice(&[1u8; 2000]);
        assert_eq!(buf1.len(), 2000);
        let b1 = buf1.split().freeze();
        println!("b1's capacity: {}", b1.len());
        println!("buf's capacity: {}", buf1.capacity());
        assert_eq!(b1.len(), 2000);
        drop(b1);
        drop(buf1);
        assert_eq!(Buffers::len(), 1);
        dbg!(Buffers::capacity());
        assert_eq!(Buffers::capacity(), 2000);
    }

    #[test]
    fn test_arena_oversized() {
        Buffers::reset();
        let buf1 = Buffers::reserve(OVERSIZE_THRESHOLD * 2);
        assert_eq!(buf1.capacity(), OVERSIZE_THRESHOLD * 2);
        // will be dropped since it's oversized
        drop(buf1);
        assert_eq!(Buffers::len(), 0);
        assert_eq!(Buffers::capacity(), 0);
    }
}
