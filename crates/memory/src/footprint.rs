// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// A trait for types that can be significant contributors to memory usage.
pub trait EstimatedMemorySize {
    /// Estimated number of bytes to be used by this value in memory.
    fn estimated_memory_size(&self) -> usize;
}

impl<T: EstimatedMemorySize + ?Sized> EstimatedMemorySize for &T {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self)
    }
}

impl<T: EstimatedMemorySize + ?Sized> EstimatedMemorySize for &mut T {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self)
    }
}

impl EstimatedMemorySize for () {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        0
    }
}

impl<T: EstimatedMemorySize> EstimatedMemorySize for Option<T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        self.as_ref()
            .map_or(0, EstimatedMemorySize::estimated_memory_size)
    }
}

impl<T: EstimatedMemorySize> EstimatedMemorySize for Vec<T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        self.iter()
            .map(EstimatedMemorySize::estimated_memory_size)
            .sum()
    }
}

impl<T: EstimatedMemorySize> EstimatedMemorySize for [T] {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        self.iter()
            .map(EstimatedMemorySize::estimated_memory_size)
            .sum()
    }
}

impl EstimatedMemorySize for String {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        self.len()
    }
}

impl EstimatedMemorySize for bytes::Bytes {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        self.len()
    }
}

impl EstimatedMemorySize for bytes::BytesMut {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        self.len()
    }
}

impl EstimatedMemorySize for [u8] {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        self.len()
    }
}

impl EstimatedMemorySize for str {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        self.len()
    }
}

impl<T: EstimatedMemorySize + ?Sized> EstimatedMemorySize for std::sync::Arc<T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self)
    }
}

impl<T: EstimatedMemorySize + ?Sized> EstimatedMemorySize for std::rc::Rc<T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self)
    }
}

impl<T: EstimatedMemorySize + ?Sized> EstimatedMemorySize for Box<T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self)
    }
}

impl<T: EstimatedMemorySize + ?Sized> EstimatedMemorySize for std::sync::MutexGuard<'_, T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self)
    }
}

impl<T: EstimatedMemorySize + ?Sized> EstimatedMemorySize for std::sync::RwLockReadGuard<'_, T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self)
    }
}

impl<T: EstimatedMemorySize + ?Sized> EstimatedMemorySize for std::sync::RwLockWriteGuard<'_, T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self)
    }
}

impl<T: EstimatedMemorySize> EstimatedMemorySize for std::cell::Ref<'_, T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self)
    }
}

impl<T: EstimatedMemorySize> EstimatedMemorySize for std::cell::RefMut<'_, T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self)
    }
}

impl<T: EstimatedMemorySize + ToOwned + ?Sized> EstimatedMemorySize for std::borrow::Cow<'_, T> {
    #[inline]
    fn estimated_memory_size(&self) -> usize {
        T::estimated_memory_size(self.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::{Bytes, BytesMut};

    #[test]
    fn test_primitives() {
        assert_eq!(().estimated_memory_size(), 0);

        let slice: &[u8] = &[1, 2, 3, 4, 5];
        assert_eq!(slice.estimated_memory_size(), 5);
    }

    #[test]
    fn test_string_and_bytes() {
        assert_eq!("hello".to_string().estimated_memory_size(), 5);
        assert_eq!(Bytes::from_static(b"world").estimated_memory_size(), 5);
        assert_eq!(BytesMut::from(&b"test"[..]).estimated_memory_size(), 4);
    }

    #[test]
    fn test_option() {
        let none: Option<String> = None;
        assert_eq!(none.estimated_memory_size(), 0);

        let some = Some("hello".to_string());
        assert_eq!(some.estimated_memory_size(), 5);
    }

    #[test]
    fn test_vec_and_slice() {
        let vec: Vec<String> = vec!["hello".to_string(), "world".to_string()];
        assert_eq!(vec.estimated_memory_size(), 10);

        let slice: &[String] = &vec;
        assert_eq!(slice.estimated_memory_size(), 10);
    }

    #[test]
    fn test_references() {
        let s = "hello".to_string();
        assert_eq!((&s).estimated_memory_size(), 5);
        assert_eq!((&&s).estimated_memory_size(), 5);

        let mut s2 = "world".to_string();
        assert_eq!((&mut s2).estimated_memory_size(), 5);
    }

    #[test]
    fn test_smart_pointers() {
        use std::borrow::Cow;
        use std::rc::Rc;
        use std::sync::Arc;

        // Box
        let boxed = Box::new("hello".to_string());
        assert_eq!(boxed.estimated_memory_size(), 5);

        // Arc
        let arc = Arc::new("world".to_string());
        assert_eq!(arc.estimated_memory_size(), 5);

        // Rc
        let rc = Rc::new("test".to_string());
        assert_eq!(rc.estimated_memory_size(), 4);

        // Cow
        let cow_borrowed: Cow<'_, str> = Cow::Borrowed("borrowed");
        assert_eq!(cow_borrowed.estimated_memory_size(), 8);

        let cow_owned: Cow<'_, str> = Cow::Owned("owned".to_string());
        assert_eq!(cow_owned.estimated_memory_size(), 5);
    }
}
