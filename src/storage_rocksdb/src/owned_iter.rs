use crate::DBIterator;
use bytes::{BufMut, Bytes, BytesMut};

pub struct OwnedIterator<'a> {
    iter: DBIterator<'a>,
    arena: BytesMut,
}

impl<'a> OwnedIterator<'a> {
    pub(crate) fn new(iter: DBIterator<'a>) -> Self {
        Self {
            iter,
            arena: BytesMut::with_capacity(8196),
        }
    }
}

impl<'a> Iterator for OwnedIterator<'a> {
    type Item = (Bytes, Bytes);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.arena.reserve(8192);

        if let Some((k, v)) = self.iter.item() {
            self.arena.put_slice(k);
            let key = self.arena.split().freeze();
            self.arena.put_slice(v);
            let value = self.arena.split().freeze();
            self.iter.next();
            Some((key, value))
        } else {
            None
        }
    }
}
