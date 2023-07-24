use bytes::Bytes;
use restate_types::identifiers::ServiceId;
use std::future::Future;

/// Container for the eager state returned by [`StateReader`]
pub struct EagerState<I> {
    iterator: I,
    partial: bool,
}

impl<I: Default> Default for EagerState<I> {
    fn default() -> Self {
        Self {
            iterator: I::default(),
            partial: true,
        }
    }
}

impl<I> EagerState<I> {
    /// Create an [`EagerState`] where the provided iterator contains only a subset of entries of the given service instance.
    pub fn new_partial(iterator: I) -> Self {
        Self {
            iterator,
            partial: true,
        }
    }

    /// Create an [`EagerState`] where the provided iterator contains all the set of entries of the given service instance.
    pub fn new_complete(iterator: I) -> Self {
        Self {
            iterator,
            partial: false,
        }
    }

    /// If true, it is not guaranteed the iterator will return all the entries for the given service instance.
    pub fn is_partial(&self) -> bool {
        self.partial
    }

    pub fn map<U, F: FnOnce(I) -> U>(self, f: F) -> EagerState<U> {
        EagerState {
            iterator: f(self.iterator),
            partial: self.partial,
        }
    }
}

impl<I: Iterator<Item = (Bytes, Bytes)>> IntoIterator for EagerState<I> {
    type Item = (Bytes, Bytes);
    type IntoIter = I;

    fn into_iter(self) -> Self::IntoIter {
        self.iterator
    }
}

pub trait StateReader {
    type StateIter: Iterator<Item = (Bytes, Bytes)>;
    type Error: std::error::Error + Send + Sync + 'static;
    type Future<'a>: Future<Output = Result<EagerState<Self::StateIter>, Self::Error>> + Send
    where
        Self: 'a;

    fn read_state<'a>(&'a self, service_id: &'a ServiceId) -> Self::Future<'_>;
}

#[cfg(any(test, feature = "mocks"))]
pub mod mocks {
    use crate::{EagerState, StateReader};
    use bytes::Bytes;
    use restate_types::identifiers::ServiceId;
    use std::convert::Infallible;
    use std::iter::empty;

    #[derive(Debug, Clone)]
    pub struct EmptyStateReader;

    impl StateReader for EmptyStateReader {
        type StateIter = std::iter::Empty<(Bytes, Bytes)>;
        type Error = Infallible;
        type Future<'a> = futures::future::Ready<Result<EagerState<Self::StateIter>, Self::Error>> where
            Self: 'a;

        fn read_state<'a>(&'a self, _service_id: &'a ServiceId) -> Self::Future<'_> {
            futures::future::ready(Ok(EagerState::new_complete(empty())))
        }
    }
}
