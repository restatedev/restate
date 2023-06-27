use futures::Stream;
use restate_common::journal::raw::PlainRawEntry;
use restate_common::types::{JournalMetadata, ServiceInvocationId};
use std::future::Future;

pub trait JournalReader {
    type JournalStream: Stream<Item = PlainRawEntry>;
    type Error: std::error::Error + Send + Sync + 'static;
    type Future<'a>: Future<Output = Result<(JournalMetadata, Self::JournalStream), Self::Error>>
        + Send
    where
        Self: 'a;

    fn read_journal<'a>(&'a self, sid: &'a ServiceInvocationId) -> Self::Future<'_>;
}

#[cfg(any(test, feature = "mocks"))]
pub mod mocks {
    use super::*;
    use restate_common::types::ServiceInvocationSpanContext;
    use std::convert::Infallible;

    #[derive(Debug, Clone)]
    pub struct EmptyJournalReader;

    impl JournalReader for EmptyJournalReader {
        type JournalStream = futures::stream::Empty<PlainRawEntry>;
        type Error = Infallible;
        type Future<'a> = futures::future::Ready<Result<(JournalMetadata, Self::JournalStream), Self::Error>> where
            Self: 'a;

        fn read_journal<'a>(&'a self, _sid: &'a ServiceInvocationId) -> Self::Future<'_> {
            futures::future::ready(Ok((
                JournalMetadata::new("test", ServiceInvocationSpanContext::empty(), 0),
                futures::stream::empty(),
            )))
        }
    }
}
