use std::future::Future;
use std::task::{Context, Poll};

use tower::Service;

/// A variant of [`tower::service_fn`] that can be used once
pub(super) fn service_fn_once<T>(f: T) -> ServiceFnOnce<T> {
    ServiceFnOnce { f: Some(f) }
}

// TODO this clone makes absolutely no sense, but we need it because of
//  https://github.com/hyperium/tonic/issues/1290
#[derive(Clone)]
pub(super) struct ServiceFnOnce<T> {
    f: Option<T>,
}

impl<T, F, Request, R, E> Service<Request> for ServiceFnOnce<T>
where
    T: FnOnce(Request) -> F,
    F: Future<Output = Result<R, E>>,
{
    type Response = R;
    type Error = E;
    type Future = F;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), E>> {
        Ok(()).into()
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let f = self
            .f
            .take()
            .expect("Cannot use the ServiceFnOnce more than once");
        f(req)
    }
}
