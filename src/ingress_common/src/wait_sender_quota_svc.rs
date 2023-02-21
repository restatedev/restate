use std::mem;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::FutureExt;
use tokio::sync::mpsc;
use tower::Service;

pub struct WaitSenderPermitService<T, UnavailableFn, S> {
    unavailable_fn: Box<UnavailableFn>,
    inner: S,

    state: WaitSenderState<T>,
}

enum WaitSenderState<T> {
    Idle(mpsc::Sender<T>),
    Acquiring(BoxFuture<'static, Result<mpsc::OwnedPermit<T>, mpsc::error::SendError<()>>>),
    ReadyToSend(mpsc::OwnedPermit<T>),
    Closed,
}

impl<T, S, UnavailableFn> WaitSenderPermitService<T, UnavailableFn, S> {
    #[allow(unused)]
    pub fn new(tx: mpsc::Sender<T>, f: UnavailableFn, inner: S) -> Self {
        Self {
            inner,
            unavailable_fn: Box::new(f),
            state: WaitSenderState::Idle(tx),
        }
    }
}

impl<T, UnavailableFn, S, E, Res, Req> Service<Req> for WaitSenderPermitService<T, UnavailableFn, S>
where
    T: Send + 'static,
    S: Service<(Req, mpsc::OwnedPermit<T>), Error = E, Response = Res>,
    UnavailableFn: Fn() -> E,
{
    type Response = Res;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            match mem::replace(&mut self.state, WaitSenderState::Closed) {
                WaitSenderState::Idle(tx) => {
                    self.state = WaitSenderState::Acquiring(tx.reserve_owned().boxed());
                }
                WaitSenderState::Acquiring(mut f) => {
                    let res = match f.poll_unpin(cx) {
                        Poll::Ready(r) => r,
                        Poll::Pending => {
                            self.state = WaitSenderState::Acquiring(f);
                            return Poll::Pending;
                        }
                    };
                    match res {
                        Ok(permit) => {
                            self.state = WaitSenderState::ReadyToSend(permit);
                            break;
                        }
                        Err(_) => {
                            self.state = WaitSenderState::Closed;
                        }
                    }
                }
                WaitSenderState::ReadyToSend(permit) => {
                    self.state = WaitSenderState::ReadyToSend(permit);
                    break;
                }
                WaitSenderState::Closed => return Poll::Ready(Err((self.unavailable_fn)())),
            }
        }
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let permit = match mem::replace(&mut self.state, WaitSenderState::Closed) {
            WaitSenderState::ReadyToSend(p) => p,
            _ => {
                panic!("Invoked call before poll_ready")
            }
        };

        self.inner.call((req, permit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::future::ok;
    use test_utils::assert;
    use tower::util::*;

    struct MockSvc;

    impl Service<((), mpsc::OwnedPermit<()>)> for MockSvc {
        type Response = ();
        type Error = ();
        type Future = futures::future::Ready<Result<(), ()>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: ((), mpsc::OwnedPermit<()>)) -> Self::Future {
            req.1.send(());
            ok(())
        }
    }

    fn unavailable() {}

    #[tokio::test]
    async fn test_contention() {
        let (tx, mut rx) = mpsc::channel(1);

        let svc_1 = WaitSenderPermitService::new(tx.clone(), unavailable, MockSvc);
        let svc_2 = WaitSenderPermitService::new(tx, unavailable, MockSvc);

        let ready_1 = svc_1.ready_oneshot();
        let mut ready_2 = svc_2.ready_oneshot();

        let svc_1_ready = ready_1.await.unwrap();

        // If svc_1 is ready, svc_2 shouldn't be
        let waker = futures::task::noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        assert!(let Poll::Pending = ready_2.poll_unpin(&mut cx));

        svc_1_ready.oneshot(()).await.unwrap();
        rx.recv().await.unwrap();

        let svc_2_ready = ready_2.await.unwrap();
        svc_2_ready.oneshot(()).await.unwrap();
        rx.recv().await.unwrap();
    }
}
