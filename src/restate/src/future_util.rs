use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::process;
use std::task::{Context, Poll};

use futures_util::TryFuture;
use pin_project::pin_project;
use tracing::error;

pub(crate) trait TryProcessAbortFuture: TryFuture {
    /// Aborts the process if the future completes with an error.
    fn abort_on_err<T>(self, component: Option<T>) -> AbortOnErr<Self, T>
    where
        Self: Sized,
        T: AsRef<str>,
    {
        AbortOnErr {
            inner: self,
            component,
        }
    }
}

#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[pin_project]
pub(crate) struct AbortOnErr<Fut, T> {
    #[pin]
    inner: Fut,
    component: Option<T>,
}

impl<Fut, T> Future for AbortOnErr<Fut, T>
where
    Fut: TryFuture,
    <Fut as TryFuture>::Error: Debug,
    T: AsRef<str>,
{
    type Output = Fut::Ok;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if let Poll::Ready(result) = this.inner.try_poll(cx) {
            match result {
                Ok(ok) => Poll::Ready(ok),
                Err(err) => {
                    let component = this
                        .component
                        .as_ref()
                        .map_or("Unknown component", |x| x.as_ref());
                    error!(error = ?err, "{component} failed");
                    process::abort();
                }
            }
        } else {
            Poll::Pending
        }
    }
}

impl<Fut: TryFuture> TryProcessAbortFuture for Fut {}
