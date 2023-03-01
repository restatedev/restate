use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::future::poll_fn;
use tokio::sync::mpsc;

pub use input::*;
pub use target::*;

#[derive(Default, Debug, thiserror::Error)]
#[error("closed")]
pub struct ClosedError;

pub trait PipeInput<T> {
    fn poll_recv(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, ClosedError>>;
}

pub trait PipeTarget<U> {
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ClosedError>>;
    fn send(self: Pin<&mut Self>, u: U) -> Result<(), ClosedError>;
}

pub struct Pipe<T, In, U, Target, Mapper, MapperFut> {
    pipe_input: In,
    pipe_target: Target,
    mapper: Mapper,

    _mapper_fut: PhantomData<MapperFut>,
    _t: PhantomData<T>,
    _u: PhantomData<U>,
}

impl<T, In, U, Target, Mapper, MapperFut> Pipe<T, In, U, Target, Mapper, MapperFut>
where
    In: PipeInput<T>,
    Target: PipeTarget<U>,
    Mapper: FnMut(T) -> MapperFut,
    MapperFut: Future<Output = U>,
{
    pub fn new(pipe_input: In, pipe_target: Target, mapper: Mapper) -> Self {
        Self {
            pipe_input,
            pipe_target,
            mapper,
            _mapper_fut: Default::default(),
            _t: Default::default(),
            _u: Default::default(),
        }
    }

    /// Returns when either pipe target, or input channel are closed
    pub async fn run(self) {
        let Pipe {
            pipe_input,
            pipe_target,
            mut mapper,
            ..
        } = self;

        tokio::pin!(pipe_input, pipe_target);

        while let Ok(()) = poll_fn(|cx| pipe_target.as_mut().poll_ready(cx)).await {
            let t = match poll_fn(|cx| pipe_input.as_mut().poll_recv(cx)).await {
                Ok(t) => t,
                Err(_) => {
                    return;
                }
            };

            let u = mapper(t).await;
            if pipe_target.as_mut().send(u).is_err() {
                return;
            }
        }
    }
}

mod input {
    use super::*;

    pub struct ReceiverPipeInput<T>(mpsc::Receiver<T>);

    impl<T> ReceiverPipeInput<T> {
        pub fn new(rx: mpsc::Receiver<T>) -> Self {
            Self(rx)
        }
    }

    impl<T> PipeInput<T> for ReceiverPipeInput<T> {
        fn poll_recv(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, ClosedError>> {
            self.get_mut()
                .0
                .poll_recv(cx)
                .map(|opt| opt.ok_or(ClosedError))
        }
    }

    pub struct UnboundedReceiverPipeInput<T>(mpsc::UnboundedReceiver<T>);

    impl<T> UnboundedReceiverPipeInput<T> {
        pub fn new(rx: mpsc::UnboundedReceiver<T>) -> Self {
            Self(rx)
        }
    }

    impl<T> PipeInput<T> for UnboundedReceiverPipeInput<T> {
        fn poll_recv(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, ClosedError>> {
            self.get_mut()
                .0
                .poll_recv(cx)
                .map(|opt| opt.ok_or(ClosedError))
        }
    }
}

mod target {
    use super::*;

    use pin_project::pin_project;

    #[pin_project(project = SenderPipeTargetStateProj)]
    pub enum SenderPipeTargetState<T, Fut> {
        Idle(Option<mpsc::Sender<T>>),
        Acquiring(#[pin] Fut),
        ReadyToSend(Option<mpsc::OwnedPermit<T>>),
        Closed,
    }

    #[pin_project]
    pub struct SenderPipeTarget<T, AcquireFn, Fut> {
        acquire_fn: AcquireFn,
        #[pin]
        state: SenderPipeTargetState<T, Fut>,
    }

    pub fn new_sender_pipe_target<T>(tx: mpsc::Sender<T>) -> impl PipeTarget<T> {
        SenderPipeTarget {
            acquire_fn: mpsc::Sender::reserve_owned,
            state: SenderPipeTargetState::Idle(Some(tx)),
        }
    }

    impl<T, AcquireFn, Fut> PipeTarget<T> for SenderPipeTarget<T, AcquireFn, Fut>
    where
        AcquireFn: Fn(mpsc::Sender<T>) -> Fut,
        Fut: Future<Output = Result<mpsc::OwnedPermit<T>, mpsc::error::SendError<()>>>,
    {
        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ClosedError>> {
            let this = self.project();
            let acquire_fn = this.acquire_fn;
            let mut state = this.state;

            loop {
                let projected_state = state.as_mut().project();

                let new_state = match projected_state {
                    SenderPipeTargetStateProj::Idle(tx) => {
                        SenderPipeTargetState::Acquiring((acquire_fn)(tx.take().unwrap()))
                    }
                    SenderPipeTargetStateProj::Acquiring(fut) => match ready!(fut.poll(cx)) {
                        Ok(permit) => SenderPipeTargetState::ReadyToSend(Some(permit)),
                        Err(_) => SenderPipeTargetState::Closed,
                    },
                    SenderPipeTargetStateProj::ReadyToSend(_) => return Poll::Ready(Ok(())),
                    SenderPipeTargetStateProj::Closed => return Poll::Ready(Err(ClosedError)),
                };

                state.set(new_state);
            }
        }

        fn send(self: Pin<&mut Self>, t: T) -> Result<(), ClosedError> {
            let this = self.project();
            let mut state = this.state;

            let permit = match state.as_mut().project() {
                SenderPipeTargetStateProj::ReadyToSend(permit) => permit.take().unwrap(),
                SenderPipeTargetStateProj::Closed => return Err(ClosedError),
                _ => {
                    panic!("Target is not ready");
                }
            };

            let sender = permit.send(t);
            state.set(SenderPipeTargetState::Idle(Some(sender)));

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pipe_bounded_bounded() {
        let (source_tx, source_rx) = mpsc::channel(1);
        let (sink_tx, mut sink_rx) = mpsc::channel(1);

        // Create the pipe and spawn it
        let pipe = Pipe::new(
            ReceiverPipeInput::new(source_rx),
            new_sender_pipe_target(sink_tx),
            |i| futures::future::ready(i + 1),
        );
        let handle = tokio::spawn(pipe.run());

        // Send and receive
        source_tx.send(0_u32).await.unwrap();
        let res: u32 = sink_rx.recv().await.unwrap();
        assert_eq!(res, 1_u32);

        // Close
        drop(sink_rx);
        drop(source_tx);
        handle.await.unwrap();
    }
}
