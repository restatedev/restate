use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::future::poll_fn;
use tokio::sync::mpsc;

pub use input::*;
pub use multi_target::*;
pub use target::*;

#[derive(Default, Debug, thiserror::Error)]
#[error("closed")]
pub struct ClosedError;

/// This trait represents the input side of the pipe.
pub trait PipeInput<T> {
    /// Poll the input to receive a new element.
    fn poll_recv(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, ClosedError>>;
}

/// This trait represents the output side of the pipe.
///
/// The [`PipeTarget`] transitions through 3 states:
///
/// * _Idle_
/// * _Ready_
/// * _Closed_
///
/// The state becomes _Ready_ when [`Self::poll_ready`] returns [`Poll::Ready`] with [`Ok`].
/// After sending a message with [`Self::send`], the state transitions back to _Idle_,
/// requiring to invoke [`Self::poll_ready`] again before the next [`Self::send`].
///
/// Both [`Self::poll_ready`] and [`Self::send`] return [`ClosedError`] if the backing target is closed.
pub trait PipeTarget<U> {
    /// Returns [`Poll::Ready`] with [`Ok`] if the [`PipeTarget`] is ready to [`Self::send`] messages.
    ///
    /// Might be invoked multiple times after it's _Ready_.
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ClosedError>>;

    /// Send the message.
    ///
    /// Panics if the [`PipeTarget`] is _Idle_, meaning there wasn't a previous successful call to [`Self::poll_ready`].
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

    pub fn new_sender_pipe_target<T>(tx: mpsc::Sender<T>) -> impl PipeTarget<T> {
        SenderPipeTarget {
            acquire_fn: mpsc::Sender::reserve_owned,
            state: SenderPipeTargetState::Idle(Some(tx)),
        }
    }

    #[pin_project(project = SenderPipeTargetStateProj)]
    enum SenderPipeTargetState<T, Fut> {
        Idle(Option<mpsc::Sender<T>>),
        Acquiring(#[pin] Fut),
        ReadyToSend(Option<mpsc::OwnedPermit<T>>),
        Closed,
    }

    #[pin_project]
    struct SenderPipeTarget<T, AcquireFn, Fut> {
        acquire_fn: AcquireFn,
        #[pin]
        state: SenderPipeTargetState<T, Fut>,
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

mod multi_target {
    use super::*;

    use pin_project::pin_project;

    pub enum EitherTarget<T1, T2> {
        Left(T1),
        Right(T2),
    }

    #[derive(Copy, Clone, PartialEq, Debug)]
    pub enum PipeTargetState {
        Idle,
        Ready,
        Closed,
    }

    #[pin_project]
    pub struct EitherPipeTarget<PT1, PT2> {
        #[pin]
        left_target: PT1,
        #[pin]
        right_target: PT2,

        left_state: PipeTargetState,
        right_state: PipeTargetState,
    }

    impl<PT1, PT2> EitherPipeTarget<PT1, PT2> {
        pub fn new(left_target: PT1, right_target: PT2) -> Self {
            EitherPipeTarget {
                left_target,
                right_target,
                left_state: PipeTargetState::Idle,
                right_state: PipeTargetState::Idle,
            }
        }
    }

    impl<T1, T2, PT1, PT2> PipeTarget<EitherTarget<T1, T2>> for EitherPipeTarget<PT1, PT2>
    where
        PT1: PipeTarget<T1>,
        PT2: PipeTarget<T2>,
    {
        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ClosedError>> {
            let this = self.project();
            let mut left_target = this.left_target;
            let mut right_target = this.right_target;

            loop {
                match (*this.left_state, *this.right_state) {
                    (PipeTargetState::Closed, _) | (_, PipeTargetState::Closed) => {
                        return Poll::Ready(Err(ClosedError))
                    }
                    (PipeTargetState::Idle, _) => {
                        *this.left_state = match ready!(left_target.as_mut().poll_ready(cx)) {
                            Ok(()) => PipeTargetState::Ready,
                            Err(_) => PipeTargetState::Closed,
                        };
                    }
                    (_, PipeTargetState::Idle) => {
                        *this.right_state = match ready!(right_target.as_mut().poll_ready(cx)) {
                            Ok(()) => PipeTargetState::Ready,
                            Err(_) => PipeTargetState::Closed,
                        };
                    }
                    (PipeTargetState::Ready, PipeTargetState::Ready) => return Poll::Ready(Ok(())),
                }
            }
        }

        fn send(self: Pin<&mut Self>, msg: EitherTarget<T1, T2>) -> Result<(), ClosedError> {
            let this = self.project();
            let left_target = this.left_target;
            let right_target = this.right_target;

            return match (*this.left_state, *this.right_state, msg) {
                (PipeTargetState::Closed, _, EitherTarget::Left(_))
                | (_, PipeTargetState::Closed, EitherTarget::Right(_)) => Err(ClosedError),
                (PipeTargetState::Ready, _, EitherTarget::Left(left_msg)) => {
                    *this.left_state = match left_target.send(left_msg) {
                        Ok(_) => PipeTargetState::Idle,
                        Err(_) => PipeTargetState::Closed,
                    };
                    Ok(())
                }
                (_, PipeTargetState::Ready, EitherTarget::Right(right_msg)) => {
                    *this.right_state = match right_target.send(right_msg) {
                        Ok(_) => PipeTargetState::Idle,
                        Err(_) => PipeTargetState::Closed,
                    };
                    Ok(())
                }
                _ => {
                    panic!("Unexpected state")
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pipe_bounded_to_bounded() {
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

    #[tokio::test]
    async fn pipe_bounded_to_multi_bounded() {
        let (source_tx, source_rx) = mpsc::channel(2);
        let (sink_left_tx, mut sink_left_rx) = mpsc::channel(1);
        let (sink_right_tx, mut sink_right_rx) = mpsc::channel(1);

        // Create the pipe and spawn it
        let pipe = Pipe::new(
            ReceiverPipeInput::new(source_rx),
            EitherPipeTarget::new(
                new_sender_pipe_target(sink_left_tx),
                new_sender_pipe_target(sink_right_tx),
            ),
            |mut i| {
                i += 1;
                futures::future::ready(if i % 2 == 0 {
                    EitherTarget::Left(i)
                } else {
                    EitherTarget::Right(i)
                })
            },
        );
        let handle = tokio::spawn(pipe.run());

        // Send and receive
        source_tx.send(0_u32).await.unwrap();
        source_tx.send(1_u32).await.unwrap();
        let res: u32 = sink_right_rx.recv().await.unwrap();
        assert_eq!(res, 1_u32);
        let res: u32 = sink_left_rx.recv().await.unwrap();
        assert_eq!(res, 2_u32);

        // Close
        drop(sink_left_rx);
        drop(sink_right_rx);
        drop(source_tx);
        handle.await.unwrap();
    }
}
