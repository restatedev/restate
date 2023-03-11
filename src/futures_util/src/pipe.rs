use std::future::{poll_fn, Future};
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use pin_project::pin_project;
use tokio::sync::mpsc;

pub use input::*;
pub use multi_target::*;
pub use target::*;

#[derive(Debug, Copy, Clone, PartialEq, thiserror::Error)]
pub enum PipeError {
    #[error("channel '{0}' closed")]
    ChannelClosed(&'static str),
}

/// This trait represents the input side of the pipe.
pub trait PipeInput<T> {
    /// Poll the input to receive a new element.
    fn poll_recv(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, PipeError>>;
}

/// This trait represents the output side of the pipe.
///
/// The [`PipeTarget`] transitions through 3 states:
///
/// * _Idle_
/// * _Ready_
/// * _Closed_
///
/// The state becomes _Ready_ when [`PipeTarget::poll_ready`] returns [`Poll::Ready`] with [`Ok`].
/// After sending a message with [`PipeTarget::send`], the state transitions back to _Idle_,
/// requiring to invoke [`PipeTarget::poll_ready`] again before the next [`PipeTarget::send`].
///
/// Both [`PipeTarget::poll_ready`] and [`PipeTarget::send`] return [`PipeError`] if the backing target is closed.
pub trait PipeTarget<U> {
    /// Returns [`Poll::Ready`] with [`Ok`] if the [`PipeTarget`] is ready to [`Self::send`] messages.
    ///
    /// Might be invoked multiple times after it's _Ready_.
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), PipeError>>;

    /// Send the message.
    ///
    /// Panics if the [`PipeTarget`] is _Idle_, meaning there wasn't a previous successful call to [`PipeTarget::poll_ready`].
    fn send(self: Pin<&mut Self>, u: U) -> Result<(), PipeError>;
}

#[derive(Copy, Clone, PartialEq, Debug)]
enum PipeState {
    Idle,
    Ready,
    Closed(PipeError),
}

/// [`Pipe`] is an abstraction to implement piping messages from one [`PipeInput`] to a [`PipeTarget`].
///
/// You can interact with the [`Pipe`]:
///
/// * Automatically using [`Pipe::run`], with a single future that polls the pipe in a loop until it's closed,
///   applying a mapper [`FnMut`] to each input message.
/// * Manually polling it, using [`Pipe::poll_next_input`] and [`Pipe::write`].
///
/// Use the manual polling API if between receiving and sending you need to mutate some data structure that the [`FnMut`] cannot own.
///
/// ## Manual polling API
///
/// The [`Pipe`] transitions through 3 states:
///
/// * _Idle_
/// * _Ready_
/// * _Closed_
///
/// The state becomes _Ready_ when [`Pipe::poll_next_input`] returns [`Poll::Ready`] with a value.
/// After sending a message with [`Pipe::write`], the state transitions back to _Idle_,
/// requiring to invoke [`Pipe::poll_next_input`] again before the next [`Pipe::write`].
#[pin_project]
pub struct Pipe<T, In, U, Target> {
    #[pin]
    pipe_input: In,
    #[pin]
    pipe_target: Target,

    state: PipeState,

    _t: PhantomData<T>,
    _u: PhantomData<U>,
}

impl<T, In, U, Target> Pipe<T, In, U, Target>
where
    In: PipeInput<T>,
    Target: PipeTarget<U>,
{
    pub fn new(pipe_input: In, pipe_target: Target) -> Self {
        Self {
            pipe_input,
            pipe_target,
            state: PipeState::Idle,
            _t: Default::default(),
            _u: Default::default(),
        }
    }

    pub fn poll_next_input(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<T, PipeError>> {
        let this = self.project();
        let input = this.pipe_input;
        let mut target = this.pipe_target;
        let state = this.state;

        // Loop to wait for the target to be ready
        loop {
            match state {
                PipeState::Idle => match ready!(target.as_mut().poll_ready(cx)) {
                    Ok(_) => {
                        *state = PipeState::Ready;
                    }
                    Err(err) => {
                        *state = PipeState::Closed(err);
                        return Poll::Ready(Err(err));
                    }
                },
                PipeState::Ready => break,
                PipeState::Closed(err) => return Poll::Ready(Err(*err)),
            }
        }

        Poll::Ready(match ready!(input.poll_recv(cx)) {
            Ok(t) => Ok(t),
            Err(err) => {
                *state = PipeState::Closed(err);
                Err(err)
            }
        })
    }

    /// Panics if the state of the [`Pipe`] is _Idle_, meaning there wasn't a previous successful call to [`Self::poll_next_input`].
    pub fn write(self: Pin<&mut Self>, u: U) -> Result<(), PipeError> {
        let this = self.project();
        let target = this.pipe_target;
        let state = this.state;

        return match state {
            PipeState::Idle => {
                panic!("Invoked write() before poll_next_input()")
            }
            PipeState::Ready => match target.send(u) {
                Ok(_) => Ok(()),
                Err(e) => {
                    *state = PipeState::Closed(e);
                    Err(e)
                }
            },
            PipeState::Closed(err) => Err(*err),
        };
    }

    /// Returns a future that polls the pipe in a loop until it's closed,
    /// applying a mapper to each input message.
    ///
    /// The future completes when either the pipe input or target is closed.
    pub async fn run<Mapper, MapperFut>(self, mut mapper: Mapper)
    where
        Mapper: FnMut(T) -> MapperFut,
        MapperFut: Future<Output = U>,
    {
        tokio::pin! {
            let pipe = self;
        }

        while let Ok(t) = poll_fn(|cx| pipe.as_mut().poll_next_input(cx)).await {
            let u = mapper(t).await;
            if pipe.as_mut().write(u).is_err() {
                return;
            }
        }
    }
}

mod input {
    use super::*;

    pub struct ReceiverPipeInput<T> {
        name: &'static str,
        rx: mpsc::Receiver<T>,
    }

    impl<T> ReceiverPipeInput<T> {
        pub fn new(rx: mpsc::Receiver<T>, name: &'static str) -> Self {
            Self { name, rx }
        }
    }

    impl<T> PipeInput<T> for ReceiverPipeInput<T> {
        fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, PipeError>> {
            self.as_mut()
                .rx
                .poll_recv(cx)
                .map(|opt| opt.ok_or(PipeError::ChannelClosed(self.name)))
        }
    }

    pub struct UnboundedReceiverPipeInput<T> {
        name: &'static str,
        rx: mpsc::UnboundedReceiver<T>,
    }

    impl<T> UnboundedReceiverPipeInput<T> {
        pub fn new(rx: mpsc::UnboundedReceiver<T>, name: &'static str) -> Self {
            Self { name, rx }
        }
    }

    impl<T> PipeInput<T> for UnboundedReceiverPipeInput<T> {
        fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<T, PipeError>> {
            self.as_mut()
                .rx
                .poll_recv(cx)
                .map(|opt| opt.ok_or(PipeError::ChannelClosed(self.name)))
        }
    }
}

mod target {
    use super::*;

    pub fn new_sender_pipe_target<T>(
        tx: mpsc::Sender<T>,
        name: &'static str,
    ) -> impl PipeTarget<T> {
        SenderPipeTarget {
            name,
            acquire_fn: mpsc::Sender::reserve_owned,
            state: SenderPipeTargetState::Idle(Some(tx)),
        }
    }

    #[pin_project(project = SenderPipeTargetStateProj)]
    enum SenderPipeTargetState<T, Fut> {
        Idle(Option<mpsc::Sender<T>>),
        Acquiring(#[pin] Fut),
        ReadyToSend(Option<mpsc::OwnedPermit<T>>),
        Closed(PipeError),
    }

    #[pin_project]
    struct SenderPipeTarget<T, AcquireFn, Fut> {
        name: &'static str,
        acquire_fn: AcquireFn,
        #[pin]
        state: SenderPipeTargetState<T, Fut>,
    }

    impl<T, AcquireFn, Fut> PipeTarget<T> for SenderPipeTarget<T, AcquireFn, Fut>
    where
        AcquireFn: Fn(mpsc::Sender<T>) -> Fut,
        Fut: Future<Output = Result<mpsc::OwnedPermit<T>, mpsc::error::SendError<()>>>,
    {
        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), PipeError>> {
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
                        Err(_) => {
                            SenderPipeTargetState::Closed(PipeError::ChannelClosed(this.name))
                        }
                    },
                    SenderPipeTargetStateProj::ReadyToSend(_) => return Poll::Ready(Ok(())),
                    SenderPipeTargetStateProj::Closed(err) => return Poll::Ready(Err(*err)),
                };

                state.set(new_state);
            }
        }

        fn send(self: Pin<&mut Self>, t: T) -> Result<(), PipeError> {
            let this = self.project();
            let mut state = this.state;

            let permit = match state.as_mut().project() {
                SenderPipeTargetStateProj::ReadyToSend(permit) => permit.take().unwrap(),
                SenderPipeTargetStateProj::Closed(err) => return Err(*err),
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

    pub enum EitherTarget<T1, T2> {
        Left(T1),
        Right(T2),
    }

    #[pin_project]
    pub struct EitherPipeTarget<PT1, PT2> {
        #[pin]
        left_target: PT1,
        #[pin]
        right_target: PT2,

        left_state: PipeState,
        right_state: PipeState,
    }

    impl<PT1, PT2> EitherPipeTarget<PT1, PT2> {
        pub fn new(left_target: PT1, right_target: PT2) -> Self {
            EitherPipeTarget {
                left_target,
                right_target,
                left_state: PipeState::Idle,
                right_state: PipeState::Idle,
            }
        }
    }

    impl<T1, T2, PT1, PT2> PipeTarget<EitherTarget<T1, T2>> for EitherPipeTarget<PT1, PT2>
    where
        PT1: PipeTarget<T1>,
        PT2: PipeTarget<T2>,
    {
        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), PipeError>> {
            let this = self.project();
            let mut left_target = this.left_target;
            let mut right_target = this.right_target;

            loop {
                match (*this.left_state, *this.right_state) {
                    (PipeState::Closed(err), _) | (_, PipeState::Closed(err)) => {
                        return Poll::Ready(Err(err))
                    }
                    (PipeState::Idle, _) => {
                        *this.left_state = match ready!(left_target.as_mut().poll_ready(cx)) {
                            Ok(()) => PipeState::Ready,
                            Err(err) => PipeState::Closed(err),
                        };
                    }
                    (_, PipeState::Idle) => {
                        *this.right_state = match ready!(right_target.as_mut().poll_ready(cx)) {
                            Ok(()) => PipeState::Ready,
                            Err(err) => PipeState::Closed(err),
                        };
                    }
                    (PipeState::Ready, PipeState::Ready) => return Poll::Ready(Ok(())),
                }
            }
        }

        fn send(self: Pin<&mut Self>, msg: EitherTarget<T1, T2>) -> Result<(), PipeError> {
            let this = self.project();
            let left_target = this.left_target;
            let right_target = this.right_target;

            return match (*this.left_state, *this.right_state, msg) {
                (PipeState::Closed(err), _, EitherTarget::Left(_))
                | (_, PipeState::Closed(err), EitherTarget::Right(_)) => Err(err),
                (PipeState::Ready, _, EitherTarget::Left(left_msg)) => {
                    let send_res = left_target.send(left_msg);
                    *this.left_state = match &send_res {
                        Ok(_) => PipeState::Idle,
                        Err(err) => PipeState::Closed(*err),
                    };
                    send_res
                }
                (_, PipeState::Ready, EitherTarget::Right(right_msg)) => {
                    let send_res = right_target.send(right_msg);
                    *this.right_state = match &send_res {
                        Ok(_) => PipeState::Idle,
                        Err(err) => PipeState::Closed(*err),
                    };
                    send_res
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
            ReceiverPipeInput::new(source_rx, "source_rx"),
            new_sender_pipe_target(sink_tx, "sink_tx"),
        );
        let handle = tokio::spawn(pipe.run(|i| futures::future::ready(i + 1)));

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
            ReceiverPipeInput::new(source_rx, "source_rx"),
            EitherPipeTarget::new(
                new_sender_pipe_target(sink_left_tx, "sink_left_tx"),
                new_sender_pipe_target(sink_right_tx, "sink_right_tx"),
            ),
        );
        let handle = tokio::spawn(pipe.run(|mut i| {
            i += 1;
            futures::future::ready(if i % 2 == 0 {
                EitherTarget::Left(i)
            } else {
                EitherTarget::Right(i)
            })
        }));

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
