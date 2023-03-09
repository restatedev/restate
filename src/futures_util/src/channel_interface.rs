use crate::command::Command;

#[derive(Default, Debug, thiserror::Error)]
#[error("closed channel")]
pub struct ClosedError;

pub enum RequestResponseSender<T: Send, R: Send> {
    Bounded(tokio::sync::mpsc::Sender<Command<T, R>>),
    Unbounded(tokio::sync::mpsc::UnboundedSender<Command<T, R>>),
}

impl<T: Send, R: Send> Clone for RequestResponseSender<T, R> {
    fn clone(&self) -> Self {
        match self {
            RequestResponseSender::Bounded(s) => RequestResponseSender::Bounded(s.clone()),
            RequestResponseSender::Unbounded(s) => RequestResponseSender::Unbounded(s.clone()),
        }
    }
}

impl<T : Send, R: Send> RequestResponseSender<T, R> {
    pub async fn send_and_receive(&self, t: T) -> Result<R, ClosedError> {
        let (cmd, mut response_tx) = Command::prepare(t);
        match self {
            RequestResponseSender::Bounded(s) => s.send(cmd).await.map_err(|_| ClosedError)?,
            RequestResponseSender::Unbounded(s) => s.send(cmd).map_err(|_| ClosedError)?,
        };

        response_tx.await.map_err(|_| ClosedError)
    }
}

impl<T: Send, R: Send> From<tokio::sync::mpsc::Sender<Command<T, R>>> for RequestResponseSender<T, R> {
    fn from(value: tokio::sync::mpsc::Sender<Command<T, R>>) -> Self {
        Self::Bounded(value)
    }
}

impl<T: Send, R: Send> From<tokio::sync::mpsc::UnboundedSender<Command<T, R>>> for RequestResponseSender<T, R> {
    fn from(value: tokio::sync::mpsc::UnboundedSender<Command<T, R>>) -> Self {
        Self::Unbounded(value)
    }
}

pub enum Sender<T: Send> {
    Bounded(tokio::sync::mpsc::Sender<T>),
    Unbounded(tokio::sync::mpsc::UnboundedSender<T>),
}

impl<T: Send> Clone for Sender<T> {
    fn clone(&self) -> Self {
        match self {
            Sender::Bounded(s) => Sender::Bounded(s.clone()),
            Sender::Unbounded(s) => Sender::Unbounded(s.clone()),
        }
    }
}

impl<T: Send> Sender<T> {
    pub async fn send(&self, t: T) -> Result<(), ClosedError> {
        match self {
            Sender::Bounded(s) => s.send(t).await.map_err(|_| ClosedError),
            Sender::Unbounded(s) => s.send(t).map_err(|_| ClosedError),
        }
    }
}

impl<T: Send> From<tokio::sync::mpsc::Sender<T>> for Sender<T> {
    fn from(value: tokio::sync::mpsc::Sender<T>) -> Self {
        Self::Bounded(value)
    }
}

impl<T: Send> From<tokio::sync::mpsc::UnboundedSender<T>> for Sender<T> {
    fn from(value: tokio::sync::mpsc::UnboundedSender<T>) -> Self {
        Self::Unbounded(value)
    }
}
