// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::fmt::Debug;

use tokio::sync::oneshot;

// --- Better integration with the command type

pub type UnboundedCommandSender<T, R> = tokio::sync::mpsc::UnboundedSender<Command<T, R>>;
pub type UnboundedCommandReceiver<T, R> = tokio::sync::mpsc::UnboundedReceiver<Command<T, R>>;

/// See [`Command`].
pub struct CommandResponseSender<T> {
    tx: Option<oneshot::Sender<T>>,
}

impl<T> CommandResponseSender<T> {
    pub fn send(self, t: T) -> Result<(), T> {
        if let Some(tx) = self.tx {
            tx.send(t)
        } else {
            Ok(())
        }
    }

    pub fn is_closed(&self) -> bool {
        match &self.tx {
            Some(tx) => tx.is_closed(),
            _ => true,
        }
    }

    pub async fn aborted(&mut self) {
        if let Some(tx) = &mut self.tx {
            tx.closed().await
        }
    }
}

pub type CommandResponseReceiver<T> = oneshot::Receiver<T>;

/// A [`Command`] is a type modelling the interaction between two async components (e.g. two [`EventLoop`]s).
/// You should use this type when the command's processor needs a way to reply back to the command's issuer.
///
/// A command lifecycle works as follows:
/// 1. Component **A** creates a new [`Command`] with [`Command::prepare`], providing the payload.
/// 2. Component **A** sends the command instance through channel to Component **B**.
/// 3. (Optional) Component **A** awaits for the command response using the provided [`CommandResponseReceiver`].
/// 4. Component **B** reads the command and unpacks it with [`Command::into_inner`].
/// 5. Component **B** sends the response back through the provided [`CommandResponseSender`].
/// 6. (Optional) Component **A** process the command response.
///
/// If the command issuer does not need an ack back from the command processor,
/// It can create the command by using [`Command::fire_and_forget`].
///
/// A complete example:
///
/// ```
/// use tokio::sync::mpsc;
/// use tokio::task::JoinHandle;
///
/// use restate_futures_util::command::*;
///
/// let (commands_tx, mut commands_rx) = mpsc::unbounded_channel();
///
/// let component_a = async move {
///     // Prepare the command
///     let (cmd, cmd_response_rx) = Command::prepare(10);
///
///     // Send it through the channel provided previously by component B
///     commands_tx.send(cmd).unwrap();
///
///     // Await for the response (optional)
///     let res: i32 = cmd_response_rx.await.unwrap();
///
///     // Or alternatively just use:
///     // commands_tx.send_and_wait_reply(10).await
/// };
///
/// let component_b = async move {
///     // Receive the commands (usually inside a loop)
///     let (payload, command_response_tx) = commands_rx.recv().await.unwrap().into_inner();
///     // Process the payload
///     let response_payload = payload * 2;
///     // Send the command response
///     command_response_tx.send(response_payload).unwrap();
/// };
/// ```
pub struct Command<T: Send, R: Send> {
    payload: T,
    response_tx: CommandResponseSender<R>,
}

impl<T: Send, R: Send> Command<T, R> {
    /// Prepare a new command to be sent.
    pub fn prepare(payload: T) -> (Self, CommandResponseReceiver<R>) {
        let (response_tx, response_rx) = oneshot::channel();
        (
            Self {
                payload,
                response_tx: CommandResponseSender {
                    tx: Some(response_tx),
                },
            },
            response_rx,
        )
    }

    /// Prepare a new fire and forget command to be sent.
    ///
    /// This command won't have the ack signal.
    pub fn fire_and_forget(payload: T) -> Self {
        Self {
            payload,
            response_tx: CommandResponseSender { tx: None },
        }
    }

    /// Borrow the payload.
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// Reply back.
    pub fn reply(self, response: R) -> Result<(), R> {
        self.response_tx.send(response)
    }

    /// Destructure the command, in order to process it.
    pub fn into_inner(self) -> (T, CommandResponseSender<R>) {
        (self.payload, self.response_tx)
    }

    /// Map the payload.
    pub fn map_payload<U: Send>(self, map_fn: impl FnOnce(T) -> U) -> Command<U, R> {
        Command {
            payload: map_fn(self.payload),
            response_tx: self.response_tx,
        }
    }
}

impl<T, R> Debug for Command<T, R>
where
    T: Send + Debug,
    R: Send,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Command(payload: {:?})", self.payload)
    }
}

impl<T: Send, R: Send> From<Command<T, R>> for (T, CommandResponseSender<R>) {
    fn from(cmd: Command<T, R>) -> Self {
        cmd.into_inner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use test_log::test;
    use tokio::sync::mpsc;

    use restate_test_util::assert_eq;

    #[test(tokio::test)]
    async fn test_back_and_forth() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let handle_command_tx = tokio::spawn(async move {
            let (cmd, cmd_response_rx) = Command::prepare(10);
            tx.send(cmd).unwrap();

            let res: i32 = cmd_response_rx.await.unwrap();
            res
        });

        tokio::spawn(async move {
            let (payload, command_response_tx) = rx.recv().await.unwrap().into();
            command_response_tx.send(payload * 2).unwrap();
        });

        assert_eq!(handle_command_tx.await.unwrap(), 20);
    }
}
