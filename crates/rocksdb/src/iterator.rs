// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use rocksdb::{DB, DBRawIteratorWithThreadMode};

use crate::RocksError;

pub enum Disposition {
    // No value to emit, continue driving the iterator
    Continue,
    // Returned when the iterator will block _if_ it's configured
    // to be non blocking (via ReadOptions::set_read_tier)
    WouldBlock,
    /// Iterator has terminated. No more values will be emitted
    /// This is a terminal state.
    Stop,
}

#[derive(Default, Clone, Eq, PartialEq)]
pub enum IterAction {
    Seek(Bytes),
    SeekToPrev(Bytes),
    Prev,
    Next,
    #[default]
    SeekToFirst,
    SeekToLast,
    // Stop the iterator
    Stop,
}

enum State<'a> {
    Active {
        iterator: DBRawIteratorWithThreadMode<'a, DB>,
        next_step: IterAction,
    },
    Terminated,
}

pub struct RocksIterator<'a, F> {
    state: State<'a>,
    f: F,
}

impl<'a, F> RocksIterator<'a, F>
where
    F: FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send + 'static,
{
    pub fn new(iterator: DBRawIteratorWithThreadMode<'a, DB>, next_step: IterAction, f: F) -> Self {
        Self {
            f,
            state: State::Active {
                iterator,
                next_step,
            },
        }
    }

    /// Drive the iterator one step at a time
    pub fn step(&mut self) -> Disposition {
        let (iterator, next_step) = match &mut self.state {
            State::Active {
                iterator,
                next_step,
                ..
            } => {
                match next_step {
                    IterAction::Seek(items) => iterator.seek(items),
                    IterAction::SeekToPrev(items) => iterator.seek_for_prev(items),
                    IterAction::Prev => iterator.prev(),
                    IterAction::Next => iterator.next(),
                    IterAction::SeekToFirst => iterator.seek_to_first(),
                    IterAction::SeekToLast => iterator.seek_to_last(),
                    IterAction::Stop => {
                        self.state = State::Terminated;
                        return Disposition::Stop;
                    }
                }
                (iterator, next_step)
            }
            State::Terminated => return Disposition::Stop,
        };

        let Some((key, value)) = iterator.item() else {
            match iterator.status() {
                Ok(()) => {
                    self.state = State::Terminated;
                    return Disposition::Stop;
                }
                Err(e) => {
                    if e.kind() == rocksdb::ErrorKind::Incomplete {
                        return Disposition::WouldBlock;
                    }
                    // todo: perhaps in the future we can allow the user to decide
                    // to retry on IO errors. But for now, we ignore the returned
                    // action and always terminate.
                    let _ = (self.f)(Err(RocksError::from(e)));
                    self.state = State::Terminated;
                    return Disposition::Stop;
                }
            }
        };

        // call the user's function
        *next_step = (self.f)(Ok((key, value)));
        Disposition::Continue
    }
}
