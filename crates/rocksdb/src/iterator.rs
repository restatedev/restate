// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

pub enum IterAction {
    Seek(Bytes),
    Prev,
    Next,
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

impl State<'_> {
    fn seek(&mut self, key: Bytes) {
        match self {
            State::Active { next_step, .. } => {
                *next_step = IterAction::Seek(key);
            }
            State::Terminated => {}
        }
    }

    fn next(&mut self) {
        match self {
            State::Active { next_step, .. } => {
                *next_step = IterAction::Next;
            }
            State::Terminated => {}
        }
    }

    fn prev(&mut self) {
        match self {
            State::Active { next_step, .. } => {
                *next_step = IterAction::Prev;
            }
            State::Terminated => {}
        }
    }

    fn seek_to_first(&mut self) {
        match self {
            State::Active { next_step, .. } => {
                *next_step = IterAction::SeekToFirst;
            }
            State::Terminated => {}
        }
    }

    fn seek_to_last(&mut self) {
        match self {
            State::Active { next_step, .. } => {
                *next_step = IterAction::SeekToLast;
            }
            State::Terminated => {}
        }
    }
}

impl<'a, F> RocksIterator<'a, F>
where
    F: Fn(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send + 'static,
{
    pub fn new(iterator: DBRawIteratorWithThreadMode<'a, DB>, f: F) -> Self {
        Self {
            f,
            state: State::Active {
                iterator,
                next_step: IterAction::SeekToFirst,
            },
        }
    }

    pub fn seek(&mut self, key: Bytes) {
        self.state.seek(key);
    }

    #[allow(dead_code)]
    pub fn next(&mut self) {
        self.state.next();
    }

    #[allow(dead_code)]
    pub fn prev(&mut self) {
        self.state.prev();
    }

    #[allow(dead_code)]
    pub fn seek_to_first(&mut self) {
        self.state.seek_to_first();
    }

    #[allow(dead_code)]
    pub fn seek_to_last(&mut self) {
        self.state.seek_to_last();
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
