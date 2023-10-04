// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;
use std::vec::IntoIter;

use futures::future::BoxFuture;
use futures::{stream, FutureExt};
use prost::Message;
use restate_invoker_api::{
    EagerState, Effect, EffectKind, InvokeInputJournal, JournalMetadata, JournalReader,
    ServiceHandle, StateReader,
};
use restate_service_protocol::pb::protocol::PollInputStreamEntryMessage;
use restate_types::identifiers::{EntryIndex, FullInvocationId, ServiceId};
use restate_types::invocation::{ServiceInvocationSpanContext, SpanRelation};
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};
use restate_types::journal::raw::{PlainRawEntry, RawEntry, RawEntryCodec};
use restate_types::journal::{Completion, CompletionResult};
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

#[derive(Debug)]
pub struct PartitionProcessorSimulator<InvokerInput, Codec> {
    journals: InMemoryJournalStorage,
    in_tx: InvokerInput,
    out_rx: mpsc::Receiver<Effect>,

    steps: VecDeque<SimulatorStep>,

    _codec: PhantomData<Codec>,
}

enum SimulatorStep {
    Handle(Box<dyn FnOnce(Effect) -> SimulatorAction>),
    Do(Duration, Box<dyn FnOnce() -> SimulatorAction>),
}

pub enum SimulatorAction {
    SendCompletion(FullInvocationId, Completion),
    Noop,
}

impl Debug for SimulatorStep {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SimulatorStep::Handle(_) => f.write_str("Handle"),
            SimulatorStep::Do(_, _) => f.write_str("Do"),
        }
    }
}

impl<InvokerInput, Codec> PartitionProcessorSimulator<InvokerInput, Codec>
where
    InvokerInput: ServiceHandle,
{
    pub async fn new(journals: InMemoryJournalStorage, mut in_tx: InvokerInput) -> Self {
        let (out_tx, out_rx) = mpsc::channel(100);
        in_tx
            .register_partition((0, 0), RangeInclusive::new(0, 0), out_tx)
            .await
            .unwrap();

        Self {
            journals,
            in_tx,
            out_rx,
            steps: Default::default(),
            _codec: Default::default(),
        }
    }

    pub fn append_handler_step<Fn>(&mut self, f: Fn)
    where
        Fn: FnOnce(Effect) -> SimulatorAction + 'static,
    {
        self.steps.push_back(SimulatorStep::Handle(Box::new(f)));
    }

    pub fn append_timed_step<Fn>(&mut self, duration: Duration, f: Fn)
    where
        Fn: FnOnce() -> SimulatorAction + 'static,
    {
        self.steps
            .push_back(SimulatorStep::Do(duration, Box::new(f)));
    }

    pub fn append_step<Fn>(&mut self, f: Fn)
    where
        Fn: FnOnce() -> SimulatorAction + 'static,
    {
        self.steps
            .push_back(SimulatorStep::Do(Duration::ZERO, Box::new(f)));
    }
}

impl<InvokerInput, Codec> PartitionProcessorSimulator<InvokerInput, Codec>
where
    InvokerInput: ServiceHandle + Debug,
    Codec: RawEntryCodec,
{
    pub async fn invoke(
        &mut self,
        fid: FullInvocationId,
        method: impl Into<String>,
        request_payload: impl Message,
    ) {
        debug!("Writing new journal");
        self.journals.create_new_journal(fid.clone(), method).await;
        self.journals
            .append_entry(
                &fid,
                RawEntry::new(
                    EnrichedEntryHeader::PollInputStream { is_completed: true },
                    PollInputStreamEntryMessage {
                        value: request_payload.encode_to_vec().into(),
                    }
                    .encode_to_vec()
                    .into(),
                ),
            )
            .await;

        debug!("Sending invoke to invoker: {:?}", fid);
        self.in_tx
            .invoke((0, 0), fid, InvokeInputJournal::NoCachedJournal)
            .await
            .unwrap();
    }

    pub async fn run(mut self) {
        while let Some(handler) = self.steps.pop_front() {
            let handler_res = match handler {
                SimulatorStep::Handle(handler_fn) => {
                    let out = self.out_rx.recv().await.unwrap();
                    debug!("Got from invoker: {:?}", out);

                    if let Effect {
                        full_invocation_id,
                        kind:
                            EffectKind::JournalEntry {
                                entry_index, entry, ..
                            },
                    } = &out
                    {
                        self.journals
                            .append_entry(full_invocation_id, entry.clone())
                            .await;
                        debug!("Notifying stored ack to invoker: {:?}", entry_index);
                        self.in_tx
                            .notify_stored_entry_ack(
                                (0, 0),
                                full_invocation_id.clone(),
                                *entry_index,
                            )
                            .await
                            .unwrap();
                    };

                    handler_fn(out)
                }
                SimulatorStep::Do(timeout, handler_fn) => {
                    tokio::time::sleep(timeout).await;
                    handler_fn()
                }
            };

            match handler_res {
                SimulatorAction::SendCompletion(fid, completion) => {
                    self.journals
                        .complete_entry::<Codec>(
                            &fid,
                            completion.entry_index,
                            completion.result.clone(),
                        )
                        .await;

                    debug!("Sending completion to invoker: {:?}", &completion);
                    self.in_tx
                        .notify_completion((0, 0), fid, completion)
                        .await
                        .unwrap();
                }
                SimulatorAction::Noop => {}
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryJournalStorage {
    #[allow(clippy::type_complexity)]
    journals: Arc<Mutex<HashMap<FullInvocationId, (JournalMetadata, Vec<PlainRawEntry>)>>>,
}

impl InMemoryJournalStorage {
    pub async fn create_new_journal(&mut self, fid: FullInvocationId, method: impl Into<String>) {
        let mut journals = self.journals.lock().await;

        let span_context = ServiceInvocationSpanContext::start(&fid, SpanRelation::None);

        journals.insert(
            fid,
            (
                JournalMetadata {
                    endpoint_id: None,
                    method: method.into().into(),
                    span_context,
                    length: 0,
                },
                vec![],
            ),
        );
    }

    pub async fn append_entry(&mut self, fid: &FullInvocationId, entry: EnrichedRawEntry) {
        let mut journals = self.journals.lock().await;
        let (meta, journal) = journals
            .get_mut(fid)
            .expect("append_entry can be invoked only when the journal is already available");

        meta.length += 1;

        // TODO workaround because we cannot implement From<EnrichedRawEntry> for PlainRawEntry due
        //  to https://github.com/restatedev/restate/issues/420
        let entry = PlainRawEntry::new(entry.header.into(), entry.entry);

        journal.push(entry);
    }

    pub async fn complete_entry<Codec>(
        &mut self,
        fid: &FullInvocationId,
        index: EntryIndex,
        result: CompletionResult,
    ) where
        Codec: RawEntryCodec,
    {
        let mut journals = self.journals.lock().await;
        let (_, journal) = journals
            .get_mut(fid)
            .expect("append_entry can be invoked only when the journal is already available");

        let raw_entry = journal
            .get_mut(index as usize)
            .expect("There should be an entry");
        Codec::write_completion(raw_entry, result).unwrap();
    }
}

impl JournalReader for InMemoryJournalStorage {
    type JournalStream = stream::Iter<IntoIter<PlainRawEntry>>;
    type Error = Infallible;
    type Future<'a> =
        BoxFuture<'static, Result<(JournalMetadata, Self::JournalStream), Self::Error>>;

    fn read_journal(&self, fid: &FullInvocationId) -> Self::Future<'_> {
        let journals_arc = self.journals.clone();
        let fid = fid.clone();
        async move {
            let journals = journals_arc.lock().await;

            let (meta, journal) = journals.get(&fid).unwrap();

            Ok((meta.clone(), stream::iter(journal.clone())))
        }
        .boxed()
    }
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryStateStorage {
    #[allow(clippy::type_complexity)]
    tables: Arc<Mutex<HashMap<ServiceId, Vec<(Bytes, Bytes)>>>>,
}

impl StateReader for InMemoryStateStorage {
    type StateIter = IntoIter<(Bytes, Bytes)>;
    type Error = Infallible;
    type Future<'a> = BoxFuture<'static, Result<EagerState<Self::StateIter>, Self::Error>>;

    fn read_state<'a>(&'a self, service_id: &'a ServiceId) -> Self::Future<'_> {
        let table_arc = self.tables.clone();
        let service_id = service_id.clone();
        async move {
            let tables = table_arc.lock().await;
            Ok(EagerState::new_complete(
                tables.get(&service_id).unwrap().clone().into_iter(),
            ))
        }
        .boxed()
    }
}
