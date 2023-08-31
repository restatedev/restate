// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Poll};

use bytes::{Buf, BufMut, Bytes};
use pin_project::pin_project;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::server::UnaryService;
use tonic::Status;

// --- UnaryService adapter that can execute call only once

pub(super) struct TonicUnaryServiceAdapter<H> {
    ingress_request_headers: Option<IngressRequestHeaders>,
    inner_fn: Option<H>,
}

impl<H> TonicUnaryServiceAdapter<H> {
    pub(super) fn new(ingress_request_headers: IngressRequestHeaders, inner_fn: H) -> Self {
        Self {
            ingress_request_headers: Some(ingress_request_headers),
            inner_fn: Some(inner_fn),
        }
    }
}

impl<H, F> UnaryService<Bytes> for TonicUnaryServiceAdapter<H>
where
    H: FnOnce(HandlerRequest) -> F + Send,
    F: Future<Output = HandlerResult> + Send,
{
    type Response = Bytes;
    type Future = TonicUnaryServiceAdapterFuture<F>;

    fn call(&mut self, request: tonic::Request<Bytes>) -> Self::Future {
        let inner_fn = self
            .inner_fn
            .take()
            .expect("This service shouldn't be called twice");
        let fut = inner_fn((
            self.ingress_request_headers.take().unwrap(),
            request.into_inner(),
        ));
        TonicUnaryServiceAdapterFuture(fut)
    }
}

#[pin_project]
pub(super) struct TonicUnaryServiceAdapterFuture<F>(#[pin] F);

impl<F> Future for TonicUnaryServiceAdapterFuture<F>
where
    F: Future<Output = HandlerResult>,
{
    type Output = Result<tonic::Response<Bytes>, Status>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        Poll::Ready(ready!(this.0.poll(cx)).map(tonic::Response::new))
    }
}

// --- Noop codec to skip encode/decode in tonic

pub(super) struct NoopCodec;

impl Codec for NoopCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = Self;
    type Decoder = Self;

    fn encoder(&mut self) -> Self::Encoder {
        NoopCodec
    }

    fn decoder(&mut self) -> Self::Decoder {
        NoopCodec
    }
}

impl Encoder for NoopCodec {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        dst.put(item);
        Ok(())
    }
}

impl Decoder for NoopCodec {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}
