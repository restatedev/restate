// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BytesMut};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use prost::encoding::{decode_varint, encode_varint, encoded_len_varint};

use restate_partition_store::keys::{MemCmpStr, MemCmpString};
use restate_util_string::ReString;

/// Trailing key fields following the encoded string, so decode benchmarks also cover the
/// multi-field key case where the string is not the entire remaining buffer.
const DECODE_SUFFIX: [u8; 32] = [7; 32];

struct Sample {
    name: &'static str,
    value: String,
    mem_cmp_encoded: Vec<u8>,
    mem_cmp_suffixed: Vec<u8>,
    string_varint_encoded: Vec<u8>,
}

fn encode_mem_cmp_string(value: &MemCmpString) -> Vec<u8> {
    let mut buf = Vec::with_capacity(value.encoded_len());
    value.encode_to(&mut buf);
    buf
}

fn string_varint_encoded_len(value: &str) -> usize {
    encoded_len_varint(u64::try_from(value.len()).expect("usize fitting into u64")) + value.len()
}

fn encode_string_varint(value: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(string_varint_encoded_len(value));
    encode_varint(value.len() as u64, &mut buf);
    buf.extend_from_slice(value.as_bytes());
    buf
}

fn decode_string_varint<B: Buf>(source: &mut B) -> String {
    let len = usize::try_from(decode_varint(source).unwrap()).unwrap();
    let mut string_data = source.take(len);

    let result = if len <= string_data.chunk().len() {
        // SAFETY: benchmark data is encoded from valid UTF-8 strings.
        unsafe { str::from_utf8_unchecked(&string_data.chunk()[..len]) }.to_owned()
    } else {
        let string_data = string_data.copy_to_bytes(len);
        // SAFETY: benchmark data is encoded from valid UTF-8 strings.
        unsafe { str::from_utf8_unchecked(&string_data) }.to_owned()
    };

    string_data.advance(len);
    result
}

fn samples() -> Vec<Sample> {
    [
        ("3c_ascii", 3),
        ("8c_ascii", 8),
        ("34c_ascii", 34),
        ("128c_ascii", 128),
        ("255c_ascii", 255),
    ]
    .into_iter()
    .map(|(name, len)| {
        let value = "a".repeat(len);
        assert_eq!(value.len(), len);
        assert_eq!(value.chars().count(), len);

        let mem_cmp_encoded = encode_mem_cmp_string(&MemCmpString::<String>::from(value.as_str()));
        let mut mem_cmp_suffixed = mem_cmp_encoded.clone();
        mem_cmp_suffixed.extend_from_slice(&DECODE_SUFFIX);
        let string_varint_encoded = encode_string_varint(value.as_str());
        Sample {
            name,
            value,
            mem_cmp_encoded,
            mem_cmp_suffixed,
            string_varint_encoded,
        }
    })
    .collect()
}

fn encode_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("mem_cmp_string/encode");

    for sample in samples() {
        group.throughput(Throughput::Bytes(sample.value.len() as u64));

        let owned: MemCmpString = MemCmpString::from(sample.value.as_str());
        group.bench_with_input(
            BenchmarkId::new("MemCmpString", sample.name),
            &owned,
            |b, value| {
                b.iter_batched(
                    || BytesMut::with_capacity(value.encoded_len()),
                    |mut buf| {
                        value.encode_to(&mut buf);
                        buf
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        let borrowed = MemCmpStr::from(sample.value.as_str());
        group.bench_with_input(
            BenchmarkId::new("MemCmpStr", sample.name),
            &borrowed,
            |b, value| {
                b.iter_batched(
                    || BytesMut::with_capacity(value.encoded_len()),
                    |mut buf| {
                        value.encode_to(&mut buf);
                        buf
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("StringVarint", sample.name),
            sample.value.as_str(),
            |b, value| {
                b.iter_batched(
                    || BytesMut::with_capacity(string_varint_encoded_len(value)),
                    |mut buf| {
                        encode_varint(value.len() as u64, &mut buf);
                        buf.extend_from_slice(value.as_bytes());
                        buf
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn decode_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("mem_cmp_string/decode");

    for sample in samples() {
        group.throughput(Throughput::Bytes(sample.value.len() as u64));

        group.bench_with_input(
            BenchmarkId::new("MemCmpString", sample.name),
            sample.mem_cmp_encoded.as_slice(),
            |b, encoded| {
                b.iter_batched(
                    || encoded,
                    |mut input| {
                        (
                            MemCmpString::<String>::decode_from(&mut input).unwrap(),
                            input,
                        )
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // the string is followed by more key fields; decode must find its end and
        // size its allocation without relying on `remaining()`.
        group.bench_with_input(
            BenchmarkId::new("MemCmpString_suffixed", sample.name),
            sample.mem_cmp_suffixed.as_slice(),
            |b, encoded| {
                b.iter_batched(
                    || encoded,
                    |mut input| {
                        (
                            MemCmpString::<String>::decode_from(&mut input).unwrap(),
                            input,
                        )
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // borrowed decode into an inlining string type; allocation-free for short strings
        group.bench_with_input(
            BenchmarkId::new("MemCmpReString", sample.name),
            sample.mem_cmp_encoded.as_slice(),
            |b, encoded| {
                b.iter_batched(
                    || encoded,
                    |mut input| {
                        (
                            MemCmpString::<ReString>::decode_from(&mut input).unwrap(),
                            input,
                        )
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("StringVarint", sample.name),
            sample.string_varint_encoded.as_slice(),
            |b, encoded| {
                b.iter_batched(
                    || encoded,
                    |mut input| (decode_string_varint(&mut input), input),
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn mem_cmp_string_benchmark(c: &mut Criterion) {
    encode_benchmark(c);
    decode_benchmark(c);
}

criterion_group!(benches, mem_cmp_string_benchmark);
criterion_main!(benches);
