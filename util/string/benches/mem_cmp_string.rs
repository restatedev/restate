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

use restate_util_string::{EncodedMemCmpStr, MemCmpStr, MemCmpString, ReString, decode_str_into};

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

fn decode_string_varint<B: Buf, const CHECK_UTF8: bool>(source: &mut B) -> String {
    let len = usize::try_from(decode_varint(source).unwrap()).unwrap();
    if len <= source.chunk().len() {
        let decoded = if CHECK_UTF8 {
            str::from_utf8(&source.chunk()[..len]).unwrap()
        } else {
            // SAFETY: benchmark inputs are encoded from valid UTF-8 strings.
            unsafe { str::from_utf8_unchecked(&source.chunk()[..len]) }
        }
        .to_owned();
        source.advance(len);
        decoded
    } else {
        let string_data = source.copy_to_bytes(len);
        if CHECK_UTF8 {
            str::from_utf8(&string_data).unwrap()
        } else {
            // SAFETY: benchmark inputs are encoded from valid UTF-8 strings.
            unsafe { str::from_utf8_unchecked(&string_data) }
        }
        .to_owned()
    }
}

fn decode_restring_varint<B: Buf, const CHECK_UTF8: bool>(source: &mut B) -> ReString {
    let len = usize::try_from(decode_varint(source).unwrap()).unwrap();
    let mut string_data = source.take(len);
    if CHECK_UTF8 {
        ReString::from_utf8_buf(&mut string_data).unwrap()
    } else {
        // SAFETY: benchmark inputs are encoded from valid UTF-8 strings.
        unsafe { ReString::from_utf8_buf_unchecked(&mut string_data) }
    }
}

fn sample(name: &'static str, value: String) -> Sample {
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
}

fn samples() -> Vec<Sample> {
    let mut samples = [
        ("3c_ascii", 3),
        ("8c_ascii", 8),
        ("34c_ascii", 34),
        ("128c_ascii", 128),
        ("255c_ascii", 255),
    ]
    .into_iter()
    .map(|(name, len)| {
        let value = "a".repeat(len);
        sample(name, value)
    })
    .collect::<Vec<_>>();
    samples.push(sample("11b_utf8_boundary", "abcdefg🦀".to_owned()));
    samples.push(sample("128b_utf8", "🦀".repeat(32)));
    samples
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

        group.bench_with_input(
            BenchmarkId::new("MemCmpString_unchecked", sample.name),
            sample.mem_cmp_encoded.as_slice(),
            |b, encoded| {
                b.iter_batched(
                    || encoded,
                    |mut input| {
                        // SAFETY: benchmark inputs are encoded from valid UTF-8 strings.
                        let decoded = unsafe {
                            MemCmpString::<String>::decode_from_unchecked(&mut input).unwrap()
                        };
                        (decoded, input)
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("decode_str_into", sample.name),
            sample.mem_cmp_encoded.as_slice(),
            |b, encoded| {
                b.iter_batched_ref(
                    || vec![0; sample.value.len()],
                    |output| decode_str_into(encoded, output).unwrap(),
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
            BenchmarkId::new("MemCmpReString_unchecked", sample.name),
            sample.mem_cmp_encoded.as_slice(),
            |b, encoded| {
                b.iter_batched(
                    || encoded,
                    |mut input| {
                        // SAFETY: benchmark inputs are encoded from valid UTF-8 strings.
                        let decoded = unsafe {
                            MemCmpString::<ReString>::decode_from_unchecked(&mut input).unwrap()
                        };
                        (decoded, input)
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("EncodedMemCmpStr", sample.name),
            sample.mem_cmp_encoded.as_slice(),
            |b, encoded| {
                b.iter(|| EncodedMemCmpStr::try_ref_from_prefix(encoded).unwrap());
            },
        );

        group.bench_with_input(
            BenchmarkId::new("EncodedMemCmpStr_unchecked", sample.name),
            sample.mem_cmp_encoded.as_slice(),
            |b, encoded| {
                b.iter(|| {
                    // SAFETY: benchmark inputs are encoded from valid UTF-8 strings.
                    unsafe { EncodedMemCmpStr::try_ref_from_prefix_unchecked(encoded).unwrap() }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("StringVarintString", sample.name),
            sample.string_varint_encoded.as_slice(),
            |b, encoded| {
                b.iter_batched(
                    || encoded,
                    |mut input| (decode_string_varint::<_, true>(&mut input), input),
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("StringVarintString_unchecked", sample.name),
            sample.string_varint_encoded.as_slice(),
            |b, encoded| {
                b.iter_batched(
                    || encoded,
                    |mut input| (decode_string_varint::<_, false>(&mut input), input),
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("StringVarintReString", sample.name),
            sample.string_varint_encoded.as_slice(),
            |b, encoded| {
                b.iter_batched(
                    || encoded,
                    |mut input| (decode_restring_varint::<_, true>(&mut input), input),
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("StringVarintReString_unchecked", sample.name),
            sample.string_varint_encoded.as_slice(),
            |b, encoded| {
                b.iter_batched(
                    || encoded,
                    |mut input| (decode_restring_varint::<_, false>(&mut input), input),
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
