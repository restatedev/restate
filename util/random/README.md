# restate-util-random

Lightweight, dependency-free pseudo-random utilities for use inside Restate
crates that need cheap, non-cryptographic randomness (e.g. sampling, shuffling,
jitter).

The PRNG is a thread-local [xorshift\*] generator, seeded from a process-wide
counter hashed with `DefaultHasher`. It is **not** cryptographically secure and
must not be used where unpredictability matters.

[xorshift\*]: https://en.wikipedia.org/wiki/Xorshift#xorshift*

## API

- `pseudo_random() -> u64` — next 64-bit pseudo-random value from the
  current thread's generator.
- `psuedo_shuffle<T>(slice: &mut [T])` — in-place Fisher–Yates shuffle using
  `pseudo_random()`.

## When to use

- Probabilistic sampling on hot paths where a real RNG would be overkill
  (e.g. `restate-rocksdb` perf-guard sampling).
- Shuffling small slices when ordering bias is acceptable.

## When NOT to use

- Anything security-sensitive (tokens, nonces, keys). Use a CSPRNG instead.
- Reproducible randomness across threads — each thread has its own seed.
