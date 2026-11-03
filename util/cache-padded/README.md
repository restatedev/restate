# restate-util-cache-padded

`CachePadded<T>`: a value aligned and padded to the target's cache-line pair, so that data
written by different threads does not share a cache line, or a line pair that the hardware
prefetches together. Dependency-free.

Use it for the few fields that different threads write on a hot path, such as a counter one
thread bumps while another reads a neighbouring field. Padding everything wastes cache: a padded
`AtomicU64` takes 128 bytes on x86-64.

The alignment per architecture follows crossbeam-utils' `CachePadded`.
