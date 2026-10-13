# Block-cache filling for low-priority scans

Low-priority partition-store scans, including query scans, no longer populate the
RocksDB block cache by default. They can still use cached blocks. This helps keep
large scans from evicting frequently accessed data.
