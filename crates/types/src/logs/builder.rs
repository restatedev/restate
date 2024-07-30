// Copyright (c) 2024-2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use super::metadata::{
    Chain, LogletConfig, LogletParams, Logs, MaybeSegment, ProviderKind, SegmentIndex,
};
use super::{LogId, Lsn};

#[derive(Debug, Default, Clone)]
pub struct LogsBuilder {
    inner: Logs,
}

#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error("log {0} already exists")]
    LogAlreadyExists(LogId),
    #[error("Segment conflicts with existing (base_lsn={0})")]
    SegmentConflict(Lsn),
}

impl LogsBuilder {
    /// Fails if the log already exists.
    pub fn add_log(
        &mut self,
        log_id: LogId,
        chain: Chain,
    ) -> Result<ChainBuilder<'_>, BuilderError> {
        if self.inner.logs.contains_key(&log_id) {
            return Err(BuilderError::LogAlreadyExists(log_id));
        }
        self.inner.logs.insert(log_id, chain);
        Ok(self.chain(&log_id).unwrap())
    }

    pub fn chain(&mut self, log_id: &LogId) -> Option<ChainBuilder<'_>> {
        let chain = self.inner.logs.get_mut(log_id)?;
        Some(ChainBuilder { inner: chain })
    }

    /// Bumps the version and returns the constructed log metadata.
    pub fn build(self) -> Logs {
        Logs {
            version: self.inner.version.next(),
            logs: self.inner.logs,
        }
    }
}

impl AsRef<Logs> for LogsBuilder {
    fn as_ref(&self) -> &Logs {
        &self.inner
    }
}

impl From<Logs> for LogsBuilder {
    fn from(value: Logs) -> LogsBuilder {
        LogsBuilder { inner: value }
    }
}

#[derive(Debug)]
pub struct ChainBuilder<'a> {
    inner: &'a mut Chain,
}

impl<'a> ChainBuilder<'a> {
    /// Removes and returns whole segments before `until_base_lsn`.
    /// if `until_base_lsn` falls inside a segment, the segment is kept but all previous
    /// segments will be dropped.
    ///
    /// By design, The API protects against removing the tail segment.
    /// `until_base_lsn` is exclusive.
    pub fn trim_prefix(&mut self, until_base_lsn: Lsn) {
        let found_base_lsn = match self.find_segment_for_lsn(until_base_lsn) {
            MaybeSegment::Some(segment) => segment.base_lsn,
            MaybeSegment::Trim { .. } => return,
        };

        self.inner.chain = self.inner.chain.split_off(&found_base_lsn);
    }

    /// `base_lsn` must be higher than all previous base_lsns.
    /// If `base_lsn` is identical to the tail segment, the new segment will **replace**
    /// the last segment but will still acquire a higher segment index.
    /// This behaviour is designed to support empty loglets.
    pub fn append_segment(
        &mut self,
        base_lsn: Lsn,
        provider: ProviderKind,
        params: LogletParams,
    ) -> Result<(), BuilderError> {
        let mut last_entry = self
            .inner
            .chain
            .last_entry()
            .expect("chain have at least one segment");
        if *last_entry.key() < base_lsn {
            // append
            let new_index = SegmentIndex(last_entry.get().index().0 + 1);
            self.inner
                .chain
                .insert(base_lsn, LogletConfig::new(new_index, provider, params));
            Ok(())
        } else if *last_entry.key() == base_lsn {
            // Replace the last segment (empty segment)
            let new_index = SegmentIndex(last_entry.get().index().0 + 1);
            last_entry.insert(LogletConfig::new(new_index, provider, params));
            Ok(())
        } else {
            // can't add to the back.
            Err(BuilderError::SegmentConflict(*last_entry.key()))
        }
    }
}

impl Deref for ChainBuilder<'_> {
    type Target = Chain;
    fn deref(&self) -> &Chain {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use crate::logs::metadata::{LogletParams, MaybeSegment, ProviderKind, Segment};
    use crate::logs::SequenceNumber;
    use crate::{Version, Versioned};
    use googletest::prelude::*;

    use super::*;

    #[test]
    fn test_default_builder() -> googletest::Result<()> {
        let builder = LogsBuilder::default();
        let logs = builder.build();
        assert_eq!(Version::MIN, logs.version());

        // a builder with 2 logs
        let mut builder = LogsBuilder::default();
        let chain = builder.add_log(
            LogId::from(1),
            Chain::new(ProviderKind::InMemory, LogletParams::from("test1")),
        )?;

        assert_eq!(chain.tail_index(), SegmentIndex(0));

        let segment = chain.find_segment_for_lsn(Lsn::INVALID);
        assert_that!(
            segment,
            pat!(MaybeSegment::Some(pat!(Segment {
                base_lsn: eq(Lsn::from(1)),
                tail_lsn: eq(None),
            })))
        );

        let segment = chain.find_segment_for_lsn(Lsn::OLDEST);
        assert_that!(
            segment,
            pat!(MaybeSegment::Some(pat!(Segment {
                base_lsn: eq(Lsn::from(1)),
                tail_lsn: eq(None),
            })))
        );

        let segment = chain.find_segment_for_lsn(Lsn::from(10000));
        assert_that!(
            segment,
            pat!(MaybeSegment::Some(pat!(Segment {
                base_lsn: eq(Lsn::from(1)),
                tail_lsn: eq(None),
            })))
        );

        // fails.
        assert_that!(
            builder.add_log(
                LogId::from(1),
                Chain::new(ProviderKind::InMemory, LogletParams::from("test1"),)
            ),
            err(pat!(BuilderError::LogAlreadyExists(eq(LogId::from(1)))))
        );

        builder
            .add_log(
                LogId::from(2),
                Chain::new(ProviderKind::InMemory, LogletParams::from("test2")),
            )
            .unwrap();
        let logs = builder.build();
        assert_eq!(Version::MIN, logs.version());
        assert_eq!(2, logs.num_logs());

        assert_eq!(
            Lsn::OLDEST,
            logs.chain(&LogId::from(1)).unwrap().tail().base_lsn
        );

        Ok(())
    }

    #[test]
    fn test_add_segments() -> googletest::Result<()> {
        let log_id = LogId::from(1);
        let mut builder = LogsBuilder::default();
        let mut chain = builder.add_log(
            log_id,
            Chain::new(ProviderKind::InMemory, LogletParams::from("test1")),
        )?;
        assert_eq!(Lsn::OLDEST, chain.head().base_lsn);

        chain.append_segment(
            Lsn::from(10),
            ProviderKind::Local,
            LogletParams::from("test2"),
        )?;

        assert_eq!(Lsn::OLDEST, chain.head().base_lsn);
        assert_eq!(Lsn::from(10), chain.tail().base_lsn);

        assert_eq!(SegmentIndex(1), chain.tail_index());
        assert_eq!(SegmentIndex(1), chain.tail().index());

        // can't, this is a conflict.
        assert_that!(
            chain.append_segment(
                Lsn::from(8),
                ProviderKind::InMemory,
                LogletParams::from("test3")
            ),
            err(pat!(BuilderError::SegmentConflict(eq(Lsn::from(10)))))
        );

        // can't as well.
        assert_that!(
            chain.append_segment(
                Lsn::from(1),
                ProviderKind::InMemory,
                LogletParams::from("test3")
            ),
            err(pat!(BuilderError::SegmentConflict(eq(Lsn::from(10)))))
        );

        // can't as well.
        assert_that!(
            chain.append_segment(
                Lsn::INVALID,
                ProviderKind::InMemory,
                LogletParams::from("test3")
            ),
            err(pat!(BuilderError::SegmentConflict(eq(Lsn::from(10)))))
        );

        assert_eq!(2, chain.num_segments());
        // replace the tail, same base_lsn
        chain.append_segment(
            Lsn::from(10),
            ProviderKind::InMemory,
            LogletParams::from("test55"),
        )?;
        assert_eq!(2, chain.num_segments());

        assert_eq!(Lsn::OLDEST, chain.head().base_lsn);
        assert_eq!(Lsn::from(10), chain.tail().base_lsn);
        assert_that!(chain.tail().config.kind, eq(ProviderKind::InMemory));

        assert_eq!(SegmentIndex(2), chain.tail_index());
        assert_eq!(SegmentIndex(2), chain.tail().index());

        // Add another segment
        chain.append_segment(
            Lsn::from(20),
            ProviderKind::InMemory,
            LogletParams::from("test5"),
        )?;
        assert_eq!(3, chain.num_segments());
        assert_eq!(SegmentIndex(3), chain.tail_index());
        assert_eq!(SegmentIndex(3), chain.tail().index());
        let base_lsns: Vec<_> = chain.iter().map(|s| s.base_lsn).collect();
        assert_that!(
            base_lsns,
            elements_are![eq(Lsn::OLDEST), eq(Lsn::from(10)), eq(Lsn::from(20))]
        );

        // can't in the middle
        assert_that!(
            chain.append_segment(
                Lsn::from(8),
                ProviderKind::InMemory,
                LogletParams::from("test3")
            ),
            err(pat!(BuilderError::SegmentConflict(eq(Lsn::from(20)))))
        );

        Ok(())
    }

    #[test]
    fn test_find_segments() -> googletest::Result<()> {
        let log_id = LogId::from(1);
        let mut builder = LogsBuilder::default();
        let mut chain = builder.add_log(
            log_id,
            Chain::with_base_lsn(
                Lsn::from(500),
                ProviderKind::InMemory,
                LogletParams::from("test1".to_owned()),
            ),
        )?;

        assert_eq!(1, chain.num_segments());
        assert_eq!(Lsn::from(500), chain.head().base_lsn);
        // let's add 5 segments, 10 lsns apart after 500
        //  510, 520, 530, 540, 550
        for i in 1..=5 {
            chain.append_segment(
                Lsn::from(500 + i * 10),
                ProviderKind::Local,
                LogletParams::from(format!("test{}", 500 + (i * 10))),
            )?;
        }

        assert_eq!(6, chain.num_segments());

        assert_eq!(Lsn::from(500), chain.head().base_lsn);
        assert_that!(
            chain.head(),
            pat!(Segment {
                base_lsn: eq(Lsn::from(500)),
                tail_lsn: eq(Some(Lsn::from(510))),
            })
        );

        assert_that!(
            chain.tail(),
            pat!(Segment {
                base_lsn: eq(Lsn::from(550)),
                tail_lsn: eq(None),
            })
        );

        // segments are [500 -> 510 -> 520 -> 530 -> 540 -> 550 -> ..]
        // Find segments
        let segment = chain.find_segment_for_lsn(Lsn::from(10));
        assert_that!(
            segment,
            pat!(MaybeSegment::Trim {
                next_base_lsn: eq(Lsn::from(500))
            })
        );

        // edge case 1 (Lsn::Invalid)
        let segment = chain.find_segment_for_lsn(Lsn::INVALID);
        assert_that!(
            segment,
            pat!(MaybeSegment::Trim {
                next_base_lsn: eq(Lsn::from(500))
            })
        );

        let base_lsns: Vec<_> = chain.iter().map(|s| s.base_lsn).collect();
        let segment_starts = [
            Lsn::from(500),
            Lsn::from(510),
            Lsn::from(520),
            Lsn::from(530),
            Lsn::from(540),
            Lsn::from(550),
        ];

        assert_eq!(base_lsns, segment_starts);

        for i in 500..=551 {
            let lsn = Lsn::from(i);
            let segment = chain.find_segment_for_lsn(lsn);
            // find highest element in expected_bases that's smaller or equal to i;
            let expected_base = segment_starts
                .iter()
                .rev()
                .find(|&&x| x <= lsn)
                .copied()
                .unwrap();

            // the base_lsn of the segment after, or None if tail.
            let expected_tail = segment_starts.iter().find(|&&x| x > lsn).copied();

            assert_that!(
                segment,
                pat!(MaybeSegment::Some(pat!(Segment {
                    base_lsn: eq(expected_base),
                    tail_lsn: eq(expected_tail),
                })))
            );
        }

        Ok(())
    }

    #[test]
    fn test_trim_log_single_segment() -> googletest::Result<()> {
        let log_id = LogId::from(1);
        let mut builder = LogsBuilder::default();
        builder.add_log(
            log_id,
            Chain::new(
                ProviderKind::InMemory,
                LogletParams::from("test1".to_owned()),
            ),
        )?;
        let mut chain = builder.chain(&log_id).unwrap();
        // removing the only segment is not allowed (no-op)
        chain.trim_prefix(Lsn::new(10));
        let segment = chain.tail();
        assert_eq!(Lsn::OLDEST, segment.base_lsn);

        chain.trim_prefix(Lsn::OLDEST);
        let segment = chain.tail();
        assert_eq!(Lsn::OLDEST, segment.base_lsn);

        // Lsn::INVALID Shouldn't trick us into removing the writeable segment
        chain.trim_prefix(Lsn::INVALID);
        let segment = chain.tail();
        assert_eq!(Lsn::OLDEST, segment.base_lsn);

        Ok(())
    }

    #[test]
    fn test_trim_log_multi_segment() -> googletest::Result<()> {
        let log_id = LogId::from(1);
        let mut builder = LogsBuilder::default();
        let mut chain = builder.add_log(
            log_id,
            Chain::with_base_lsn(
                Lsn::from(500),
                ProviderKind::InMemory,
                LogletParams::from("test1".to_owned()),
            ),
        )?;

        assert_eq!(1, chain.num_segments());
        assert_eq!(Lsn::from(500), chain.head().base_lsn);
        // let's add 5 segments, 10 lsns apart after 500
        //  510, 520, 530, 540, 550
        for i in 1..=5 {
            chain.append_segment(
                Lsn::from(500 + i * 10),
                ProviderKind::Local,
                LogletParams::from(format!("test{}", 500 + (i * 10))),
            )?;
        }

        assert_eq!(6, chain.num_segments());
        assert_eq!(Lsn::from(500), chain.head().base_lsn);
        assert_eq!(Some(Lsn::from(510)), chain.head().tail_lsn);

        assert_eq!(Lsn::from(550), chain.tail().base_lsn);
        assert_eq!(None, chain.tail().tail_lsn);

        // no segments behind 10 point, nothing changed
        chain.trim_prefix(Lsn::new(10));
        assert_eq!(6, chain.num_segments());
        assert_eq!(Lsn::from(500), chain.head().base_lsn);
        assert_eq!(Lsn::from(550), chain.tail().base_lsn);

        // no segments behind 500 point, nothing changed (validates exclusive trim)
        chain.trim_prefix(Lsn::new(500));
        assert_eq!(6, chain.num_segments());
        assert_eq!(Lsn::from(500), chain.head().base_lsn);
        assert_eq!(Lsn::from(550), chain.tail().base_lsn);

        // no complete segments behind 505 point, nothing changed (validates we only trim full
        // segments)
        chain.trim_prefix(Lsn::new(505));
        assert_eq!(6, chain.num_segments());
        assert_eq!(Lsn::from(500), chain.head().base_lsn);
        assert_eq!(Lsn::from(550), chain.tail().base_lsn);

        // 1 segment behind 510 point
        chain.trim_prefix(Lsn::new(510));
        assert_eq!(5, chain.num_segments());
        assert_eq!(Lsn::from(510), chain.head().base_lsn);
        assert_eq!(Lsn::from(550), chain.tail().base_lsn);

        // 2 segment behind 530 point
        chain.trim_prefix(Lsn::new(530));
        assert_eq!(3, chain.num_segments());
        assert_eq!(Lsn::from(530), chain.head().base_lsn);
        assert_eq!(Lsn::from(550), chain.tail().base_lsn);

        // 1 deleteable segment behind 600 point
        chain.trim_prefix(Lsn::new(600));
        assert_eq!(1, chain.num_segments());
        assert_eq!(Lsn::from(550), chain.tail().base_lsn);

        // no-op can't delete the tail.
        chain.trim_prefix(Lsn::MAX);
        assert_eq!(1, chain.num_segments());
        assert_eq!(Lsn::from(550), chain.tail().base_lsn);

        assert_eq!(SegmentIndex(5), chain.tail_index());

        Ok(())
    }
}
