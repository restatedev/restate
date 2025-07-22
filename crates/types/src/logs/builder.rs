// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;
use std::ops::Deref;

use super::metadata::{
    Chain, LogletConfig, LogletParams, Logs, LogsConfiguration, LookupIndex, MaybeSegment,
    ProviderKind, SegmentIndex,
};
use super::{LogId, Lsn};
use crate::Version;
use crate::replicated_loglet::ReplicatedLogletParams;

#[derive(Debug, Default, Clone)]
pub struct LogsBuilder {
    inner: Logs,
    modified: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error("log {0} is permanently sealed")]
    ChainPermanentlySealed(LogId),
    #[error("log {0} already exists")]
    LogAlreadyExists(LogId),
    #[error("loglet params could not be deserialized: {0}")]
    ParamsSerde(#[from] serde_json::Error),
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
        for loglet_config in chain.chain.values() {
            // needed if other loglets than the replicated one are enabled
            #[allow(irrefutable_let_patterns)]
            if let ProviderKind::Replicated = loglet_config.kind {
                let params =
                    ReplicatedLogletParams::deserialize_from(loglet_config.params.as_bytes())?;
                self.inner.lookup_index.add_replicated_loglet(
                    log_id,
                    loglet_config.index(),
                    params,
                );
            }
        }
        self.inner.logs.insert(log_id, chain);
        // update replicated loglet index
        self.modified = true;
        Ok(self.chain(log_id).unwrap())
    }

    pub fn chain(&mut self, log_id: LogId) -> Option<ChainBuilder<'_>> {
        let chain = self.inner.logs.get_mut(&log_id)?;
        Some(ChainBuilder {
            log_id,
            inner: chain,
            lookup_index: &mut self.inner.lookup_index,
            modified: &mut self.modified,
        })
    }

    /// Bumps the version and returns the constructed log metadata.
    pub fn build(self) -> Logs {
        Logs {
            version: self.inner.version.next(),
            logs: self.inner.logs,
            lookup_index: self.inner.lookup_index,
            config: self.inner.config,
        }
    }

    pub fn set_version(&mut self, version: NonZeroU32) {
        // because we increment this value on build() so we assume that the input is the intended
        // outcome of the build() call.
        self.inner.version = Version::from(u32::from(version) - 1);
    }

    /// Sets default logs configuration.
    pub fn set_configuration(&mut self, config: LogsConfiguration) {
        if self.inner.config != config {
            self.inner.config = config;
            self.modified = true;
        }
    }

    pub fn configuration(&self) -> &LogsConfiguration {
        self.inner.configuration()
    }

    pub fn build_if_modified(self) -> Option<Logs> {
        if self.modified {
            Some(Logs {
                version: self.inner.version.next(),
                logs: self.inner.logs,
                lookup_index: self.inner.lookup_index,
                config: self.inner.config,
            })
        } else {
            None
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
        LogsBuilder {
            inner: value,
            modified: false,
        }
    }
}

#[derive(Debug)]
pub struct ChainBuilder<'a> {
    log_id: LogId,
    inner: &'a mut Chain,
    lookup_index: &'a mut LookupIndex,
    modified: &'a mut bool,
}

impl ChainBuilder<'_> {
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

        let remaining = self.inner.chain.split_off(&found_base_lsn);
        if !self.inner.chain.is_empty() {
            *self.modified = true;

            for loglet_config in self.inner.chain.values() {
                // needed if other loglets than the replicated one are enabled
                #[allow(irrefutable_let_patterns)]
                if let ProviderKind::Replicated = loglet_config.kind {
                    // if it was inserted correctly before, we shouldn't fail to deserialize it.
                    // validation happens at original insert time.
                    let params =
                        ReplicatedLogletParams::deserialize_from(loglet_config.params.as_bytes())
                            .expect("params should be deserializable");
                    self.lookup_index.rm_replicated_loglet_reference(
                        self.log_id,
                        loglet_config.index(),
                        params.loglet_id,
                    );
                }
            }
        }

        self.inner.chain = remaining;
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
    ) -> Result<SegmentIndex, BuilderError> {
        if self.inner.state.is_sealed() {
            return Err(BuilderError::ChainPermanentlySealed(self.log_id));
        }

        let mut last_entry = self
            .inner
            .chain
            .last_entry()
            .expect("chain have at least one segment");

        match *last_entry.key() {
            key if key < base_lsn => {
                // append
                let new_index = SegmentIndex(last_entry.get().index().0 + 1);
                // needed if other loglets than the replicated one are enabled
                #[allow(irrefutable_let_patterns)]
                if let ProviderKind::Replicated = provider {
                    let params = ReplicatedLogletParams::deserialize_from(params.as_bytes())?;
                    self.lookup_index
                        .add_replicated_loglet(self.log_id, new_index, params);
                }
                self.inner
                    .chain
                    .insert(base_lsn, LogletConfig::new(new_index, provider, params));
                *self.modified = true;
                Ok(new_index)
            }
            key if key == base_lsn => {
                // Replace the last segment (empty segment)
                {
                    // Let's remove the loglet from the index if it's a replicated loglet
                    let old = last_entry.get();
                    // needed if other loglets than the replicated one are enabled
                    #[allow(irrefutable_let_patterns)]
                    if let ProviderKind::Replicated = old.kind {
                        let params =
                            ReplicatedLogletParams::deserialize_from(old.params.as_bytes())?;
                        self.lookup_index.rm_replicated_loglet_reference(
                            self.log_id,
                            old.index(),
                            params.loglet_id,
                        );
                    }
                }
                let new_index = SegmentIndex(last_entry.get().index().0 + 1);
                // needed if other loglets than the replicated one are enabled
                #[allow(irrefutable_let_patterns)]
                if let ProviderKind::Replicated = provider {
                    let params = ReplicatedLogletParams::deserialize_from(params.as_bytes())?;
                    self.lookup_index
                        .add_replicated_loglet(self.log_id, new_index, params);
                }
                last_entry.insert(LogletConfig::new(new_index, provider, params));
                *self.modified = true;
                Ok(new_index)
            }
            _ => {
                // can't add to the back.
                Err(BuilderError::SegmentConflict(*last_entry.key()))
            }
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
    use std::num::NonZeroU8;

    use crate::logs::SequenceNumber;
    use crate::logs::metadata::{LogletParams, MaybeSegment, ProviderKind, Segment};
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
            LogId::new(1),
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
                LogId::new(1),
                Chain::new(ProviderKind::InMemory, LogletParams::from("test1"),)
            ),
            err(pat!(BuilderError::LogAlreadyExists(eq(LogId::new(1)))))
        );

        builder
            .add_log(
                LogId::new(2),
                Chain::new(ProviderKind::InMemory, LogletParams::from("test2")),
            )
            .unwrap();
        let logs = builder.build();
        assert_eq!(Version::MIN, logs.version());
        assert_eq!(2, logs.num_logs());

        assert_eq!(
            Lsn::OLDEST,
            logs.chain(&LogId::new(1)).unwrap().tail().base_lsn
        );

        Ok(())
    }

    #[test]
    fn test_add_segments() -> googletest::Result<()> {
        let log_id = LogId::new(1);
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
        let log_id = LogId::new(1);
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
        let log_id = LogId::new(1);
        let mut builder = LogsBuilder::default();
        builder.add_log(
            log_id,
            Chain::new(
                ProviderKind::InMemory,
                LogletParams::from("test1".to_owned()),
            ),
        )?;
        let mut chain = builder.chain(log_id).unwrap();
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
        let log_id = LogId::new(1);
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

        // 1 deletable segment behind 600 point
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

    #[test]
    fn test_lookup_index() -> googletest::Result<()> {
        use crate::GenerationalNodeId;
        use crate::logs::LogletId;
        use crate::replicated_loglet::ReplicatedLogletParams;
        use crate::replication::{NodeSet, ReplicationProperty};

        let mut builder = LogsBuilder::default();

        let loglet1 = ReplicatedLogletParams {
            loglet_id: LogletId::from(1),
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            nodeset: NodeSet::new(),
        };

        let loglet2 = ReplicatedLogletParams {
            loglet_id: LogletId::from(2),
            sequencer: GenerationalNodeId::new(1, 1),
            replication: ReplicationProperty::new(NonZeroU8::new(2).unwrap()),
            nodeset: NodeSet::new(),
        };

        let loglet1_params = LogletParams::from(loglet1.serialize()?);
        let loglet2_params = LogletParams::from(loglet2.serialize()?);

        // log-1 -> [replicated-loglet-1]
        let chain1 = builder.add_log(
            LogId::new(1),
            Chain::new(ProviderKind::Replicated, loglet1_params.clone()),
        )?;
        assert_eq!(Lsn::OLDEST, chain1.head().base_lsn);
        let _ = chain1;

        let found1 = builder
            .inner
            .lookup_index
            .get_replicated_loglet(&LogletId::from(1))
            .unwrap();
        assert_that!(found1.references.len(), eq(1));
        assert_that!(found1.params, eq(loglet1));

        assert_that!(
            builder
                .inner
                .lookup_index
                .get_replicated_loglet(&LogletId::from(2)),
            none()
        );

        // log-2 -> [replicated-loglet-1]
        builder.add_log(
            LogId::new(2),
            Chain::new(ProviderKind::Replicated, loglet1_params),
        )?;

        let found1 = builder
            .inner
            .lookup_index
            .get_replicated_loglet(&LogletId::from(1))
            .unwrap();

        // should be referenced twice
        assert_that!(found1.references.len(), eq(2));
        let _ = found1;

        // let's add loglet2 to chain1 and check that after removing it, it is removed from the
        // lookup index.
        //
        // log-1 -> [replicated-loglet-1, replicated-loglet-2]
        builder.chain(LogId::new(1)).unwrap().append_segment(
            Lsn::from(10),
            ProviderKind::Replicated,
            loglet2_params.clone(),
        )?;

        // add a loglet of another type at the end to allow the replicated loglet to be trimmed
        // later
        //
        // log-1 -> [replicated-loglet-1, replicated-loglet-2, in-memory-loglet]
        builder.chain(LogId::new(1)).unwrap().append_segment(
            Lsn::from(100),
            ProviderKind::InMemory,
            LogletParams::from("test".to_string()),
        )?;

        let found2 = builder
            .inner
            .lookup_index
            .get_replicated_loglet(&LogletId::from(2))
            .unwrap();
        assert_that!(found2.references.len(), eq(1));

        let found1 = builder
            .inner
            .lookup_index
            .get_replicated_loglet(&LogletId::from(1))
            .unwrap();

        // should be referenced twice
        assert_that!(found1.references.len(), eq(2));
        let _ = found1;
        // log-1 -> [in-memory-loglet]
        builder.chain(LogId::new(1)).unwrap().trim_prefix(Lsn::MAX);
        assert_that!(builder.chain(LogId::new(1)).unwrap().chain.len(), eq(1));

        // log-1 -> [in-memory-loglet]
        // log-2 -> [replicated-loglet-1]
        assert_that!(
            builder
                .inner
                .lookup_index
                .get_replicated_loglet(&LogletId::from(2)),
            none()
        );

        let found1 = builder
            .inner
            .lookup_index
            .get_replicated_loglet(&LogletId::from(1))
            .unwrap();
        // down to one reference
        assert_that!(found1.references.len(), eq(1));

        // Replacing the tail segment of chain2 should remove the remaining reference to loglet
        //
        // log-2 -> [replicated-loglet-2]
        builder.chain(LogId::new(2)).unwrap().append_segment(
            Lsn::OLDEST,
            ProviderKind::Replicated,
            loglet2_params.clone(),
        )?;

        let found2 = builder
            .inner
            .lookup_index
            .get_replicated_loglet(&LogletId::from(2))
            .unwrap();
        assert_that!(found2.references.len(), eq(1));

        assert_that!(
            builder
                .inner
                .lookup_index
                .get_replicated_loglet(&LogletId::from(1)),
            none()
        );

        Ok(())
    }
}
