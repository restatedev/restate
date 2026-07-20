// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::StorageError;
use restate_storage_api::deduplication_table::{
    DedupInformation, DedupSequenceNumber, EpochSequenceNumber, ProducerId, ReadDeduplicationTable,
    WriteDeduplicationTable,
};
use restate_types::identifiers::LeaderEpoch;
use restate_wal_protocol::v2;

/// Read access to the command deduplication state — the producer/epoch sequence numbers already
/// applied, used to reject replayed or out-of-order commands.
pub trait HasDedup {
    /// Returns a read-only view of the dedup cache.
    fn dedup(&self) -> impl DedupAccess;
}

/// Mutating access to the deduplication state. Records are staged into the supplied storage
/// transaction and only take effect when it commits.
pub trait HasDedupMut: HasDedup {
    /// Returns a mutable view of the dedup cache.
    fn dedup_mut(&mut self) -> impl DedupMut;
}

/// Read-only view of the deduplication state.
pub trait DedupAccess {
    fn my_dedup_epoch(&self) -> LeaderEpoch;

    fn is_duplicate<S>(
        &self,
        dedup: &v2::Dedup,
        storage: &mut S,
    ) -> impl Future<Output = Result<bool, StorageError>>
    where
        S: ReadDeduplicationTable;
}

/// Mutable view of the deduplication state: persist the dedup information of an applied command.
pub trait DedupMut {
    fn store_dedup_information<S>(
        &mut self,
        txn: &mut S,
        dedup: &v2::Dedup,
    ) -> Result<(), StorageError>
    where
        S: WriteDeduplicationTable;
}

pub struct Dedup {
    /// The processor will reject self proposals < than this epoch
    my_dedup_esn: EpochSequenceNumber,
    // todo: add cache for other producers
}

impl Dedup {
    #[cfg(test)]
    pub fn new_empty() -> Self {
        Self {
            my_dedup_esn: EpochSequenceNumber {
                leader_epoch: LeaderEpoch::INVALID,
                sequence_number: 0,
            },
        }
    }

    pub async fn create<S>(storage: &mut S) -> Result<Self, StorageError>
    where
        S: ReadDeduplicationTable,
    {
        // This is the last esn we observed through the deduplication of self proposals
        let my_dedup_esn = storage
            .get_dedup_sequence_number(ProducerId::self_producer())
            .await?
            .map(|dedup| {
                let DedupSequenceNumber::Esn(esn) = dedup else {
                    panic!("self producer must store epoch sequence numbers!");
                };
                esn
            })
            .unwrap_or_else(|| EpochSequenceNumber {
                leader_epoch: LeaderEpoch::INVALID,
                sequence_number: 0,
            });

        Ok(Self { my_dedup_esn })
    }
}

impl DedupAccess for Dedup {
    fn my_dedup_epoch(&self) -> LeaderEpoch {
        self.my_dedup_esn.leader_epoch
    }

    async fn is_duplicate<S>(
        &self,
        dedup: &v2::Dedup,
        storage: &mut S,
    ) -> Result<bool, StorageError>
    where
        S: ReadDeduplicationTable,
    {
        // todo(azmy): use dedup() directly without first converting to DedupInformation
        let dedup_information: Option<DedupInformation> = dedup.clone().into();
        let Some(dedup_information) = dedup_information else {
            return Ok(false);
        };

        if dedup_information.producer_id.is_self_producer() {
            let DedupSequenceNumber::Esn(esn) = dedup_information.sequence_number else {
                panic!("self producer must store epoch sequence numbers!");
            };
            return Ok(self.my_dedup_esn >= esn);
        }

        let Some(last_dsn) = storage
            .get_dedup_sequence_number(&dedup_information.producer_id)
            .await?
        else {
            return Ok(false);
        };

        // Check whether we have seen this message before
        match (last_dsn, &dedup_information.sequence_number) {
            (DedupSequenceNumber::Esn(last_esn), DedupSequenceNumber::Esn(esn)) => {
                Ok(last_esn >= *esn)
            }
            (DedupSequenceNumber::Sn(last_sn), DedupSequenceNumber::Sn(sn)) => Ok(last_sn >= *sn),
            (last_dsn, dsn) => panic!(
                "sequence number types do not match: last sequence number '{last_dsn:?}', received sequence number '{dsn:?}'"
            ),
        }
    }
}

impl DedupMut for Dedup {
    fn store_dedup_information<S>(
        &mut self,
        txn: &mut S,
        dedup: &v2::Dedup,
    ) -> Result<(), StorageError>
    where
        S: WriteDeduplicationTable,
    {
        // todo(azmy): use dedup() directly without first converting to DedupInformation
        let dedup_information: Option<DedupInformation> = dedup.clone().into();
        if let Some(dedup_information) = dedup_information {
            if dedup_information.producer_id.is_self_producer() {
                let DedupSequenceNumber::Esn(esn) = dedup_information.sequence_number else {
                    panic!("self producer must store epoch sequence numbers!");
                };
                self.my_dedup_esn = esn;
            }

            txn.put_dedup_seq_number(
                dedup_information.producer_id.clone(),
                &dedup_information.sequence_number,
            )?;
        }
        Ok(())
    }
}

// -- Boilerplate --
impl<P: HasDedup> HasDedup for &P {
    #[inline]
    fn dedup(&self) -> impl DedupAccess {
        (**self).dedup()
    }
}

impl<P: HasDedup> HasDedup for &mut P {
    #[inline]
    fn dedup(&self) -> impl DedupAccess {
        (**self).dedup()
    }
}

impl<P: HasDedupMut> HasDedupMut for &mut P {
    #[inline]
    fn dedup_mut(&mut self) -> impl DedupMut {
        (**self).dedup_mut()
    }
}

impl<T: DedupAccess> DedupAccess for &T {
    fn my_dedup_epoch(&self) -> LeaderEpoch {
        (**self).my_dedup_epoch()
    }

    async fn is_duplicate<S>(
        &self,
        dedup: &v2::Dedup,
        storage: &mut S,
    ) -> Result<bool, StorageError>
    where
        S: ReadDeduplicationTable,
    {
        (**self).is_duplicate(dedup, storage).await
    }
}

impl<T: DedupMut> DedupMut for &mut T {
    fn store_dedup_information<S>(
        &mut self,
        txn: &mut S,
        dedup: &v2::Dedup,
    ) -> Result<(), StorageError>
    where
        S: WriteDeduplicationTable,
    {
        (**self).store_dedup_information(txn, dedup)
    }
}
