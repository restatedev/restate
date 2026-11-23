// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::UniqueTimestamp;
use restate_limiter::LimitKey;
use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::WriteVQueueTable;
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_types::identifiers::PartitionId;
use restate_types::identifiers::partitioner::HashPartitioner;
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;
use restate_types::{LockName, Scope};

use crate::mocks::{differential_sessions, rows};

#[restate_core::test]
async fn vqueue_metadata_matches_v1() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let scope = Scope::try_non_interned("tenant").unwrap();
    let at = |offset: u64| {
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000 + offset))
            .unwrap()
    };
    let metas = [
        (
            VQueueId::custom(HashPartitioner::compute_partition_key("alice"), "lock"),
            VQueueMeta::new(
                at(0),
                None,
                LimitKey::None,
                VQueueLink::Lock(LockName::parse("Cart/alice").unwrap()),
            ),
        ),
        (
            VQueueId::custom(HashPartitioner::compute_partition_key("tenant"), "service"),
            VQueueMeta::new(
                at(1),
                Some(scope),
                LimitKey::None,
                VQueueLink::Service("Checkout".into()),
            ),
        ),
        (
            VQueueId::custom(7, "unlinked"),
            VQueueMeta::new(at(2), None, LimitKey::None, VQueueLink::None),
        ),
    ];
    let mut tx = store.transaction();
    for (id, meta) in &metas {
        tx.create_vqueue(id, meta);
    }
    tx.commit().await.unwrap();
    drop(tx);

    let [old, new] = differential_sessions(&manager).await;
    let id = &metas[0].0;
    for sql in [
        "SELECT * FROM sys_vqueue_meta ORDER BY id".to_owned(),
        format!("SELECT lock_name, is_active FROM sys_vqueue_meta WHERE id = '{id}'"),
        "SELECT id, service_name FROM sys_vqueue_meta WHERE scope = 'tenant'".into(),
        "SELECT id FROM sys_vqueue_meta WHERE scope IS NULL ORDER BY id LIMIT 1 OFFSET 1".into(),
        "SELECT COUNT(*) FROM sys_vqueue_meta".into(),
    ] {
        assert_eq!(
            rows(new.as_ref(), &sql).await,
            rows(old.as_ref(), &sql).await,
            "{sql}"
        );
    }
    assert_eq!(
        rows(new.as_ref(), "SELECT COUNT(*) FROM sys_vqueue_meta").await,
        [["3"]]
    );

    // A finite set of IDs is looked up by key. The predicate still applies to
    // the fetched rows, before LIMIT, and a JOIN keeps its multiplicity.
    let [lock, service, unlinked] = metas.map(|(id, _)| id);
    let missing = VQueueId::custom(9, "missing");
    let looked_up = [
        format!(
            "SELECT id, service_name FROM sys_vqueue_meta \
             WHERE id IN ('{lock}', '{service}', '{missing}', '{service}') ORDER BY id"
        ),
        format!(
            "SELECT id FROM sys_vqueue_meta \
             WHERE id IN ('{lock}', '{service}', '{unlinked}') AND service_name = 'Checkout' LIMIT 1"
        ),
        format!(
            "SELECT id FROM sys_vqueue_meta WHERE id = '{lock}' OR id = '{unlinked}' ORDER BY id"
        ),
        format!(
            "SELECT m.id FROM sys_vqueue_meta m \
             JOIN (VALUES ('{lock}'), ('{lock}'), ('{service}')) AS v(x) ON m.id = v.x \
             WHERE m.id IN ('{lock}', '{service}') ORDER BY m.id"
        ),
    ];
    // Scanned, or with LIMIT 0 not read at all.
    let scanned = [
        format!("SELECT id FROM sys_vqueue_meta WHERE id NOT IN ('{lock}') ORDER BY id"),
        "SELECT id FROM sys_vqueue_meta WHERE scope = 'tenant'".to_owned(),
        format!("SELECT id FROM sys_vqueue_meta WHERE id IN ('{lock}') LIMIT 0"),
    ];
    for (sql, lookup) in looked_up
        .iter()
        .map(|sql| (sql, true))
        .chain(scanned.iter().map(|sql| (sql, false)))
    {
        assert_eq!(
            rows(new.as_ref(), sql).await,
            rows(old.as_ref(), sql).await,
            "{sql}"
        );
        let plan = rows(new.as_ref(), &format!("EXPLAIN {sql}"))
            .await
            .concat()
            .join("\n");
        assert_eq!(plan.contains("MultiGetExec"), lookup, "{sql}\n{plan}");
    }
    let mut counts = Vec::new();
    for sql in [&looked_up[0], &looked_up[1], &looked_up[3]] {
        counts.push(rows(new.as_ref(), sql).await.len());
    }
    assert_eq!(
        counts,
        [2, 1, 3],
        "duplicates_and_missing_ids_residual_before_limit_join_multiplicity"
    );
}
