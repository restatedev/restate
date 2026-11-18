// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Online migrations: one-time changes to a partition's data that run in the
//! background while the partition serves requests.
//!
//! You write one file under `migrations/online/` that implements
//! [`OnlineMigration`], and add one line to [`ONLINE_MIGRATIONS`]. The
//! framework does the rest: it registers the migration in each store's registry,
//! walks its source table in small slices between the partition processor's
//! commands, paces and retries the slices, records the work on a job in
//! `restate.cluster.migration_jobs`, and marks the migration `complete` in
//! `restate.cluster.registry` once the walk is through. The journal
//! completion-index cleanup, `jc_orphan_cleanup/1`, is the worked example.
//!
//! # 1. Is an online migration the right kind?
//!
//! See the table in [`crate::migrations`]. In short: an online migration suits
//! cleanups and conversions that are correct while commands keep writing.
//! Foreground code that needs the whole store converted before it runs needs a
//! startup migration instead.
//!
//! It only runs on stores that carry the `registry` storage feature, which a
//! store gets with its first index. A store without it gets no entry, no walk and
//! no job: an online migration never raises a store's version floor. Today every
//! index family is opt-in (`experimental-enable-track-entry-indexes`,
//! `experimental-enable-track-status-gauges`), so until one is on by default an
//! online migration reaches only the stores that enabled one. A change that must
//! reach every store needs a startup migration for now.
//!
//! # 2. Name and version
//!
//! [`OnlineMigration::NAME`] is `"name/N"`, such as `"jc_orphan_cleanup/1"`. A
//! store runs each name once, ever, and keeps its entry forever. So a name is
//! never renamed or reused: a migration whose rows, change or source differ in
//! any way is a new version, `"jc_orphan_cleanup/2"`, which runs once more on
//! every store. The name's halves also name the kind of its walks
//! (`jc_orphan_cleanup` version 1); a malformed name fails compilation.
//!
//! # 3. Why it is safe, and what you must make true
//!
//! The walk visits, in key order, every row of the source that existed when the
//! migration registered, up to that moment's last key, the walk's *end*. Each
//! slice runs in the processor's own transaction, between two commands, and
//! commits your changes together with the walk's progress: a crash loses both or
//! neither.
//!
//! ```text
//! source   A    B    C    D    E   │ F    G
//!          ────migrated───┘        │ └ written after the walk started:
//!                     cursor = C   end = E   never visited
//! ```
//!
//! Three things are yours to make true:
//!
//! - **Foreground writes need no migration.** Rows written behind the cursor
//!   (`B` rewritten after the walk passed it) or beyond the end (`F`, `G`) are
//!   never visited. From the release that adds the migration on, commands must
//!   write rows that are already in the migrated form, or never write what the
//!   migration removes.
//! - **Decide from what the transaction reads now.** The row you get was read
//!   in this slice, and lookups through `tx`
//!   ([`PartitionStoreTransaction::lookup_for_migration`]) see the committed
//!   store plus this slice's own writes. Never reuse anything read in an
//!   earlier slice: commands ran in between. A fresh value of your type runs
//!   each slice, so state in `self` lives for one slice at most. A row whose
//!   work the engine rolls back, to keep the slice within its budget, ends the
//!   slice, so state cached in `self` never outlives the rows it was read for:
//!   that makes a cache in `self` across a slice's rows safe.
//! - **Migrating twice changes nothing.** A walk restarts from the beginning
//!   when it was lost, for example after a binary that doesn't know the migration
//!   ran the store; a failed slice is retried. Each row may be visited again:
//!   for a row already in the migrated form, change nothing and report
//!   `changed: false`.
//!
//! # 4. Implement the trait
//!
//! ```ignore
//! /// Deletes the rows of MY_TABLE that nothing references any more.
//! #[derive(Default)]
//! pub(crate) struct MyCleanup;
//!
//! impl OnlineMigration for MyCleanup {
//!     const NAME: &'static str = "my_cleanup/1";
//!     const SOURCE: Source = Source::MyTable;
//!     const ON_UNKNOWN: MigrationOnUnknown = MigrationOnUnknown::Keep;
//!     type Row = MyKey;
//!
//!     fn decode(key: &[u8], _value: &[u8]) -> Result<MyKey, StorageError> {
//!         MyKey::deserialize_from(&mut &key[..])
//!     }
//!
//!     fn migrate(
//!         &mut self,
//!         tx: &mut PartitionStoreTransaction<'_>,
//!         row: &MyKey,
//!     ) -> Result<Migrated, StorageError> {
//!         // Look up what decides, through `tx`, and count what it read.
//!         let (referenced, read_bytes) = tx.lookup_for_migration(row.referencing_key())?;
//!         if !referenced {
//!             tx.delete_key(row)?;
//!         }
//!         Ok(Migrated { changed: !referenced, read_bytes })
//!     }
//! }
//! ```
//!
//! `decode` gets a whole key of the source: the walk checks its length first
//! ([`Source::check_key`]), so a table key's decoder, which may panic on a short
//! key, is safe to call. A source the walk doesn't support yet is a new
//! [`Source`] variant, with its scan and key length: the walk only reads
//! partition-keyed tables. A malformed key, or an error from `decode` or
//! `migrate`, fails the slice: nothing it staged commits, the job records the
//! error, and the slice retries after a backoff while the partition keeps
//! serving.
//!
//! # 5. Register it
//!
//! Add `&MigrationDefinition::of::<MyCleanup>()` to [`ONLINE_MIGRATIONS`]. Every
//! fenced store registers it at its next open: one walk and one job if the
//! source has rows, `complete` at once if it has none.
//!
//! # 6. Choose `ON_UNKNOWN`
//!
//! A binary that doesn't know the migration, after a rollback, finds its entry
//! and applies [`MigrationOnUnknown`]:
//!
//! - `Keep` if such a binary may run the store: its writes neither misread the
//!   migrated rows nor break what the migration established, or only undo it in
//!   a harmless way, like a cleanup that may leave some garbage again. It leaves
//!   the entry, and if the migration is in progress, its walk is discarded and
//!   restarts from the beginning when a binary that knows the migration opens
//!   the store. Most migrations are `Keep`.
//! - `Refuse` if such a binary must not run the store: it fails to open it. This
//!   blocks rollbacks below the release that adds the migration once it has
//!   registered; say so in the release note.
//!
//! No version floor is needed for either: binaries older than the registry
//! can't open a fenced store, and every other binary reads the policy from the
//! entry.
//!
//! # 7. Test it
//!
//! Use the harness in `testing`, which runs your migration as a node would:
//!
//! ```ignore
//! const MY_CLEANUP: &MigrationDefinition = &MigrationDefinition::of::<MyCleanup>();
//!
//! let mut store = Harness::fenced(&[MY_CLEANUP]).await;
//! write_rows(store.store()).await;       // as commands do
//! store.open().await?;                   // registers it and starts its walk
//! store.slice::<MyCleanup>(2).await?;    // one slice of two rows
//! store.open().await?;                   // a restart resumes the walk
//! store.run::<MyCleanup>(2).await?;      // the rest, two rows a slice
//! assert_eq!(store.entry::<MyCleanup>().unwrap().state(), Some(MigrationState::Complete));
//! ```
//!
//! Each harness opens a store of its own, so a test can compare several.
//! Cover at least: a restart in the middle; slices of one row against one
//! large slice, on two stores with the same rows; foreground writes between
//! slices, behind the cursor and beyond the end; a failed slice
//! (`fail_next_slice`) commits nothing and its retry does the work; and an
//! empty source. The worked example's tests show each.
//!
//! # 8. Release note
//!
//! Add one to `release-notes/unreleased/` when users can notice the migration:
//! the data it changes, the time or space it takes, or a rollback it blocks.
//! Operators follow it in `restate.cluster.migration_jobs` (kind `migration`)
//! and see it complete in `restate.cluster.registry`.

use restate_storage_api::StorageError;
use restate_util_string::ReString;

use crate::PartitionStoreTransaction;
use crate::registry::MigrationOnUnknown;
use crate::walk::engine::SliceRunner;
use crate::walk::{OnUnknown, Source, Walk, WalkKind};

/// The online migrations this binary knows, one line each:
/// `&MigrationDefinition::of::<MyCleanup>()`. Each registers at the next open of
/// every store that carries the `registry` storage feature.
pub(crate) const ONLINE_MIGRATIONS: &[&MigrationDefinition] = &[];

/// A one-time change to the rows of one source table, run online; see the
/// module documentation for how to write one.
#[cfg_attr(not(test), expect(dead_code, reason = "ONLINE_MIGRATIONS is empty"))]
pub(crate) trait OnlineMigration: Default + 'static {
    /// The versioned name, `"name/N"`, such as `"jc_orphan_cleanup/1"`. Never
    /// renamed or reused: a changed migration is a new version.
    const NAME: &'static str;
    /// The table it walks, row by row in key order.
    const SOURCE: Source;
    /// What a binary that doesn't know this migration does with its entry.
    const ON_UNKNOWN: MigrationOnUnknown;
    /// One decoded source row.
    type Row;

    /// Decodes the source row stored under `key` with `value`. `key` is a key
    /// of [`Self::SOURCE`] with that table's key length, checked by the walk
    /// ([`Source::check_key`]); `value` is as stored.
    fn decode(key: &[u8], value: &[u8]) -> Result<Self::Row, StorageError>;

    /// Stages the work for `row` in the processor's transaction `tx`, deciding
    /// from what `tx` reads now.
    fn migrate(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        row: &Self::Row,
    ) -> Result<Migrated, StorageError>;
}

/// What [`OnlineMigration::migrate`] did with one row.
#[cfg_attr(not(test), expect(dead_code, reason = "ONLINE_MIGRATIONS is empty"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Migrated {
    /// Whether it changed anything: counted as the job's `rows_changed`.
    pub(crate) changed: bool,
    /// Bytes of keys and values its lookups read, besides the row itself:
    /// counted against the slice's read limit.
    pub(crate) read_bytes: usize,
}

/// What the registry and the walk engine need of an [`OnlineMigration`], built
/// by [`Self::of`].
#[derive(Debug)]
pub(crate) struct MigrationDefinition {
    /// [`OnlineMigration::NAME`].
    pub(crate) name: &'static str,
    /// [`OnlineMigration::SOURCE`].
    pub(crate) source: Source,
    /// [`OnlineMigration::ON_UNKNOWN`].
    pub(crate) on_unknown: MigrationOnUnknown,
    /// The kind of its walks: the name's halves.
    walk: (&'static str, u32),
    /// Stages one slice of its walk: the slice runner monomorphized for it.
    pub(crate) run: SliceRunner,
}

impl MigrationDefinition {
    /// The definition of `M`.
    ///
    /// # Panics
    /// At compile time, in a constant such as [`ONLINE_MIGRATIONS`], if `M::NAME`
    /// isn't `"name/N"` with `N > 0` and no leading zero.
    #[cfg_attr(not(test), expect(dead_code, reason = "ONLINE_MIGRATIONS is empty"))]
    pub(crate) const fn of<M: OnlineMigration>() -> Self {
        Self {
            name: M::NAME,
            source: M::SOURCE,
            on_unknown: M::ON_UNKNOWN,
            walk: split_name(M::NAME),
            run: crate::registry::migration::run::<M>,
        }
    }

    /// The kind of its walks: `jc_orphan_cleanup` version 1 for
    /// `"jc_orphan_cleanup/1"`.
    pub(crate) fn walk_kind(&self) -> WalkKind {
        WalkKind {
            name: ReString::from_static(self.walk.0),
            version: self.walk.1,
        }
    }

    /// The policy of its walks: a kept entry's walk is discarded by a binary
    /// that doesn't know the migration, a refusing one refuses too.
    pub(crate) fn walk_policy(&self) -> OnUnknown {
        match self.on_unknown {
            MigrationOnUnknown::Keep => OnUnknown::Discard,
            MigrationOnUnknown::Refuse => OnUnknown::Refuse,
        }
    }

    /// Whether `walk` is one of its walks: of its kind, over its source.
    pub(crate) fn runs(&self, walk: &Walk) -> bool {
        walk.kind.name == self.walk.0
            && walk.kind.version == self.walk.1
            && walk.source() == Some(self.source)
    }
}

/// Splits a migration name `"name/N"` into `("name", N)`, panicking on any
/// other shape: at compile time, where definitions are constants.
#[cfg_attr(not(test), expect(dead_code, reason = "ONLINE_MIGRATIONS is empty"))]
const fn split_name(name: &'static str) -> (&'static str, u32) {
    let bytes = name.as_bytes();
    let mut at = bytes.len();
    let mut version: u32 = 0;
    let mut scale: u32 = 1;
    while at > 0 && bytes[at - 1].is_ascii_digit() {
        version += (bytes[at - 1] - b'0') as u32 * scale;
        scale *= 10;
        at -= 1;
    }
    assert!(
        at >= 2 && at < bytes.len() && bytes[at - 1] == b'/' && bytes[at] != b'0',
        "an online migration is named `name/N`, with N > 0 and no leading zero"
    );
    (name.split_at(at - 1).0, version)
}

/// Runs online migrations on a partition store as a node does, so that a
/// migration's tests take a few lines each (see the module documentation).
///
/// ```ignore
/// let mut store = Harness::fenced(&[MY_CLEANUP]).await;
/// store.open().await?;
/// store.fail_next_slice();
/// assert!(store.slice::<MyCleanup>(10).await.is_err());
/// assert_eq!(store.job::<MyCleanup>().unwrap().failures, 1);
/// ```
#[cfg(test)]
pub(crate) mod testing {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::time::Duration;

    use restate_rocksdb::{RocksDbManager, RocksError};
    use restate_storage_api::StorageError;
    use restate_types::SemanticRestateVersion;
    use restate_types::identifiers::PartitionId;
    use restate_types::partition_table::Partition;
    use restate_types::sharding::KeyRange;

    use crate::faults::FaultPoint;
    use crate::features::LoadedStorageFeatures;
    use crate::fsm_table::{
        get_min_restate_version_from_partition_db, get_storage_features_from_partition_db,
        get_storage_version_from_partition_db,
    };
    use crate::job::MigrationJob;
    use crate::registry::records::{get_job, get_registry};
    use crate::registry::{FenceState, IndexDefinition, IndexKind, Migration, Requests};
    use crate::walk::engine::{self, Slice, SliceBudget, SliceError};
    use crate::{MigrationError, PartitionStore, PartitionStoreManager, PartitionStoreTransaction};

    use super::{MigrationDefinition, OnlineMigration};

    fn ignore(
        _: &mut PartitionStoreTransaction<'_>,
        _: u32,
        _: &restate_storage_api::vqueue_table::EntryContext<'_>,
        _: Option<&restate_storage_api::vqueue_table::EntryStateRef<'_>>,
        _: Option<&restate_storage_api::vqueue_table::EntryStateRef<'_>>,
    ) {
    }

    /// An index that maintains nothing. Registering it fences the store, as a
    /// release's first index does.
    static FENCE: IndexDefinition = IndexDefinition {
        name: "fence/1",
        kind: IndexKind::Aggregate,
        apply: ignore,
        can_fill: true,
        introduced: crate::registry::TEST_INTRODUCED,
    };

    /// The partition of the next harness's store.
    static NEXT_PARTITION: AtomicU16 = AtomicU16::new(0);

    /// One partition store, opened by a binary that knows some migrations. Each
    /// harness has a partition of its own, so a test can hold several.
    pub(crate) struct Harness {
        store: PartitionStore,
        /// Whether opens register an index, which fences the store.
        fenced: bool,
        known: &'static [&'static MigrationDefinition],
        _manager: Arc<PartitionStoreManager>,
    }

    impl Harness {
        /// A store that the next [`Self::open`] fences, by a binary that knows
        /// `known`. Its migrations register at that open.
        pub(crate) async fn fenced(known: &'static [&'static MigrationDefinition]) -> Self {
            let mut harness = Self::unfenced(known).await;
            harness.fence();
            harness
        }

        /// A store that no open fences: no index registers, so no migration does.
        pub(crate) async fn unfenced(known: &'static [&'static MigrationDefinition]) -> Self {
            RocksDbManager::init();
            let manager = PartitionStoreManager::create(true).await.unwrap();
            let id = PartitionId::from(NEXT_PARTITION.fetch_add(1, Ordering::Relaxed));
            let store = manager
                .open(&Partition::new(id, KeyRange::FULL), None)
                .await
                .unwrap();
            Self {
                store,
                fenced: false,
                known,
                _manager: manager,
            }
        }

        /// From the next open on, an index registers, which fences the store.
        pub(crate) fn fence(&mut self) {
            self.fenced = true;
        }

        /// From the next open on, the binary knows `known`, and no other
        /// migration: a rollback, or a newer release.
        pub(crate) fn knowing(&mut self, known: &'static [&'static MigrationDefinition]) {
            self.known = known;
        }

        /// The store, to seed rows and write as commands do.
        pub(crate) fn store(&mut self) -> &mut PartitionStore {
            &mut self.store
        }

        /// Reconciles the registry as opening the partition does: registers the
        /// known migrations, restarts the walks they lost, and publishes the
        /// records.
        pub(crate) async fn open(&mut self) -> Result<(), MigrationError> {
            let db = self.store.partition_db().clone();
            let mut features = LoadedStorageFeatures::load(
                get_storage_features_from_partition_db(&db).unwrap(),
                get_storage_version_from_partition_db(&db).unwrap(),
                SemanticRestateVersion::current(),
            )
            .unwrap();
            let mut min = get_min_restate_version_from_partition_db(&db).unwrap();
            let requests = Requests {
                maintain: self.fenced.then_some(&FENCE).into_iter().collect(),
                migrations: self.known,
                ..Requests::default()
            };
            self.store
                .reconcile_registry(
                    &[&FENCE],
                    &requests,
                    FenceState {
                        features: &mut features,
                        current_version: SemanticRestateVersion::current(),
                        min_restate_version: &mut min,
                    },
                )
                .await
        }

        /// Runs one slice of `M`'s walk within `budget`, a row count or a
        /// [`SliceBudget`]. A failed slice is recorded on the job, as the
        /// background driver does.
        ///
        /// # Panics
        /// If `M` has no walk.
        pub(crate) async fn slice<M: OnlineMigration>(
            &mut self,
            budget: impl Into<SliceBudget>,
        ) -> Result<Slice, StorageError> {
            let walk = self
                .store
                .published_registry()
                .registry()
                .migration(M::NAME)
                .and_then(|migration| migration.walk)
                .unwrap_or_else(|| panic!("{} has no walk", M::NAME));
            let db = self.store.partition_db().clone();
            let mut tx = self.store.transaction();
            match engine::run_slice(&mut tx, &db, walk, budget).await {
                Ok(slice) => Ok(slice),
                Err(SliceError::Staging(err)) => {
                    let error = engine::describe(&err);
                    engine::record_failure(&mut tx, &db, walk, &error, Duration::ZERO)
                        .await
                        .unwrap();
                    Err(err)
                }
                Err(SliceError::Commit(err)) => Err(err),
            }
        }

        /// Runs `M`'s walk to its end in slices of `rows`, and returns how many
        /// slices that took.
        pub(crate) async fn run<M: OnlineMigration>(
            &mut self,
            rows: usize,
        ) -> Result<usize, StorageError> {
            let mut slices = 1;
            while self.slice::<M>(rows).await? == Slice::Progressed {
                slices += 1;
            }
            Ok(slices)
        }

        /// Makes the next slice fail once it staged its work: it commits
        /// nothing, as when its commit is refused.
        pub(crate) fn fail_next_slice(&self) {
            let faults = self.store.partition_db().faults();
            faults.fail(FaultPoint::SliceStaged, RocksError::ReadOnly);
        }

        /// `M`'s registry entry, as stored.
        pub(crate) fn entry<M: OnlineMigration>(&self) -> Option<Migration> {
            get_registry(self.store.partition_db())
                .unwrap()?
                .migration(M::NAME)
                .cloned()
        }

        /// `M`'s job record, as stored.
        pub(crate) fn job<M: OnlineMigration>(&self) -> Option<MigrationJob> {
            get_job(self.store.partition_db(), self.entry::<M>()?.id)
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use restate_storage_api::Transaction;
    use restate_types::identifiers::InvocationUuid;

    use crate::job::{MigrationJobKind, MigrationJobStatus};
    use crate::journal_table_v2::JournalCompletionIdToCommandIndexKey;
    use crate::keys::{EncodeTableKey, KeyKind};
    use crate::registry::MigrationState::{Complete, InProgress};
    use crate::registry::records;
    use crate::walk::engine::{Slice, SliceBudget};
    use crate::{MigrationError, PartitionStore, StorageAccess};

    use super::testing::Harness;
    use super::*;

    const MARKED: &[u8] = b"marked";
    /// What `Mark` reads per row besides it, as a lookup would.
    const LOOKUP: usize = 100;

    /// Marks each journal completion-index row, unless it is marked already.
    #[derive(Default)]
    struct Mark;

    impl OnlineMigration for Mark {
        const NAME: &'static str = "mark/1";
        const SOURCE: Source = Source::JournalCompletionId;
        const ON_UNKNOWN: MigrationOnUnknown = MigrationOnUnknown::Keep;
        type Row = (Vec<u8>, bool);

        fn decode(key: &[u8], value: &[u8]) -> Result<Self::Row, StorageError> {
            Ok((key.to_vec(), value == MARKED))
        }

        fn migrate(
            &mut self,
            tx: &mut PartitionStoreTransaction<'_>,
            (key, marked): &Self::Row,
        ) -> Result<Migrated, StorageError> {
            if !marked {
                tx.raw_put_cf(KeyKind::JournalV2CompletionIdToCommandIndex, key, MARKED);
            }
            Ok(Migrated {
                changed: !marked,
                read_bytes: LOOKUP,
            })
        }
    }

    /// `Mark` under another name, refusing binaries that don't know it.
    #[derive(Default)]
    struct MarkOrRefuse(Mark);

    impl OnlineMigration for MarkOrRefuse {
        const NAME: &'static str = "mark_or_refuse/1";
        const SOURCE: Source = Mark::SOURCE;
        const ON_UNKNOWN: MigrationOnUnknown = MigrationOnUnknown::Refuse;
        type Row = <Mark as OnlineMigration>::Row;

        fn decode(key: &[u8], value: &[u8]) -> Result<Self::Row, StorageError> {
            Mark::decode(key, value)
        }

        fn migrate(
            &mut self,
            tx: &mut PartitionStoreTransaction<'_>,
            row: &Self::Row,
        ) -> Result<Migrated, StorageError> {
            self.0.migrate(tx, row)
        }
    }

    /// Walks the entry statuses, of which these stores have none.
    #[derive(Default)]
    struct Empty;

    impl OnlineMigration for Empty {
        const NAME: &'static str = "empty/1";
        const SOURCE: Source = Source::EntryStatus;
        const ON_UNKNOWN: MigrationOnUnknown = MigrationOnUnknown::Keep;
        type Row = ();

        fn decode(_: &[u8], _: &[u8]) -> Result<(), StorageError> {
            unreachable!("the source is empty")
        }

        fn migrate(
            &mut self,
            _: &mut PartitionStoreTransaction<'_>,
            _: &(),
        ) -> Result<Migrated, StorageError> {
            unreachable!("the source is empty")
        }
    }

    const MARK: &MigrationDefinition = &MigrationDefinition::of::<Mark>();
    const REFUSE: &MigrationDefinition = &MigrationDefinition::of::<MarkOrRefuse>();
    const EMPTY: &MigrationDefinition = &MigrationDefinition::of::<Empty>();

    /// The completion-index row that [`seed`] writes at `partition_key`.
    fn row_key(partition_key: u64) -> JournalCompletionIdToCommandIndexKey {
        JournalCompletionIdToCommandIndexKey {
            partition_key,
            invocation_uuid: InvocationUuid::from_u128(partition_key.into()),
            completion_id: 1,
        }
    }

    /// Writes unmarked completion-index rows at partition keys `at`, as commands
    /// do, and returns their keys.
    async fn seed(store: &mut PartitionStore, at: impl IntoIterator<Item = u64>) -> Vec<Vec<u8>> {
        let mut tx = store.transaction();
        let keys = at
            .into_iter()
            .map(|partition_key| {
                let mut key = BytesMut::new();
                row_key(partition_key).serialize_to(&mut key);
                tx.raw_put_cf(KeyKind::JournalV2CompletionIdToCommandIndex, &key, b"old");
                key.to_vec()
            })
            .collect();
        tx.commit().await.unwrap();
        keys
    }

    /// Which of the rows `keys` are marked.
    fn marked(store: &mut PartitionStore, keys: &[Vec<u8>]) -> Vec<bool> {
        let db = store.partition_db();
        let raw = db.rocksdb().inner().as_raw_db();
        keys.iter()
            .map(|key| raw.get_cf(db.cf_handle(), key).unwrap().as_deref() == Some(MARKED))
            .collect()
    }

    #[restate_core::test]
    async fn migrations_register_on_fenced_stores_and_complete_without_rows() {
        let mut store = Harness::unfenced(&[MARK, EMPTY]).await;
        seed(store.store(), [1, 2]).await;

        // Without the fence, nothing registers: no entry, no walk, no job.
        store.open().await.unwrap();
        assert_eq!(store.entry::<Mark>(), None);
        assert!(!store.store().storage_features().is_registry);
        assert!(
            store
                .store()
                .published_registry()
                .walks()
                .entries()
                .is_empty()
        );

        // The release's first index fences the store, and the same open
        // registers the migrations from the same counter: one with rows gets a
        // walk and a pending job, one without completes at once.
        store.fence();
        store.open().await.unwrap();
        let mark = store.entry::<Mark>().unwrap();
        assert_eq!(
            (mark.id, mark.state(), mark.walk),
            (65, Some(InProgress), Some(1))
        );
        let job = store.job::<Mark>().unwrap();
        assert_eq!(
            (job.kind, job.status, job.walk, job.created_at),
            (
                MigrationJobKind::Migration,
                MigrationJobStatus::Pending,
                Some(1),
                mark.timeline.registered_at
            )
        );
        let empty = store.entry::<Empty>().unwrap();
        assert_eq!(
            (empty.id, empty.state(), empty.walk),
            (66, Some(Complete), None)
        );
        assert_eq!(empty.timeline.completed_at, empty.timeline.registered_at);
        assert!(store.job::<Empty>().is_none(), "no job without a walk");

        // Reopening registers nothing again.
        let registry = records::get_registry(store.store().partition_db()).unwrap();
        store.open().await.unwrap();
        assert_eq!(
            records::get_registry(store.store().partition_db()).unwrap(),
            registry
        );
    }

    #[restate_core::test]
    async fn walks_run_in_budgeted_slices_resume_after_reopen_and_complete() {
        let mut store = Harness::fenced(&[MARK]).await;
        let mut rows = seed(store.store(), [10, 20, 30, 40, 50]).await;
        store.open().await.unwrap();
        let walk = store.entry::<Mark>().unwrap().walk.unwrap();

        // A migration's lookups read through the slice's transaction: they see
        // its staged writes, and count the bytes of key and value they read.
        let row = rows[0].len() + b"old".len();
        let mut tx = store.store().transaction();
        assert_eq!(tx.lookup_for_migration(row_key(10)).unwrap(), (true, row));
        tx.delete_key(&row_key(10)).unwrap();
        let deleted = tx.lookup_for_migration(row_key(10)).unwrap();
        assert_eq!(deleted, (false, rows[0].len()));
        drop(tx);

        // Two rows; then a read limit that admits the next row but not its
        // lookup: that row's work rolls back and waits for the next slice.
        assert_eq!(store.slice::<Mark>(2).await.unwrap(), Slice::Progressed);
        let tight = SliceBudget {
            read_bytes: 2 * row + LOOKUP + 1,
            ..SliceBudget::from(10)
        };
        assert_eq!(store.slice::<Mark>(tight).await.unwrap(), Slice::Progressed);
        let marked_so_far = [true, true, true, false, false];
        assert_eq!(marked(store.store(), &rows), marked_so_far);
        let job = store.job::<Mark>().unwrap();
        assert_eq!(
            (job.status, job.slices, job.rows_examined, job.rows_changed),
            (MigrationJobStatus::Running, 2, 3, 3)
        );

        // Commands write behind the cursor and beyond the walk's end: those rows
        // are theirs to write in the migrated form, the walk never visits them.
        rows.extend(seed(store.store(), [10, 60]).await.into_iter().skip(1));

        // A restart resumes the walk where it was; the rest takes one slice.
        store.open().await.unwrap();
        assert_eq!(store.entry::<Mark>().unwrap().walk, Some(walk));
        assert_eq!(store.run::<Mark>(2).await.unwrap(), 1);
        assert_eq!(
            marked(store.store(), &rows),
            [false, true, true, true, true, false]
        );

        // Complete for good: in the stored and the published registry, with the
        // job completed and the walk gone.
        let mark = store.entry::<Mark>().unwrap();
        assert_eq!((mark.state(), mark.walk), (Some(Complete), None));
        assert!(mark.timeline.completed_at.is_some() && mark.timeline.completed_by.is_some());
        let published = store.store().published_registry();
        assert_eq!(published.registry().migration(Mark::NAME), Some(&mark));
        assert!(published.walks().entries().is_empty());
        let job = store.job::<Mark>().unwrap();
        assert_eq!(
            (job.status, job.passes, job.rows_changed, job.walk),
            (MigrationJobStatus::Completed, 1, 5, None)
        );
        assert!(job.finished_at.is_some() && job.expires_at.is_some());
        assert!(
            records::load(store.store().partition_db())
                .unwrap()
                .0
                .walks
                .entries()
                .is_empty()
        );
        store.open().await.unwrap();
        assert_eq!(store.entry::<Mark>(), Some(mark), "never runs again");

        // Another store with the same rows does it all in one large slice.
        let mut large = Harness::fenced(&[MARK]).await;
        let rows = seed(large.store(), [10, 20, 30, 40, 50]).await;
        large.open().await.unwrap();
        assert_eq!(large.run::<Mark>(100).await.unwrap(), 1);
        assert_eq!(marked(large.store(), &rows), [true; 5]);
        assert_eq!(large.job::<Mark>().unwrap().rows_changed, 5);
    }

    #[restate_core::test]
    async fn failures_lost_records_and_unknown_migrations() {
        let mut store = Harness::fenced(&[MARK, REFUSE]).await;
        let rows = seed(store.store(), [1, 2, 3]).await;
        store.open().await.unwrap();

        // A failed slice commits nothing; its job records the failure.
        store.fail_next_slice();
        assert!(store.slice::<Mark>(10).await.is_err());
        assert_eq!(marked(store.store(), &rows), [false; 3]);
        let job = store.job::<Mark>().unwrap();
        assert_eq!((job.failures, job.consecutive_failures), (1, 1));
        assert!(job.last_error.is_some());

        // So does a source key cut short, before `decode` sees it.
        let db = store.store().partition_db().clone();
        let raw = db.rocksdb().inner().as_raw_db();
        let short = &rows[0][..rows[0].len() - 1];
        raw.put_cf(db.cf_handle(), short, b"old").unwrap();
        assert!(matches!(
            store.slice::<Mark>(10).await,
            Err(StorageError::DataIntegrityError)
        ));
        assert_eq!(store.job::<Mark>().unwrap().failures, 2);
        raw.delete_cf(db.cf_handle(), short).unwrap();

        // A lost job record is rebuilt from the registry, with unknown times.
        let key = records::RecordKey::job(db.partition().id(), job.id());
        raw.delete_cf(db.cf_handle(), key).unwrap();
        store.open().await.unwrap();
        let rebuilt = store.job::<Mark>().unwrap();
        assert_eq!(
            (rebuilt.status, rebuilt.walk, rebuilt.created_at),
            (MigrationJobStatus::Pending, job.walk, None)
        );

        // A binary that doesn't know a refusing migration doesn't open the store,
        // and changes nothing.
        let registry = records::get_registry(&db).unwrap();
        store.knowing(&[MARK]);
        assert!(matches!(
            store.open().await,
            Err(MigrationError::MigrationBarrier(reason)) if reason.contains("'mark_or_refuse/1'")
        ));
        assert_eq!(records::get_registry(&db).unwrap(), registry);

        // One that doesn't know a kept migration keeps its entry and discards
        // its walk; the job, which never ran a slice, stays pending without it.
        store.knowing(&[REFUSE]);
        store.open().await.unwrap();
        let kept = store.entry::<Mark>().unwrap();
        assert_eq!((kept.state(), kept.walk), (Some(InProgress), None));
        let waiting = store.job::<Mark>().unwrap();
        assert_eq!(
            (waiting.status, waiting.walk),
            (MigrationJobStatus::Pending, None)
        );

        // A binary that knows it again restarts it from the beginning. Rows the
        // other migration marked meanwhile count as unchanged.
        assert_eq!(store.run::<MarkOrRefuse>(2).await.unwrap(), 2);
        store.knowing(&[MARK, REFUSE]);
        store.open().await.unwrap();
        let restarted = store.entry::<Mark>().unwrap().walk;
        assert!(restarted.is_some_and(|walk| Some(walk) != job.walk));
        store.run::<Mark>(10).await.unwrap();
        assert_eq!(marked(store.store(), &rows), [true; 3]);
        let job = store.job::<Mark>().unwrap();
        assert_eq!(
            (job.status, job.rows_changed),
            (MigrationJobStatus::Completed, 0)
        );
        assert_eq!(store.job::<MarkOrRefuse>().unwrap().rows_changed, 3);

        // A complete migration still keeps or refuses as its entry says.
        store.knowing(&[REFUSE]);
        store.open().await.unwrap();
        assert_eq!(store.entry::<Mark>().unwrap().state(), Some(Complete));
        store.knowing(&[]);
        assert!(store.open().await.is_err());
    }
}
