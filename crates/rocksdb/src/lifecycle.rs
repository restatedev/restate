// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use restate_util_lifecycle::{Gate, KeepAlive, KeepAliveTarget, ScopeParent};

use crate::RocksDb;

impl KeepAliveTarget for RocksDb {
    fn name(&self) -> &str {
        RocksDb::name(self).as_str()
    }
}

/// A database is the top of a tree of scopes. Sealing or draining the database
/// also seals or drains every scope inside it.
impl ScopeParent for RocksDb {
    fn gate(&self) -> &Arc<Gate> {
        RocksDb::gate(self)
    }
}

impl RocksDb {
    /// Keeps this database open until the returned value and all of its clones
    /// have been dropped.
    pub fn keep_alive(self: &Arc<Self>) -> KeepAlive<RocksDb> {
        KeepAlive::from(Arc::clone(self))
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Waker};

    use restate_types::protobuf::common::DatabaseKind;
    use restate_util_lifecycle::Scope;

    use crate::configuration::{
        CfConfigurator, DbConfigurator, create_default_cf_options, create_default_db_options,
    };
    use crate::{
        CfPrefixPattern, DbEnv, DbName, DbSpecBuilder, GlobalExecutor, RocksAccess, RocksDb,
    };

    struct Configurator;

    impl DbConfigurator for Configurator {
        fn get_db_options(&self, name: &DbName, env: &DbEnv) -> rocksdb::Options {
            create_default_db_options(env, name)
        }
    }

    impl CfConfigurator for Configurator {
        fn get_cf_options(&self, _: &DbName, _: &str, env: &DbEnv) -> rocksdb::Options {
            create_default_cf_options(env.write_buffer_manager.as_ref())
        }
    }

    fn is_pending(future: impl Future) -> bool {
        pin!(future)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    }

    #[test]
    fn scopes_inside_the_database_keep_it_open_and_are_sealed_with_it() -> anyhow::Result<()> {
        let tempdir = restate_test_util::tempdir()?;
        let spec = DbSpecBuilder::new(
            DbName::new("keep-alive"),
            DatabaseKind::PartitionStore,
            tempdir.path().join("db"),
            Configurator,
        )
        .add_cf_pattern(CfPrefixPattern::ANY, Configurator)
        .build()?;
        let executor = GlobalExecutor::new(
            threadpool::ThreadPool::new(1),
            threadpool::ThreadPool::new(1),
        );
        let db = Arc::new(RocksDb::new(
            RocksAccess::open_db(spec, DbEnv::default())?,
            executor,
        ));
        let mut closed = pin!(db.closed());

        // A scope inside the database is sealed together with it.
        let keep_alive = db.keep_alive();
        assert_eq!(format!("{keep_alive:?}"), r#"KeepAlive("keep-alive")"#);
        let scope = Scope::new("partition", keep_alive.clone());
        db.seal();
        assert!(scope.gate().try_acquire().is_none());

        // The keep-alive and the scope outlive the database handle they came
        // from.
        drop(db);
        drop(keep_alive);
        assert!(is_pending(closed.as_mut()));

        // Without a manager, dropping the last reference closes the database on
        // the dropping thread.
        drop(scope);
        assert!(!is_pending(closed.as_mut()));
        Ok(())
    }
}
