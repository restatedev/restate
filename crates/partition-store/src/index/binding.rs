// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The schema binding of the service index: prepares a selection over the
//! `EntryByService` logical table as a read of the `EntryByStageService` index,
//! and turns each index key into one row, decoding only the columns the query
//! asked for.

use std::ops::ControlFlow;

use restate_storage_api::StorageError;
use restate_storage_query_api::schema::RecordProgress;
use restate_storage_query_api::selection::Selection;
use restate_storage_query_api::tables::index::{EntryByService, EntryByServiceView};
use restate_storage_query_api::{LogicalSchema, PreparedRead, RowSink, SchemaBinding, Table};

use crate::keys::selection::PrepareKeySelection;
use crate::{Result, break_on_err};

use super::{EntryByStageService, EntryByStageServiceKey};

pub enum EntryByServiceBinding {}

impl SchemaBinding for EntryByServiceBinding {
    type Schema = EntryByService;
    type Table = EntryByStageService;
    type Materializer = ();

    fn materialize_next(
        record: <Self::Table as Table>::Record<'_>,
        _: &mut (),
        emit: &mut impl RowSink<Self::Schema>,
    ) -> crate::Result<RecordProgress> {
        if let ControlFlow::Break(result) = Self::materialize_projected(record, emit) {
            result?;
        }
        Ok(RecordProgress::Done)
    }

    fn prepare(selection: &Selection<Self::Schema>) -> Result<PreparedRead<Self::Table>> {
        let request = EntryByStageServiceKey::prepare_candidates(selection)?;
        let precision = request.precision();
        Ok(PreparedRead::new(request, precision))
    }

    fn materialize(
        (key, payload): <Self::Table as Table>::Record<'_>,
        emit: &mut impl for<'a> FnMut(
            <Self::Schema as LogicalSchema>::Row<'a>,
        ) -> ControlFlow<Result<()>>,
    ) -> ControlFlow<Result<()>> {
        if !payload.is_empty() {
            return ControlFlow::Break(Err(StorageError::DataIntegrityError));
        }
        let key = break_on_err(key.decode_all())?;
        emit(EntryByServiceView {
            service_name: key.service_name.as_str(),
            stage: key.stage,
            transitioned_at: key.transitioned_at.0.to_unix_millis(),
            canonical_id: key.canonical_id,
        })
    }

    fn materialize_projected(
        (key, payload): <Self::Table as Table>::Record<'_>,
        emit: &mut impl RowSink<Self::Schema>,
    ) -> ControlFlow<Result<()>> {
        if !payload.is_empty() {
            return ControlFlow::Break(Err(StorageError::DataIntegrityError));
        }
        // Structure and complete identity are required even for COUNT(*).
        let key = break_on_err(key.take_all())?;
        let id = break_on_err(key.canonical_id.decode())?;
        emit.append_with(|builder| {
            let mut row = builder.row();
            if row.is_service_name_defined() {
                row.service_name(key.service_name.decode()?);
            }
            if row.is_stage_defined() {
                row.stage(key.stage.decode()?.as_str());
            }
            if row.is_transitioned_at_defined() {
                row.transitioned_at(
                    key.transitioned_at.decode()?.0.to_unix_millis().as_u64() as i64
                );
            }
            row.fmt_canonical_id(id);
            row.fmt_entry_id(id.to_base_entry_id());
            row.partition_key(id.partition_key());
            Ok(())
        })
    }
}
