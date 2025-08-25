// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::LogBuilder;
use restate_types::{
    Version,
    logs::{
        LogId,
        metadata::{LogletRef, ProviderKind, Segment},
    },
    replicated_loglet::ReplicatedLogletParams,
};

pub(crate) fn append_segment_row(
    builder: &mut LogBuilder,
    ver: Version,
    id: LogId,
    segment: &Segment<'_>,
    replicated_loglet_params: Option<&LogletRef<ReplicatedLogletParams>>,
) {
    let mut row = builder.row();

    row.logs_metadata_version(ver.into());

    row.log_id(id.into());
    row.segment_index(segment.config.index().into());
    row.base_lsn(segment.base_lsn.into());
    row.fmt_kind(segment.config.kind);

    if segment.config.kind == ProviderKind::Replicated {
        if let Some(LogletRef { params, .. }) = replicated_loglet_params {
            row.fmt_loglet_id(params.loglet_id);
            row.fmt_sequencer(params.sequencer);
            row.fmt_replication(&params.replication);
            row.fmt_nodeset(&params.nodeset);
        }
    }
}
