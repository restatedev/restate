// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::table_util::format_using;

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
    output: &mut String,
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
    row.kind(format_using(output, &segment.config.kind));

    if segment.config.kind == ProviderKind::Replicated
        && let Some(LogletRef { params, .. }) = replicated_loglet_params
    {
        row.loglet_id(format_using(output, &params.loglet_id));
        row.sequencer(format_using(output, &params.sequencer));
        row.replication(format_using(output, &params.replication));
        row.nodeset(format_using(output, &params.nodeset));
    }
}
