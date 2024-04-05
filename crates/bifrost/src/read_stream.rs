// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use restate_types::logs::{LogId, Lsn};

use crate::bifrost::BifrostInner;
use crate::{Error, LogRecord};

pub struct LogReadStream {
    inner: Arc<BifrostInner>,
    log_id: LogId,
    read_pointer: Lsn,
}

impl LogReadStream {
    pub(crate) fn new(inner: Arc<BifrostInner>, log_id: LogId, after: Lsn) -> Self {
        Self {
            inner,
            log_id,
            read_pointer: after,
        }
    }

    fn seek_to(&mut self, record: &LogRecord) {
        let read_pointer = match &record.record {
            // On trim gaps, we fast-forward the read pointer to the end of the gap. We do
            // this after delivering a TrimGap record. This means that the next read operation
            // skips over the boundary of the gap.
            crate::Record::TrimGap(trim_gap) => trim_gap.until,
            crate::Record::Data(_) => record.offset,
            crate::Record::Seal(_) => record.offset,
        };
        self.read_pointer = read_pointer;
    }

    /// Read the next record from the log after the current read pointer. The future will resolve
    /// after the record is available to read, this will async-block indefinitely if no records are
    /// ever written to the log beyond the read pointer.
    ///
    /// This future is "Cancellation" safe.
    pub async fn read_next(&mut self) -> Result<LogRecord, Error> {
        let record = self
            .inner
            .read_next_single(self.log_id, self.read_pointer)
            .await?;

        self.seek_to(&record);
        Ok(record)
    }

    /// Like `read_next` but returns `None` if there are no more records to read.
    pub async fn read_next_opt(&mut self) -> Result<Option<LogRecord>, Error> {
        let record_opt = self
            .inner
            .read_next_single_opt(self.log_id, self.read_pointer)
            .await?;
        if let Some(ref record) = record_opt {
            self.seek_to(record);
        }
        Ok(record_opt)
    }

    /// Current read pointer. This is the LSN of the last read record, or the
    /// LSN that we will read "after" if we call `read_next`.
    pub fn current_read_pointer(&self) -> Lsn {
        self.read_pointer
    }
}

#[cfg(test)]
mod tests {

    use crate::Bifrost;

    use super::*;

    use googletest::prelude::*;
    use restate_core::{TaskKind, TestCoreEnv};
    use tracing::info;
    use tracing_test::traced_test;

    use restate_types::logs::Payload;

    #[tokio::test]
    #[traced_test]
    async fn test_basic_readstream() -> Result<()> {
        let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        let tc = node_env.tc;
        tc.run_in_scope("test", None, async {
            let read_after = Lsn::from(5);

            let mut bifrost = Bifrost::init().await;

            let mut reader = bifrost.create_reader(LogId::from(0), Lsn::from(5));
            assert_eq!(read_after, reader.current_read_pointer());

            // We have not written anything yet, this should return None.
            assert!(reader.read_next_opt().await?.is_none());
            // read points should not change, nothing has been read.
            assert_eq!(read_after, reader.current_read_pointer());

            // spawn a reader that reads 5 records and exits.
            let id = tc.spawn(TaskKind::Disposable, "read-records", None, async move {
                for i in 1..=5 {
                    let record = reader.read_next().await?;
                    let expected_lsn = Lsn::from(i) + read_after;
                    info!(?record, "read record");
                    assert_eq!(expected_lsn, record.offset);
                    assert_eq!(
                        Payload::from(format!("record{}", expected_lsn)),
                        record.record.into_payload_unchecked()
                    );
                    assert_eq!(expected_lsn, reader.current_read_pointer());
                }
                Ok(())
            })?;

            let reader_bg_handle = tc.take_task(id).expect("read-records task to exist");

            tokio::task::yield_now().await;
            // Not finished, we still didn't append records
            assert!(!reader_bg_handle.is_finished());

            // append 5 records to the log
            for i in 1..=5 {
                let lsn = bifrost
                    .append(LogId::from(0), format!("record{}", i).into())
                    .await?;
                info!(?lsn, "appended record");
            }

            // Written records are not enough for the reader to finish.
            // Not finished, we still didn't append records
            tokio::task::yield_now().await;
            assert!(!reader_bg_handle.is_finished());
            assert!(!logs_contain("read record"));

            // write 5 more records.
            for i in 6..=10 {
                bifrost
                    .append(LogId::from(0), format!("record{}", i).into())
                    .await?;
            }

            // reader has finished
            reader_bg_handle.await?;
            assert!(logs_contain("read record"));

            Ok(())
        })
        .await
    }
}
