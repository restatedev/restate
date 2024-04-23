// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod io;
mod segmented_queue;

pub use segmented_queue::SegmentQueue;

#[cfg(test)]
mod tests {
    use super::*;

    use restate_types::identifiers::InvocationId;
    use tempfile::tempdir;

    #[tokio::test]
    async fn simple_example() {
        let temp_dir = tempdir().unwrap();
        let mut queue = SegmentQueue::new(temp_dir.path(), 1);

        queue.enqueue(1).await;
        queue.enqueue(2).await;
        queue.enqueue(3).await;
        queue.enqueue(4).await;

        assert_eq!(queue.dequeue().await, Some(1));
        assert_eq!(queue.dequeue().await, Some(2));
        assert_eq!(queue.dequeue().await, Some(3));
        assert_eq!(queue.dequeue().await, Some(4));

        assert_eq!(queue.dequeue().await, None);
    }

    #[tokio::test]
    async fn serde_sid() {
        let temp_dir = tempdir().unwrap();
        let mut queue = SegmentQueue::new(temp_dir.path(), 1);

        queue.enqueue(InvocationId::mock_random()).await;
        queue.enqueue(InvocationId::mock_random()).await;
        queue.enqueue(InvocationId::mock_random()).await;
        queue.enqueue(InvocationId::mock_random()).await;

        assert!(queue.dequeue().await.is_some());
        assert!(queue.dequeue().await.is_some());
        assert!(queue.dequeue().await.is_some());
        assert!(queue.dequeue().await.is_some());

        assert_eq!(queue.dequeue().await, None);
    }

    #[tokio::test]
    async fn alternate_enq_deq() {
        let temp_dir = tempdir().unwrap();
        let mut queue = SegmentQueue::new(temp_dir.path(), 1);

        queue.enqueue(1).await;
        assert_eq!(queue.dequeue().await, Some(1));

        queue.enqueue(2).await;
        assert_eq!(queue.dequeue().await, Some(2));

        assert_eq!(queue.dequeue().await, None);
    }
}
