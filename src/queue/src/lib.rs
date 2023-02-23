mod io;
mod segmented_queue;

pub use segmented_queue::SegmentQueue;

#[cfg(test)]
mod tests {
    use super::*;
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
