use std::time::{SystemTime, UNIX_EPOCH};

use futures::{StreamExt, stream::BoxStream};
use tokio::{
    runtime::Handle,
    sync::mpsc::{Receiver, Sender},
    task::JoinSet,
};

use crate::{DistError, DistResult};

pub fn timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64
}

pub fn get_local_ip() -> String {
    local_ip_address::local_ip()
        .expect("Failed to get local IP")
        .to_string()
}

pub struct ReceiverStreamBuilder<O> {
    tx: Sender<DistResult<O>>,
    rx: Receiver<DistResult<O>>,
    join_set: JoinSet<DistResult<()>>,
}

impl<O: Send + 'static> ReceiverStreamBuilder<O> {
    /// Create new channels with the specified buffer size
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);

        Self {
            tx,
            rx,
            join_set: JoinSet::new(),
        }
    }

    /// Get a handle for sending data to the output
    pub fn tx(&self) -> Sender<DistResult<O>> {
        self.tx.clone()
    }

    /// Same as [`Self::spawn`] but it spawns the task on the provided runtime
    pub fn spawn_on<F>(&mut self, task: F, handle: &Handle)
    where
        F: Future<Output = DistResult<()>>,
        F: Send + 'static,
    {
        self.join_set.spawn_on(task, handle);
    }

    /// Create a stream of all data written to `tx`
    pub fn build(self) -> BoxStream<'static, DistResult<O>> {
        let Self {
            tx,
            rx,
            mut join_set,
        } = self;

        // Doesn't need tx
        drop(tx);

        // future that checks the result of the join set, and propagates panic if seen
        let check = async move {
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(task_result) => {
                        match task_result {
                            // Nothing to report
                            Ok(_) => continue,
                            // This means a blocking task error
                            Err(error) => return Some(Err(error)),
                        }
                    }
                    // This means a tokio task error, likely a panic
                    Err(e) => {
                        return Some(Err(DistError::internal(format!("Tokio join error: {e}"))));
                    }
                }
            }
            None
        };

        let check_stream = futures::stream::once(check)
            // unwrap Option / only return the error
            .filter_map(|item| async move { item });

        // Convert the receiver into a stream
        let rx_stream = futures::stream::unfold(rx, |mut rx| async move {
            let next_item = rx.recv().await;
            next_item.map(|next_item| (next_item, rx))
        });

        // Merge the streams together so whichever is ready first
        // produces the batch
        futures::stream::select(rx_stream, check_stream).boxed()
    }
}
