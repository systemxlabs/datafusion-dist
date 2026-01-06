use std::{fmt::Debug, sync::Arc};

use log::{debug, error};
use tokio::{runtime::Handle, sync::Notify};

pub trait DistExecutor: Debug + Send + Sync {
    fn handle(&self) -> &Handle;
}

/// Creates a Tokio [`Runtime`] for use with CPU bound tasks
///
/// Tokio forbids dropping `Runtime`s in async contexts, so creating a separate
/// `Runtime` correctly is somewhat tricky. This structure manages the creation
/// and shutdown of a separate thread.
///
/// # Notes
/// On drop, the thread will wait for all remaining tasks to complete.
///
/// Depending on your application, more sophisticated shutdown logic may be
/// required, such as ensuring that no new tasks are added to the runtime.
///
/// # Credits
/// This code is derived from code originally written for [InfluxDB 3.0]
///
/// [InfluxDB 3.0]: https://github.com/influxdata/influxdb3_core/tree/6fcbb004232738d55655f32f4ad2385523d10696/executor
#[derive(Debug)]
pub struct DefaultExecutor {
    /// Handle is the tokio structure for interacting with a Runtime.
    handle: Handle,
    /// Signal to start shutting down
    notify_shutdown: Arc<Notify>,
    /// When thread is active, is Some
    thread_join_handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for DefaultExecutor {
    fn drop(&mut self) {
        // Notify the thread to shutdown.
        self.notify_shutdown.notify_one();
        // In a production system you also need to ensure your code stops adding
        // new tasks to the underlying runtime after this point to allow the
        // thread to complete its work and exit cleanly.
        if let Some(thread_join_handle) = self.thread_join_handle.take() {
            // If the thread is still running, we wait for it to finish
            debug!("Shutting down default executor thread...");
            if let Err(e) = thread_join_handle.join() {
                error!("Error joining default executor thread: {e:?}",);
            } else {
                debug!("Default executor thread shutdown successfully.");
            }
        }
    }
}

impl DefaultExecutor {
    /// Create a new Tokio Runtime for CPU bound tasks
    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("default-executor-worker")
            .build()
            .expect("Creating tokio runtime");
        let handle = runtime.handle().clone();
        let notify_shutdown = Arc::new(Notify::new());
        let notify_shutdown_captured = Arc::clone(&notify_shutdown);

        // The runtime runs and is dropped on a separate thread
        let thread_join_handle = std::thread::spawn(move || {
            runtime.block_on(async move {
                notify_shutdown_captured.notified().await;
            });
            // Note: runtime is dropped here, which will wait for all tasks
            // to complete
        });

        Self {
            handle,
            notify_shutdown,
            thread_join_handle: Some(thread_join_handle),
        }
    }

    /// Return a handle suitable for spawning CPU bound tasks
    ///
    /// # Notes
    ///
    /// If a task spawned on this handle attempts to do IO, it will error with a
    /// message such as:
    ///
    /// ```text
    /// A Tokio 1.x context was found, but IO is disabled.
    /// ```
    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

impl DistExecutor for DefaultExecutor {
    fn handle(&self) -> &Handle {
        self.handle()
    }
}

impl Default for DefaultExecutor {
    fn default() -> Self {
        Self::new()
    }
}

pub fn logging_executor_metrics(handle: &Handle) {
    let metrics = handle.metrics();
    debug!(
        "Executor metrics num_workers: {}, num_alive_tasks: {}, global_queue_depth: {}",
        metrics.num_workers(),
        metrics.num_alive_tasks(),
        metrics.global_queue_depth()
    );
}
