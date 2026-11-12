//! Spans for the work a node does on behalf of a job.
//!
//! The library's own lines are `log` records. A host that bridges `log` into
//! `tracing` — as datafusion's own dist binary does — only gets them attributed
//! to a job when there is a span to attribute them to. So each time work
//! changes hands the work is named after the job, stage and partition it
//! belongs to.
//!
//! Two rules matter more than the names:
//!
//! * A span does not follow a future into a `tokio::spawn` or a
//!   `Handle::spawn` — and every task runs on dist's own CPU-bound runtime —
//!   nor does it follow a stream into whoever polls it, a tonic server or the
//!   `ProxyExec` of the stage that consumes this one. Whoever hands a future to
//!   a runtime or a stream to a caller instruments it explicitly; see
//!   [`DistRuntime::submit`] and [`DistRuntime::execute_local`].
//! * A span is local to a process. One job spans several processes, and linking
//!   them is the transport's job, not this module's.
//!
//! Spans for single tasks are `DEBUG`, the level this crate logs at, because a
//! job can have thousands of them; a host that wants the per-task view is
//! asking for `datafusion_dist` at debug level anyway. Job-level spans are
//! `INFO`, one per job.
//!
//! [`DistRuntime::submit`]: crate::runtime::DistRuntime::submit
//! [`DistRuntime::execute_local`]: crate::runtime::DistRuntime::execute_local

use tracing::Span;

use crate::{cluster::NodeId, planner::TaskId};

/// The span covering [`DistRuntime::submit`]: the job as the submitting node
/// sees it. `stages` and `nodes` are recorded once the plan has been staged and
/// the tasks distributed.
///
/// [`DistRuntime::submit`]: crate::runtime::DistRuntime::submit
pub(crate) fn submit_span(job_id: &str) -> Span {
    tracing::info_span!(
        "dist.job.submit",
        job_id = %job_id,
        stages = tracing::field::Empty,
        nodes = tracing::field::Empty,
    )
}

/// The span covering the periodic check for jobs that completed everywhere.
pub(crate) fn check_completed_span(jobs: usize) -> Span {
    tracing::info_span!("dist.job.check_completed", jobs)
}

/// The span covering the cleanup of jobs on the nodes that ran them.
pub(crate) fn cleanup_jobs_span(jobs: usize) -> Span {
    tracing::info_span!("dist.job.cleanup", jobs)
}

/// The span covering the wait for a job's stage0 tasks to be polled, after
/// which the job is given up on.
pub(crate) fn stage0_poll_span(jobs: usize) -> Span {
    tracing::info_span!("dist.stage0.poll", jobs)
}

/// The span covering delivery of the tasks of one job to one node.
pub(crate) fn send_tasks_span(node_id: &NodeId, tasks: usize) -> Span {
    tracing::debug_span!("dist.job.send_tasks", node_id = %node_id, tasks)
}

/// The span covering a node receiving the tasks of a job.
pub(crate) fn receive_tasks_span(job_id: &str, node_id: &NodeId, tasks: usize) -> Span {
    tracing::debug_span!("dist.job.receive_tasks", job_id = %job_id, node_id = %node_id, tasks)
}

/// The span covering the execution of one task on one node, whether the node is
/// the local one or one the task is fetched from.
pub(crate) fn task_span(task_id: &TaskId, node_id: &NodeId) -> Span {
    tracing::debug_span!(
        "dist.task.execute",
        job_id = %task_id.job_id,
        stage = task_id.stage,
        partition = task_id.partition,
        node_id = %node_id,
    )
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fmt,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        task::Poll,
    };

    use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, tree_node::TreeNodeRecursion};
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    use datafusion_physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
        empty::EmptyExec, stream::RecordBatchStreamAdapter,
    };
    use futures::StreamExt;
    use parking_lot::Mutex;
    use tokio::runtime::Handle;
    use tracing::{
        Event, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id, Record},
    };

    use crate::{
        DistError, DistResult,
        cluster::{DistCluster, NodeId, NodeState},
        config::DistConfig,
        executor::DistExecutor,
        network::{DistNetwork, ScheduledTasks},
        planner::{StageId, TaskId},
        runtime::DistRuntime,
    };

    /// Runs the tasks of a stage and logs from inside the poll, which is where
    /// a plan reads its batches and where dist's own executor lines are written.
    #[derive(Debug)]
    struct ProbeExec {
        schema: SchemaRef,
        properties: Arc<PlanProperties>,
    }

    impl DisplayAs for ProbeExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "ProbeExec")
        }
    }

    impl ExecutionPlan for ProbeExec {
        fn name(&self) -> &str {
            "ProbeExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(
                &Arc<dyn PhysicalExpr>,
            ) -> Result<TreeNodeRecursion, DataFusionError>,
        ) -> Result<TreeNodeRecursion, DataFusionError> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream, DataFusionError> {
            let stream = futures::stream::poll_fn(|_cx| {
                tracing::debug!("probe: polled");
                Poll::Ready(None::<Result<arrow::array::RecordBatch, DataFusionError>>)
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream,
            )))
        }
    }

    #[derive(Debug)]
    struct SingleNodeCluster {
        node: NodeId,
    }

    #[async_trait::async_trait]
    impl DistCluster for SingleNodeCluster {
        async fn heartbeat(&self, _node_id: NodeId, _state: NodeState) -> DistResult<()> {
            Ok(())
        }

        async fn alive_nodes(&self) -> DistResult<HashMap<NodeId, NodeState>> {
            Ok(HashMap::from([(self.node.clone(), NodeState::default())]))
        }
    }

    /// A job whose tasks all stay on one node never reaches the network.
    #[derive(Debug)]
    struct UnreachableNetwork {
        node: NodeId,
    }

    #[async_trait::async_trait]
    impl DistNetwork for UnreachableNetwork {
        fn local_node(&self) -> NodeId {
            self.node.clone()
        }

        async fn send_tasks(&self, _node_id: NodeId, _tasks: ScheduledTasks) -> DistResult<()> {
            Err(DistError::internal("single node test"))
        }

        async fn execute_task(
            &self,
            _node_id: NodeId,
            _task_id: TaskId,
        ) -> DistResult<SendableRecordBatchStream> {
            Err(DistError::internal("single node test"))
        }

        async fn get_jobs(
            &self,
            _node_id: NodeId,
            _job_ids: Option<Vec<crate::JobId>>,
        ) -> DistResult<HashMap<StageId, crate::network::StageInfo>> {
            Err(DistError::internal("single node test"))
        }

        async fn cleanup_jobs(
            &self,
            _node_id: NodeId,
            _job_ids: Vec<crate::JobId>,
        ) -> DistResult<()> {
            Err(DistError::internal("single node test"))
        }
    }

    /// Keeps the tasks on the test's own current-thread runtime, so that the
    /// thread-local subscriber below sees what the spawned driver writes.
    #[derive(Debug)]
    struct CurrentRuntime {
        handle: Handle,
    }

    impl DistExecutor for CurrentRuntime {
        fn handle(&self) -> &Handle {
            &self.handle
        }
    }

    #[derive(Debug, Clone)]
    struct SpanRecord {
        id: u64,
        name: String,
        fields: Vec<(String, String)>,
    }

    impl SpanRecord {
        fn field(&self, name: &str) -> Option<&str> {
            self.fields
                .iter()
                .find(|(field, _)| field == name)
                .map(|(_, value)| value.as_str())
        }
    }

    #[derive(Debug, Clone)]
    struct EventRecord {
        message: String,
        /// The span that was current when the event was recorded.
        span: u64,
    }

    #[derive(Debug, Default)]
    struct Recorder {
        spans: Mutex<Vec<SpanRecord>>,
        events: Mutex<Vec<EventRecord>>,
        next_id: AtomicU64,
    }

    /// The recorder behind a `Subscriber`, which has to be `'static`.
    #[derive(Debug, Clone)]
    struct Recording(Arc<Recorder>);

    thread_local! {
        /// Spans entered on this thread, innermost last. The subscriber records
        /// the top of this stack as the current span of an event, which for a
        /// `log` -> `tracing` bridge is what attributes a line to a job.
        static STACK: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    struct FieldVisitor<'a>(&'a mut Vec<(String, String)>);

    impl Visit for FieldVisitor<'_> {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.0
                .push((field.name().to_string(), format!("{value:?}")));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.push((field.name().to_string(), value.to_string()));
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.push((field.name().to_string(), value.to_string()));
        }
    }

    impl Subscriber for Recording {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, attrs: &Attributes<'_>) -> Id {
            // Span ids are non-zero: `Id::from_u64(0)` is not a valid id.
            let id = self.0.next_id.fetch_add(1, Ordering::Relaxed) + 1;
            let mut fields = Vec::new();
            attrs.record(&mut FieldVisitor(&mut fields));
            self.0.spans.lock().push(SpanRecord {
                id,
                name: attrs.metadata().name().to_string(),
                fields,
            });
            Id::from_u64(id)
        }

        fn record(&self, span: &Id, values: &Record<'_>) {
            let mut fields = Vec::new();
            values.record(&mut FieldVisitor(&mut fields));
            if let Some(record) = self
                .0
                .spans
                .lock()
                .iter_mut()
                .find(|record| record.id == span.into_u64())
            {
                record.fields.extend(fields);
            }
        }

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut fields = Vec::new();
            event.record(&mut FieldVisitor(&mut fields));
            let message = fields
                .iter()
                .find(|(field, _)| field == "message")
                .map(|(_, value)| value.clone())
                .unwrap_or_default();
            let span = STACK.with(|stack| stack.borrow().last().copied().unwrap_or(0));
            self.0.events.lock().push(EventRecord { message, span });
        }

        fn enter(&self, span: &Id) {
            STACK.with(|stack| stack.borrow_mut().push(span.into_u64()));
        }

        fn exit(&self, span: &Id) {
            STACK.with(|stack| {
                let mut stack = stack.borrow_mut();
                if stack.last() == Some(&span.into_u64()) {
                    stack.pop();
                }
            });
        }
    }

    /// A line written while reading batches belongs to the job: the span opened
    /// when the node received the tasks is not enough on its own, because the
    /// plan runs in a task spawned on dist's executor and the stream is polled
    /// by whoever fetched the task.
    #[tokio::test]
    async fn task_lines_are_attributed_to_the_job() {
        let recorder = Arc::new(Recorder::default());
        let _guard = tracing::subscriber::set_default(Recording(recorder.clone()));

        let node = NodeId {
            host: "127.0.0.1".to_string(),
            port: 50050,
        };
        let ctx = SessionContext::new();
        let runtime = DistRuntime::new(
            ctx.task_ctx(),
            Arc::new(DistConfig::default()),
            Arc::new(SingleNodeCluster { node: node.clone() }),
            Arc::new(UnreachableNetwork { node: node.clone() }),
        )
        .with_executor(Arc::new(CurrentRuntime {
            handle: Handle::current(),
        }));

        let job_id: crate::JobId = "test-job".into();
        let task_id = TaskId {
            job_id: job_id.clone(),
            stage: 0,
            partition: 0,
        };

        let schema: SchemaRef = Arc::new(Schema::new(vec![ArrowField::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ProbeExec {
            schema: schema.clone(),
            properties: EmptyExec::new(schema).properties().clone(),
        });
        let distribution = Arc::new(HashMap::from([(task_id.clone(), node.clone())]));
        let scheduled_tasks = ScheduledTasks::new(
            HashMap::from([(task_id.stage_id(), plan)]),
            vec![task_id.clone()],
            distribution,
            // The job meta is the caller's business data: it holds a `req_id`
            // this library knows nothing about and must not look for.
            Arc::new(HashMap::from([("req_id".to_string(), "req-1".to_string())])),
        );

        runtime
            .receive_tasks(scheduled_tasks)
            .await
            .expect("receive tasks");

        let mut stream = runtime
            .execute_local(task_id.clone())
            .await
            .expect("execute local");
        while stream.next().await.is_some() {}
        drop(stream);

        let spans = recorder.spans.lock().clone();
        let receive = spans
            .iter()
            .find(|span| span.name == "dist.job.receive_tasks")
            .expect("receive_tasks span");
        assert_eq!(receive.field("job_id"), Some("test-job"));
        assert_eq!(receive.field("tasks"), Some("1"));
        assert_eq!(receive.field("req_id"), None);

        let task = spans
            .iter()
            .find(|span| span.name == "dist.task.execute")
            .expect("execute_task span");
        assert_eq!(task.field("job_id"), Some("test-job"));
        assert_eq!(task.field("stage"), Some("0"));
        assert_eq!(task.field("partition"), Some("0"));
        assert_eq!(task.field("node_id"), Some("127.0.0.1:50050"));

        let events = recorder.events.lock().clone();
        let probe = events
            .iter()
            .find(|event| event.message.contains("probe: polled"))
            .expect("the plan was polled");
        let probe_span = spans
            .iter()
            .find(|span| span.id == probe.span)
            .expect("the event is attributed to a span");
        assert_eq!(probe_span.name, "dist.task.execute");
    }
}
