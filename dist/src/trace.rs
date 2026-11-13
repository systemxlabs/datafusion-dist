//! The trace of a job, and the spans that name the work a node does for it.
//!
//! The library's own lines are `log` records. A host that bridges `log` into
//! `tracing` — as datafabric does — only gets them attributed to a request when
//! there is a span to attribute them to, and a distributed query crosses
//! processes, so the span has to carry ids that the other processes already
//! know. Those ids travel the way the rest of the world sends them: a W3C
//! `traceparent`, in the metadata of the call that starts the work.
//!
//! Two rules matter more than the names:
//!
//! * A span does not follow a future into a `tokio::spawn` or a
//!   `Handle::spawn` — and every task runs on dist's own CPU-bound runtime —
//!   nor does it follow a stream into whoever polls it, a tonic server or the
//!   `ProxyExec` of the stage that consumes this one. Whoever hands a future to
//!   a runtime or a stream to a caller instruments it explicitly; see
//!   [`DistRuntime::submit`] and [`DistRuntime::execute_local`].
//! * A context describes one hop, not one job. `trace_id` is the invariant; the
//!   span id changes at every hop, so each node derives its own from the
//!   context it was handed and passes the derived one on. That is what makes
//!   the tree `submit → send_tasks → receive_tasks → execute_task` visible in
//!   the log lines of three processes.
//!
//! Spans for single tasks are `DEBUG`, the level this crate logs at, because a
//! job can have thousands of them; a host that wants the per-task view is
//! asking for `datafusion_dist` at debug level anyway. Job-level spans are
//! `INFO`, one per job.
//!
//! [`DistRuntime::submit`]: crate::runtime::DistRuntime::submit
//! [`DistRuntime::execute_local`]: crate::runtime::DistRuntime::execute_local

use std::fmt;

use tracing::Span;

use crate::{DistError, DistResult, cluster::NodeId, planner::TaskId};

/// The header a call carries its trace in, as the W3C Trace Context
/// recommendation spells it.
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// Bit 0 of the flags field: the trace is sampled.
const SAMPLED: u8 = 0x01;

/// The trace a span belongs to: the trace id, the span it is a child of, and the
/// sampling flag — that is, the four fields of a `traceparent`, minus the span id
/// of the hop that is about to open a span.
///
/// A node receives one of these, derives its own span id from it with
/// [`TraceContext::child`], and hands the derived context on. The library never
/// invents a trace id: a job is traced because the caller asked for it.
///
/// ```text
/// 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
/// ^  ^                                ^                ^
/// |  |                                |                +-- flags: bit 0 = sampled
/// |  |                                +-- parent span id, 8 bytes
/// |  +-- trace id, 16 bytes
/// +-- version, 1 byte
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TraceContext {
    trace_id: Option<[u8; 16]>,
    parent_span_id: Option<[u8; 8]>,
    flags: u8,
}

impl TraceContext {
    /// A job nobody asked to trace. Spans opened from it carry empty ids rather
    /// than a trace of their own, so that "not traced" stays distinguishable
    /// from "traced, but this node lost the context".
    pub fn none() -> Self {
        Self::default()
    }

    /// Whether this context carries a trace at all.
    pub fn is_none(&self) -> bool {
        self.trace_id.is_none()
    }

    /// Parse a `traceparent` header value.
    ///
    /// Returns `None` for everything the recommendation calls invalid: a missing
    /// or non-hex field, a wrong length, an all-zero id, a version of `ff`, or a
    /// version above `00` that we cannot be sure we read correctly. A header
    /// that does not parse is the caller's bug, but it is not worth failing a
    /// query over: a transport that reads one off a header falls back to
    /// [`TraceContext::none`].
    pub fn parse(header: &str) -> Option<Self> {
        let header = header.trim();
        let mut fields = header.split('-');
        let version = fields.next()?;
        if version.len() != 2 || !is_lower_hex(version) || version == "ff" {
            return None;
        }
        let trace_id = parse_hex::<16>(fields.next()?)?;
        let parent_span_id = parse_hex::<8>(fields.next()?)?;
        let flags = parse_hex::<1>(fields.next()?)?;
        // A version-`00` header has exactly four fields; a newer one may append
        // more, and those are readable only if the four we know are intact.
        if fields.next().is_some() && version == "00" {
            return None;
        }
        if trace_id == [0u8; 16] || parent_span_id == [0u8; 8] {
            return None;
        }
        Some(Self {
            trace_id: Some(trace_id),
            parent_span_id: Some(parent_span_id),
            flags: flags[0],
        })
    }

    /// As [`TraceContext::parse`], for a caller that would rather report the
    /// malformed header than run untraced.
    pub fn from_traceparent(header: &str) -> DistResult<Self> {
        Self::parse(header)
            .ok_or_else(|| DistError::internal(format!("Invalid {TRACEPARENT_HEADER}: {header}")))
    }

    /// The trace id as it is logged: the empty string when there is no trace, so
    /// that one query always has the same set of fields.
    pub fn trace_id_hex(&self) -> String {
        self.trace_id.map(hex).unwrap_or_default()
    }

    /// The span this context is a child of, as it is logged.
    pub fn parent_span_id_hex(&self) -> String {
        self.parent_span_id.map(hex).unwrap_or_default()
    }

    pub fn is_sampled(&self) -> bool {
        self.flags & SAMPLED != 0
    }

    /// The header to send with a call that continues this trace: the same trace
    /// and flags, and the span id this context was derived from.
    pub fn traceparent(&self) -> Option<String> {
        let trace_id = self.trace_id?;
        let parent_span_id = self.parent_span_id?;
        Some(format!(
            "00-{}-{}-{:02x}",
            hex(trace_id),
            hex(parent_span_id),
            self.flags
        ))
    }

    /// The context to hand to the children of a span this node is about to open,
    /// and the span id of that span.
    ///
    /// The span id is `None` when there is no trace: a span of an untraced job
    /// still exists, it just has nothing to report.
    pub fn child(&self) -> (Self, Option<[u8; 8]>) {
        let Some(trace_id) = self.trace_id else {
            return (Self::none(), None);
        };
        let span_id = new_span_id();
        (
            Self {
                trace_id: Some(trace_id),
                parent_span_id: Some(span_id),
                flags: self.flags,
            },
            Some(span_id),
        )
    }
}

impl TryFrom<&str> for TraceContext {
    type Error = DistError;

    fn try_from(header: &str) -> DistResult<Self> {
        Self::from_traceparent(header)
    }
}

impl fmt::Display for TraceContext {
    /// The header value, which is what a caller that logs its own context and a
    /// caller that sends it on both want.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.traceparent() {
            Some(header) => f.write_str(&header),
            None => f.write_str(""),
        }
    }
}

/// A span id of a span this node opens.
///
/// Generated rather than derived from anything: a span id names one span, and a
/// job re-executed after a failure is a different span of the same trace.
fn new_span_id() -> [u8; 8] {
    loop {
        let bytes = *uuid::Uuid::new_v4().as_bytes();
        let id = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        if id != [0u8; 8] {
            return id;
        }
    }
}

fn hex<const N: usize>(bytes: [u8; N]) -> String {
    let mut out = String::with_capacity(N * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn parse_hex<const N: usize>(field: &str) -> Option<[u8; N]> {
    if field.len() != N * 2 || !is_lower_hex(field) {
        return None;
    }
    let mut out = [0u8; N];
    for (byte, pair) in out.iter_mut().zip(field.as_bytes().chunks(2)) {
        let pair = std::str::from_utf8(pair).ok()?;
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(out)
}

/// The recommendation wants lowercase hex. Accepting uppercase would make two
/// spellings of one trace id two different traces in a log query.
fn is_lower_hex(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The span id as it is logged.
pub(crate) fn span_id_hex(span_id: Option<[u8; 8]>) -> String {
    span_id.map(hex).unwrap_or_default()
}

/// The span covering [`DistRuntime::submit`]: the job as the submitting node
/// sees it. `stages` and `nodes` are recorded once the plan has been staged and
/// the tasks distributed.
///
/// [`DistRuntime::submit`]: crate::runtime::DistRuntime::submit
pub(crate) fn submit_span(ctx: &TraceContext, span_id: Option<[u8; 8]>, job_id: &str) -> Span {
    tracing::info_span!(
        "dist.job.submit",
        trace_id = %ctx.trace_id_hex(),
        span_id = %span_id_hex(span_id),
        parent_span_id = %ctx.parent_span_id_hex(),
        job_id = %job_id,
        stages = tracing::field::Empty,
        nodes = tracing::field::Empty,
    )
}

/// The span covering delivery of the tasks of one job to one node.
pub(crate) fn send_tasks_span(
    ctx: &TraceContext,
    span_id: Option<[u8; 8]>,
    node_id: &NodeId,
    tasks: usize,
) -> Span {
    tracing::debug_span!(
        "dist.job.send_tasks",
        trace_id = %ctx.trace_id_hex(),
        span_id = %span_id_hex(span_id),
        parent_span_id = %ctx.parent_span_id_hex(),
        node_id = %node_id,
        tasks,
    )
}

/// The span covering a node receiving the tasks of a job.
pub(crate) fn receive_tasks_span(
    ctx: &TraceContext,
    span_id: Option<[u8; 8]>,
    job_id: &str,
    node_id: &NodeId,
    tasks: usize,
) -> Span {
    tracing::debug_span!(
        "dist.job.receive_tasks",
        trace_id = %ctx.trace_id_hex(),
        span_id = %span_id_hex(span_id),
        parent_span_id = %ctx.parent_span_id_hex(),
        job_id = %job_id,
        node_id = %node_id,
        tasks,
    )
}

/// The span covering the execution of one task on one node, whether the node is
/// the local one or one the task is fetched from.
pub(crate) fn task_span(
    ctx: &TraceContext,
    span_id: Option<[u8; 8]>,
    task_id: &TaskId,
    node_id: &NodeId,
) -> Span {
    tracing::debug_span!(
        "dist.task.execute",
        trace_id = %ctx.trace_id_hex(),
        span_id = %span_id_hex(span_id),
        parent_span_id = %ctx.parent_span_id_hex(),
        job_id = %task_id.job_id,
        stage = task_id.stage,
        partition = task_id.partition,
        node_id = %node_id,
    )
}

/// The span covering the reporting of one job's completion, so that the line
/// that says a job ended names the job and the trace even though the batch it
/// was checked in held many.
pub(crate) fn job_completed_span(
    ctx: &TraceContext,
    span_id: Option<[u8; 8]>,
    job_id: &str,
) -> Span {
    tracing::info_span!(
        "dist.job.completed",
        trace_id = %ctx.trace_id_hex(),
        span_id = %span_id_hex(span_id),
        parent_span_id = %ctx.parent_span_id_hex(),
        job_id = %job_id,
    )
}

/// The span covering the cleanup of one job on the nodes that ran it.
pub(crate) fn job_cleanup_span(ctx: &TraceContext, span_id: Option<[u8; 8]>, job_id: &str) -> Span {
    tracing::info_span!(
        "dist.job.cleanup",
        trace_id = %ctx.trace_id_hex(),
        span_id = %span_id_hex(span_id),
        parent_span_id = %ctx.parent_span_id_hex(),
        job_id = %job_id,
    )
}

/// The span covering the cleanup of a batch of jobs whose trace the caller has
/// no context for, e.g. an admin RPC naming job ids.
pub(crate) fn cleanup_batch_span(jobs: usize) -> Span {
    tracing::info_span!("dist.job.cleanup", jobs)
}

/// The span covering the periodic check for jobs that completed everywhere.
pub(crate) fn check_completed_span(jobs: usize) -> Span {
    tracing::info_span!("dist.job.check_completed", jobs)
}

/// The span covering the wait for a job's stage0 tasks to be polled, after
/// which the job is given up on.
pub(crate) fn stage0_poll_span(jobs: usize) -> Span {
    tracing::info_span!("dist.stage0.poll", jobs)
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

    use arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field as ArrowField, Schema, SchemaRef},
    };
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

    use super::{TRACEPARENT_HEADER, TraceContext};
    use crate::{
        DistError, DistResult, JobId,
        cluster::{DistCluster, NodeId, NodeState},
        config::DistConfig,
        executor::DistExecutor,
        network::{DistNetwork, ScheduledTasks, StageInfo},
        planner::{StageId, TaskId},
        runtime::DistRuntime,
    };

    const SAMPLE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    // ── the context itself ────────────────────────────────────────────────────

    #[test]
    fn a_well_formed_header_is_parsed() {
        let ctx = TraceContext::from_traceparent(SAMPLE).expect("valid");
        assert_eq!(ctx.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.parent_span_id_hex(), "00f067aa0ba902b7");
        assert!(ctx.is_sampled());
        // The header a child of this context sends is the same header: nothing
        // was invented on the way.
        assert_eq!(ctx.traceparent().as_deref(), Some(SAMPLE));
    }

    #[test]
    fn a_child_keeps_the_trace_and_moves_the_span() {
        let parent = TraceContext::from_traceparent(SAMPLE).expect("valid");
        let (child, span_id) = parent.child();
        let span_id = span_id.expect("a traced context has a span");
        assert_eq!(child.trace_id_hex(), parent.trace_id_hex());
        assert_eq!(child.parent_span_id_hex(), super::hex(span_id));
        assert_ne!(child.parent_span_id_hex(), parent.parent_span_id_hex());
        assert_eq!(
            child.traceparent(),
            Some(format!(
                "00-{}-{}-01",
                parent.trace_id_hex(),
                super::hex(span_id)
            ))
        );
    }

    #[test]
    fn a_malformed_header_is_rejected() {
        for header in [
            "",
            "00",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902bg-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ] {
            assert!(TraceContext::parse(header).is_none(), "{header}");
            assert!(TraceContext::from_traceparent(header).is_err(), "{header}");
        }
    }

    #[test]
    fn an_untraced_context_logs_empty_ids_and_sends_no_header() {
        let ctx = TraceContext::none();
        assert!(ctx.is_none());
        assert_eq!(ctx.trace_id_hex(), "");
        assert_eq!(ctx.parent_span_id_hex(), "");
        assert_eq!(ctx.traceparent(), None);
        let (child, span_id) = ctx.child();
        assert!(child.is_none());
        assert!(span_id.is_none());
    }

    // ── the context across the hop ────────────────────────────────────────────

    /// Runs the tasks of a stage and logs from inside the poll, which is where a
    /// plan reads its batches and where dist's own executor lines are written.
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
                Poll::Ready(None::<Result<RecordBatch, DataFusionError>>)
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
        ) -> DistResult<HashMap<StageId, StageInfo>> {
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

    /// A job the caller traced: the context the caller hands to `submit` becomes
    /// the parent of the job's spans, the ids stay the same on the node that
    /// runs the tasks, and a line written while reading batches belongs to the
    /// job.
    #[tokio::test]
    async fn a_traced_job_keeps_its_trace_where_the_tasks_run() {
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

        let schema: SchemaRef = Arc::new(Schema::new(vec![ArrowField::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ProbeExec {
            schema: schema.clone(),
            properties: EmptyExec::new(schema).properties().clone(),
        });

        let job_id: JobId = "test-job".into();
        // The job meta is the caller's business data: it holds a `req_id` this
        // library knows nothing about and must not look for.
        let job_meta = Arc::new(HashMap::from([("req_id".to_string(), "req-1".to_string())]));

        let caller = TraceContext::from_traceparent(SAMPLE).expect("valid");
        let mut stage0 = runtime
            .submit(job_id.clone(), plan, job_meta)
            .with_trace_context(caller)
            .await
            .expect("submit");

        let (task_id, _) = stage0.drain().next().expect("one stage0 task");
        let mut stream = runtime
            .execute_local(task_id.clone())
            .await
            .expect("execute local");
        while stream.next().await.is_some() {}
        drop(stream);

        let spans = recorder.spans.lock().clone();
        let span = |name: &str| {
            spans
                .iter()
                .find(|span| span.name == name)
                .unwrap_or_else(|| panic!("{name} span"))
        };

        let submit = span("dist.job.submit");
        assert_eq!(
            submit.field("trace_id"),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        assert_eq!(submit.field("parent_span_id"), Some("00f067aa0ba902b7"));
        assert_eq!(submit.field("job_id"), Some("test-job"));
        let submit_span_id = submit.field("span_id").expect("a traced span has an id");
        assert_eq!(submit_span_id.len(), 16);

        let receive = span("dist.job.receive_tasks");
        assert_eq!(receive.field("trace_id"), submit.field("trace_id"));
        assert_eq!(receive.field("parent_span_id"), Some(submit_span_id));
        assert_eq!(receive.field("tasks"), Some("1"));
        assert_eq!(receive.field("req_id"), None);
        let receive_span_id = receive.field("span_id").expect("a traced span has an id");

        let task = span("dist.task.execute");
        assert_eq!(task.field("trace_id"), submit.field("trace_id"));
        assert_eq!(task.field("parent_span_id"), Some(receive_span_id));
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

    /// The metadata header the transport sends carries the trace, so a node that
    /// receives it derives its own span id instead of inventing a new trace.
    #[test]
    fn the_header_of_a_hop_round_trips_through_the_metadata() {
        let ctx = TraceContext::from_traceparent(SAMPLE).expect("valid");
        let (hop, _) = ctx.child();
        let header = hop.traceparent().expect("a traced hop has a header");

        let received = TraceContext::from_traceparent(&header).expect("round trip");
        assert_eq!(received.trace_id_hex(), ctx.trace_id_hex());
        assert_eq!(
            received.parent_span_id_hex(),
            hop.parent_span_id_hex(),
            "the hopping span becomes the parent of the receiver's span"
        );
        assert_eq!(TRACEPARENT_HEADER, "traceparent");
    }
}
