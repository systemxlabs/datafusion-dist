//! The trace of a job, and the spans that name the work a node does for it.
//!
//! The library's own lines are `log` records. A host that bridges `log` into
//! `tracing` — as datafusion's own dist binary does — only gets them
//! attributed to a request when there is a span to attribute them to, and a
//! distributed query crosses processes, so the span has to carry ids that the
//! other processes already know. Those ids travel the way the rest of the
//! world sends them: a W3C `traceparent`, in the metadata of the call that
//! starts the work.
//!
//! The tracing itself is [fastrace](https://crates.io/crates/fastrace). A hop
//! of a job opens two spans, and the pair is what this module calls a [`Hop`]:
//!
//! * a fastrace `Span` — the real trace. fastrace owns the span ids, the
//!   sampling flag, and the W3C codec, and a host that installs a reporter
//!   (jaeger, otel, …) gets the whole
//!   `submit -> send_tasks -> receive_tasks -> execute_task` tree, across
//!   however many processes the job touches, exported rather than only logged.
//! * a mirror `tracing` span recording the same ids as fields. `tracing` does
//!   not let a span carry ids its subscriber did not invent, and a `log` ->
//!   `tracing` bridge attributes a line by the fields of the span that is
//!   current: the mirror is what keeps every `log` line a node writes about a
//!   job greppable by `trace_id`.
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
//!   span id changes at every hop, so each node opens a new fastrace root from
//!   the context it was handed and passes the context of its own span on. That
//!   is what makes the tree `submit -> send_tasks -> receive_tasks ->
//!   execute_task` visible in the log lines of three processes.
//!
//! Spans for single tasks are `DEBUG`, the level this crate logs at, because a
//! job can have thousands of them; a host that wants the per-task view is
//! asking for `datafusion_dist` at debug level anyway. Job-level spans are
//! `INFO`, one per job. fastrace itself has no levels: whether spans are
//! collected at all is the reporter's `Config`.
//!
//! [`DistRuntime::submit`]: crate::runtime::DistRuntime::submit
//! [`DistRuntime::execute_local`]: crate::runtime::DistRuntime::execute_local

use std::fmt;

use fastrace::{Span, collector::SpanContext};

use crate::{DistError, DistResult, cluster::NodeId, planner::TaskId};

/// The header a call carries its trace in, as the W3C Trace Context
/// recommendation spells it.
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// The trace a span belongs to: the trace id and the span it is a child of —
/// that is, the four fields of a `traceparent`, minus the span id of the hop
/// that is about to open a span.
///
/// A node receives one of these, opens a span as its child, and hands the
/// context of its own span on. The library never invents a trace id: a job is
/// traced because the caller asked for it.
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
pub struct TraceContext(Option<SpanContext>);

impl TraceContext {
    /// A job nobody asked to trace. Spans opened from it carry empty ids rather
    /// than a trace of their own, so that "not traced" stays distinguishable
    /// from "traced, but this node lost the context".
    pub fn none() -> Self {
        Self::default()
    }

    /// Whether this context carries a trace at all.
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }

    /// Parse a `traceparent` header value.
    ///
    /// Returns `None` for everything malformed: a missing field, an id that is
    /// not hex or does not fit the id's width, an all-zero id, or a version
    /// other than `00`. A header that does not parse is the caller's bug, but
    /// it is not worth failing a query over: a transport that reads one off a
    /// header falls back to [`TraceContext::none`].
    pub fn parse(header: &str) -> Option<Self> {
        SpanContext::decode_w3c_traceparent(header).map(|ctx| Self(Some(ctx)))
    }

    /// As [`TraceContext::parse`], for a caller that would rather report the
    /// malformed header than run untraced.
    pub fn from_traceparent(header: &str) -> DistResult<Self> {
        Self::parse(header)
            .ok_or_else(|| DistError::internal(format!("Invalid {TRACEPARENT_HEADER}: {header}")))
    }

    /// Whether the trace is sampled. An untraced context reads as not sampled.
    pub fn is_sampled(&self) -> bool {
        self.0.is_some_and(|ctx| ctx.sampled)
    }

    /// The trace id as it is logged: the empty string when there is no trace, so
    /// that one query always has the same set of fields.
    pub fn trace_id_hex(&self) -> String {
        self.0
            .map(|ctx| ctx.trace_id.to_string())
            .unwrap_or_default()
    }

    /// The span this context is a child of, as it is logged.
    pub fn parent_span_id_hex(&self) -> String {
        self.0
            .map(|ctx| ctx.span_id.to_string())
            .unwrap_or_default()
    }

    /// The header to send with a call that continues this trace: the same trace
    /// and flags, and the span id this context refers to.
    pub fn traceparent(&self) -> Option<String> {
        self.0.map(|ctx| ctx.encode_w3c_traceparent())
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

impl TryFrom<&str> for TraceContext {
    type Error = DistError;

    fn try_from(header: &str) -> DistResult<Self> {
        Self::from_traceparent(header)
    }
}

/// One hop of a job: a fastrace span for trace collection, and its `tracing`
/// mirror carrying the same ids as fields for the `log` bridge.
///
/// A hop of a traced job is a new fastrace root: its parent is the context
/// that arrived with the call, linked by trace id rather than by a collect
/// token, so the hop reports whenever its span drops no matter which of the
/// job's other hops are still open on this or another node.
pub(crate) struct Hop {
    /// The fastrace span. `Span::noop()` when the job is not traced, so an
    /// untraced job never grows a trace of its own.
    pub(crate) fr: Span,
    /// The mirror span, recording `trace_id`, `span_id`, and
    /// `parent_span_id` as fields — empty when the job is not traced. This is
    /// the span a `log` -> `tracing` bridge attributes lines to.
    pub(crate) tr: tracing::Span,
}

impl Hop {
    /// The context to hand to the children of this hop: the same trace, with
    /// this hop's span as the parent. [`TraceContext::none`] for an untraced
    /// job, or a build without fastrace's `enable` feature.
    pub(crate) fn child(&self) -> TraceContext {
        TraceContext(SpanContext::from_span(&self.fr))
    }

    /// Enters both spans on the current thread until the guard drops. For
    /// synchronous scopes; a future or stream takes the spans by field
    /// instead, e.g. `future.in_span(hop.fr).instrument(hop.tr)`, so that
    /// `tokio::spawn` and whoever polls the stream cannot strand the context.
    pub(crate) fn enter(&self) -> HopGuard {
        HopGuard {
            _fr: self.fr.set_local_parent(),
            _tr: self.tr.clone().entered(),
        }
    }
}

/// Both spans of a hop, entered on the current thread.
pub(crate) struct HopGuard {
    _fr: fastrace::local::LocalParentGuard,
    _tr: tracing::span::EnteredSpan,
}

/// Opens the fastrace span of a hop and the context this hop hands on.
///
/// The span is a fastrace root parented to `parent`: fastrace assigns the span
/// id, and [`SpanContext::from_span`] reads it back as the parent of the next
/// hop. An untraced job opens no span at all.
fn open(name: &'static str, parent: &TraceContext) -> (Span, TraceContext) {
    let fr = match parent.0 {
        Some(ctx) => Span::root(name, ctx),
        None => Span::noop(),
    };
    // The context this hop hands on: the same trace, parented at this span.
    let child = TraceContext(SpanContext::from_span(&fr));
    (fr, child)
}

/// The ids of one hop as the mirror span records them: this hop's trace and
/// span id, and the incoming span id as the parent. Empty strings when the job
/// is not traced, so one query always has the same set of fields.
fn ids(parent: &TraceContext, child: &TraceContext) -> [String; 3] {
    match (parent.0, child.0) {
        (Some(parent), Some(child)) => [
            child.trace_id.to_string(),
            child.span_id.to_string(),
            parent.span_id.to_string(),
        ],
        _ => [String::new(), String::new(), String::new()],
    }
}

/// The span covering [`DistRuntime::submit`]: the job as the submitting node
/// sees it. `stages` and `nodes` are recorded once the plan has been staged and
/// the tasks distributed. `link`, when there is one, is the local span the
/// submission was asked for — the caller's fastrace tree, which only has one
/// parent per span, cannot also be the parent of this one.
///
/// [`DistRuntime::submit`]: crate::runtime::DistRuntime::submit
pub(crate) fn submit_hop(parent: &TraceContext, job_id: &str, link: Option<SpanContext>) -> Hop {
    let (fr, child) = open("dist.job.submit", parent);
    let fr = match link {
        Some(link) => fr.with_link(link),
        None => fr,
    };
    let fr = fr.with_property(|| ("job_id", job_id.to_string()));
    let [trace_id, span_id, parent_span_id] = ids(parent, &child);
    let tr = tracing::info_span!(
        "dist.job.submit",
        trace_id = %trace_id,
        span_id = %span_id,
        parent_span_id = %parent_span_id,
        job_id = %job_id,
        stages = tracing::field::Empty,
        nodes = tracing::field::Empty,
    );
    Hop { fr, tr }
}

/// The hop covering delivery of the tasks of one job to one node.
pub(crate) fn send_tasks_hop(parent: &TraceContext, node_id: &NodeId, tasks: usize) -> Hop {
    let (fr, child) = open("dist.job.send_tasks", parent);
    let node_id = node_id.to_string();
    let fr = fr.with_properties(|| [("node_id", node_id.clone()), ("tasks", tasks.to_string())]);
    let [trace_id, span_id, parent_span_id] = ids(parent, &child);
    let tr = tracing::debug_span!(
        "dist.job.send_tasks",
        trace_id = %trace_id,
        span_id = %span_id,
        parent_span_id = %parent_span_id,
        node_id = %node_id,
        tasks,
    );
    Hop { fr, tr }
}

/// The hop covering a node receiving the tasks of a job.
pub(crate) fn receive_tasks_hop(
    parent: &TraceContext,
    job_id: &str,
    node_id: &NodeId,
    tasks: usize,
) -> Hop {
    let (fr, child) = open("dist.job.receive_tasks", parent);
    let node_id = node_id.to_string();
    let fr = fr.with_properties(|| {
        [
            ("job_id", job_id.to_string()),
            ("node_id", node_id.clone()),
            ("tasks", tasks.to_string()),
        ]
    });
    let [trace_id, span_id, parent_span_id] = ids(parent, &child);
    let tr = tracing::debug_span!(
        "dist.job.receive_tasks",
        trace_id = %trace_id,
        span_id = %span_id,
        parent_span_id = %parent_span_id,
        job_id = %job_id,
        node_id = %node_id,
        tasks,
    );
    Hop { fr, tr }
}

/// The hop covering the execution of one task on one node, whether the node is
/// the local one or one the task is fetched from. `link`, when there is one,
/// is the local span that asked for the task — a second parent a single
/// hierarchy cannot express.
pub(crate) fn task_hop(
    parent: &TraceContext,
    task_id: &TaskId,
    node_id: &NodeId,
    link: Option<SpanContext>,
) -> Hop {
    let (fr, child) = open("dist.task.execute", parent);
    let fr = match link {
        Some(link) => fr.with_link(link),
        None => fr,
    };
    let node_id = node_id.to_string();
    let job_id = task_id.job_id.to_string();
    let fr = fr.with_properties(|| {
        [
            ("job_id", job_id.clone()),
            ("stage", task_id.stage.to_string()),
            ("partition", task_id.partition.to_string()),
            ("node_id", node_id.clone()),
        ]
    });
    let [trace_id, span_id, parent_span_id] = ids(parent, &child);
    let tr = tracing::debug_span!(
        "dist.task.execute",
        trace_id = %trace_id,
        span_id = %span_id,
        parent_span_id = %parent_span_id,
        job_id = %job_id,
        stage = task_id.stage,
        partition = task_id.partition,
        node_id = %node_id,
    );
    Hop { fr, tr }
}

/// The hop covering the reporting of one job's completion, so that the line
/// that says a job ended names the job and the trace even though the batch it
/// was checked in held many.
pub(crate) fn job_completed_hop(parent: &TraceContext, job_id: &str) -> Hop {
    let (fr, child) = open("dist.job.completed", parent);
    let fr = fr.with_property(|| ("job_id", job_id.to_string()));
    let [trace_id, span_id, parent_span_id] = ids(parent, &child);
    let tr = tracing::info_span!(
        "dist.job.completed",
        trace_id = %trace_id,
        span_id = %span_id,
        parent_span_id = %parent_span_id,
        job_id = %job_id,
    );
    Hop { fr, tr }
}

/// The hop covering the cleanup of one job on the nodes that ran it.
pub(crate) fn job_cleanup_hop(parent: &TraceContext, job_id: &str) -> Hop {
    let (fr, child) = open("dist.job.cleanup", parent);
    let fr = fr.with_property(|| ("job_id", job_id.to_string()));
    let [trace_id, span_id, parent_span_id] = ids(parent, &child);
    let tr = tracing::info_span!(
        "dist.job.cleanup",
        trace_id = %trace_id,
        span_id = %span_id,
        parent_span_id = %parent_span_id,
        job_id = %job_id,
    );
    Hop { fr, tr }
}

/// The hop covering the cleanup of a batch of jobs whose trace the caller has
/// no context for, e.g. an admin RPC naming job ids.
pub(crate) fn cleanup_batch_hop(jobs: usize) -> Hop {
    Hop {
        fr: Span::noop(),
        tr: tracing::info_span!("dist.job.cleanup", jobs),
    }
}

/// The hop covering the periodic check for jobs that completed everywhere.
pub(crate) fn check_completed_hop(jobs: usize) -> Hop {
    Hop {
        fr: Span::noop(),
        tr: tracing::info_span!("dist.job.check_completed", jobs),
    }
}

/// The hop covering the wait for a job's stage0 tasks to be polled, after
/// which the job is given up on.
pub(crate) fn stage0_poll_hop(jobs: usize) -> Hop {
    Hop {
        fr: Span::noop(),
        tr: tracing::info_span!("dist.stage0.poll", jobs),
    }
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
    use fastrace::{
        collector::{Config, SpanId, TestReporter, TraceId},
        prelude::SpanContext,
    };
    use futures::StreamExt;
    use parking_lot::Mutex;
    use tokio::runtime::Handle;
    use tracing::{
        Event, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id, Record},
    };

    use super::{TRACEPARENT_HEADER, TraceContext, submit_hop};
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

    fn property<'a>(span: &'a fastrace::collector::SpanRecord, key: &str) -> Option<&'a str> {
        span.properties
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_ref())
    }

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
    fn a_malformed_header_is_rejected() {
        for header in [
            "",
            "00",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "00-4bf92f3577b34da6a3ce929d0e0e47364-00f067aa0ba902b7-01",
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
        assert!(!ctx.is_sampled());
        assert_eq!(ctx.trace_id_hex(), "");
        assert_eq!(ctx.parent_span_id_hex(), "");
        assert_eq!(ctx.traceparent(), None);
        // An untraced hop opens no span and hands on no context.
        let hop = submit_hop(&ctx, "job", None);
        assert!(hop.child().is_none());
    }

    #[test]
    fn a_hop_keeps_the_trace_and_moves_the_span() {
        let caller = TraceContext::from_traceparent(SAMPLE).expect("valid");
        let hop = submit_hop(&caller, "job", None);
        let child = hop.child();

        let caller = caller.0.expect("traced");
        let child = child.0.expect("a traced hop hands on a context");
        assert_eq!(child.trace_id, caller.trace_id);
        assert_ne!(child.span_id, caller.span_id);

        // The context the hop hands on names the hop's own span as the parent,
        // so the header round-trips through the metadata of the next call.
        let header = TraceContext(Some(child))
            .traceparent()
            .expect("a traced hop has a header");
        let received = TraceContext::parse(&header).expect("round trip");
        assert_eq!(received.0, Some(child));
        assert_eq!(TRACEPARENT_HEADER, "traceparent");
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

    /// A job the caller traced: the context the caller hands to `submit`
    /// becomes the parent of the job's spans, the ids stay the same on the node
    /// that runs the tasks, and a line written while reading batches belongs to
    /// the job — in the fastrace tree a host reporter exports, and in the
    /// tracing mirror a `log` -> `tracing` bridge attributes lines to.
    #[tokio::test]
    async fn a_traced_job_keeps_its_trace_where_the_tasks_run() {
        let (reporter, reported) = TestReporter::new();
        fastrace::set_reporter(reporter, Config::default());
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
        // library knows nothing about, and the `traceparent` that traces the
        // job to the request that produced it.
        let job_meta = Arc::new(HashMap::from([
            ("req_id".to_string(), "req-1".to_string()),
            (TRACEPARENT_HEADER.to_string(), SAMPLE.to_string()),
        ]));

        let mut stage0 = runtime
            .submit(job_id.clone(), plan, job_meta)
            .await
            .expect("submit");

        let (task_id, _) = stage0.drain().next().expect("one stage0 task");
        let mut stream = runtime
            .execute_local(task_id.clone())
            .await
            .expect("execute local");
        while stream.next().await.is_some() {}
        drop(stream);
        fastrace::flush();

        // ── the fastrace tree a reporter exports ──────────────────────────
        let spans = reported.lock();
        let trace_id = TraceId(0x4bf92f3577b34da6a3ce929d0e0e4736);
        let caller_span = SpanId(0x00f067aa0ba902b7);
        let span = |name: &str| {
            spans
                .iter()
                .find(|span| span.name == name)
                .unwrap_or_else(|| panic!("{name} span"))
        };

        let submit = span("dist.job.submit");
        assert_eq!(submit.trace_id, trace_id);
        // The caller's span is the parent of the job's first span.
        assert_eq!(submit.parent_id, caller_span);
        assert_eq!(property(submit, "job_id"), Some("test-job"));

        let receive = span("dist.job.receive_tasks");
        assert_eq!(receive.trace_id, trace_id);
        assert_eq!(receive.parent_id, submit.span_id);
        assert_eq!(property(receive, "tasks"), Some("1"));
        assert_eq!(property(receive, "req_id"), None);

        let task = span("dist.task.execute");
        assert_eq!(task.trace_id, trace_id);
        assert_eq!(task.parent_id, receive.span_id);
        assert_eq!(property(task, "job_id"), Some("test-job"));
        assert_eq!(property(task, "stage"), Some("0"));
        assert_eq!(property(task, "partition"), Some("0"));
        assert_eq!(property(task, "node_id"), Some("127.0.0.1:50050"));

        let driver = span("dist.task.driver");
        assert_eq!(driver.trace_id, trace_id);
        assert_eq!(driver.parent_id, task.span_id);

        // ── the tracing mirror a log bridge attributes lines to ───────────
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
}
