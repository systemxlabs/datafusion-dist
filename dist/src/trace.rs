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
//! # What a host may rely on
//!
//! Span names are part of the interface: a log sink that attributes a line to a
//! job does it by reading the fields of the spans the line was written under, so
//! a rename is a silent break of the host's queries. They are exported as the
//! constants of [`span_name`], and every span carries the same three trace
//! fields, spelled the same way:
//!
//! * `trace_id` — 32 lowercase hex digits, the same on every node and every span
//!   of one job; empty when the job was not traced.
//! * `span_id` — 16 lowercase hex digits, unique per span. Empty only when the
//!   job was not traced.
//! * `parent_span_id` — the `span_id` of the span this one is a child of; empty
//!   at the root and for untraced spans.
//!
//! The three trace fields are on a span when it is created, so a subscriber that
//! snapshots span fields at creation already has everything it needs to join a
//! line to its job. Fields that are only known later — `stages` and `nodes` of
//! `dist.job.submit`, for instance — are recorded onto the span afterwards, and
//! a subscriber needs `on_record` to see those.
//!
//! Every span carries all three, inner spans included, so a sink that prefers
//! the closest one over the outermost can pick the innermost span of a line and
//! still get a complete set of ids.
//!
//! One field goes beyond the three: `dist.task.fetch` carries `link_span_id`,
//! the span of the local task whose read caused this fetch — the edge from the
//! consumer's work to the fetch, which a parent cannot express. It is empty when
//! there was no such span, e.g. when a host called the runtime directly with no
//! task of its own.
//!
//! [`DistRuntime::submit`]: crate::runtime::DistRuntime::submit
//! [`DistRuntime::execute_local`]: crate::runtime::DistRuntime::execute_local

use std::fmt;

use tracing::Span;

use crate::{DistError, DistResult, cluster::NodeId, planner::TaskId};

/// The header a call carries its trace in, as the W3C Trace Context
/// recommendation spells it.
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// The names of the spans this crate opens, as the constants a log query and a
/// dashboard filter on.
///
/// They are here so that a rename is a compile-time change on this side instead
/// of a silent break on the host's side: datafabric's log sink hardcodes
/// `dist.job.submit`, and this list is the whole of what it may hardcode.
pub mod span_name {
    /// `DistRuntime::submit`, one per job on the node that plans it.
    pub const JOB_SUBMIT: &str = "dist.job.submit";
    /// Delivery of a job's tasks to one node. Only opened when the tasks leave
    /// the node that planned them.
    pub const JOB_SEND_TASKS: &str = "dist.job.send_tasks";
    /// A node receiving the tasks of a job.
    pub const JOB_RECEIVE_TASKS: &str = "dist.job.receive_tasks";
    /// The execution of one task on the node that runs it.
    pub const TASK_EXECUTE: &str = "dist.task.execute";
    /// The fetch of one task's output stream from another node, on the node that
    /// consumes it. Named apart from [`TASK_EXECUTE`] so that one trace does not
    /// read as if a task ran twice.
    pub const TASK_FETCH: &str = "dist.task.fetch";
    /// The reporting of one job's completion.
    pub const JOB_COMPLETED: &str = "dist.job.completed";
    /// The cleanup of one job on the nodes that ran it.
    pub const JOB_CLEANUP: &str = "dist.job.cleanup";
    /// The cleanup of a batch of jobs named by an admin RPC.
    pub const JOB_CLEANUP_BATCH: &str = "dist.job.cleanup_batch";
    /// The completion check of one job, including the checks that give up on a
    /// job whose nodes are gone.
    pub const JOB_CHECK_COMPLETED: &str = "dist.job.check_completed";
    /// The periodic completion check of a batch of jobs.
    pub const JOB_CHECK_COMPLETED_BATCH: &str = "dist.job.check_completed_batch";
    /// The wait for a job's stage0 tasks to be polled.
    pub const STAGE0_POLL: &str = "dist.stage0.poll";
}

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

    /// A context from the ids themselves, for a caller that has them as bytes.
    ///
    /// An all-zero id is the recommendation's way of saying "no id", so it
    /// reads as [`TraceContext::none`] rather than as a context that cannot be
    /// put on a header.
    pub fn from_parts(trace_id: [u8; 16], parent_span_id: [u8; 8], flags: u8) -> Self {
        if trace_id == [0u8; 16] || parent_span_id == [0u8; 8] {
            return Self::none();
        }
        Self {
            trace_id: Some(trace_id),
            parent_span_id: Some(parent_span_id),
            flags,
        }
    }

    /// The same trace, with `span_id` as the span this context points at.
    ///
    /// [`TraceContext::child`] is for a node that is about to open the span and
    /// wants the library to name it; this is for a caller that has already
    /// opened it — a host with a request span of its own — and only needs the
    /// context to point there instead of at whatever it arrived as.
    ///
    /// An untraced context stays untraced: there is no trace to attach a span
    /// to.
    pub fn with_parent_span_id(self, span_id: [u8; 8]) -> Self {
        let Some(trace_id) = self.trace_id else {
            return Self::none();
        };
        if span_id == [0u8; 8] {
            return Self::none();
        }
        Self {
            trace_id: Some(trace_id),
            parent_span_id: Some(span_id),
            flags: self.flags,
        }
    }

    /// The trace id as bytes, for a caller that feeds another tracer.
    pub fn trace_id(&self) -> Option<[u8; 16]> {
        self.trace_id
    }

    /// The id of the span this context points at, as bytes.
    pub fn parent_span_id(&self) -> Option<[u8; 8]> {
        self.parent_span_id
    }

    /// The flags byte of the `traceparent`, carried as it arrived.
    pub fn flags(&self) -> u8 {
        self.flags
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

    /// Whether the caller asked for the trace to be sampled.
    ///
    /// This is passed through, not acted on: the library opens its spans whether
    /// or not the bit is set, because the decision to skip work belongs to
    /// whoever pays for the work, not to a flag that arrived with a header. A
    /// host that wants sampling turns the spans off itself.
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
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(N * 2);
    for byte in bytes {
        // Two pushes of a byte the table already holds, because this runs once
        // per id field of every span and a formatter per byte is money.
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
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

/// One hop of a trace: the span that names it, the context its children go on
/// with, and the ids it logs.
///
/// The three belong together and are wrong apart — a span whose `span_id` is not
/// the one the context announces leaves a `parent_span_id` that no span ever
/// wrote, which is worse than no trace at all. So they are produced in one place,
/// the [`hop!`] macro, and a new span cannot be added with one of the three
/// fields a log query reads missing.
#[derive(Clone, Debug)]
pub(crate) struct Hop {
    /// What a child of this hop is built from: the same trace, with this hop's
    /// span as the parent.
    pub ctx: TraceContext,
    /// This hop, as the tracer sees it.
    pub span: Span,
    /// This hop's span id, for the places that name it from outside a span —
    /// the `traceparent` of a call, the `link_span_id` of a fetch.
    pub span_id: Option<[u8; 8]>,
}

/// Build a [`Hop`]: derive the child context, open the span, keep the id.
///
/// The three fields every span carries are written here once, which is the whole
/// point: they used to be three lines to copy into each of eight span
/// constructors, and a fourth constructor added without them compiled fine and
/// only lost a column in the host's log.
macro_rules! hop {
    ($level:expr, $name:expr, $parent:expr, $($field:tt)*) => {{
        let parent: TraceContext = $parent;
        let (ctx, span_id) = parent.child();
        Hop {
            span: tracing::span!(
                $level,
                $name,
                trace_id = %parent.trace_id_hex(),
                span_id = %crate::trace::span_id_hex(span_id),
                parent_span_id = %parent.parent_span_id_hex(),
                $($field)*
            ),
            ctx,
            span_id,
        }
    }};
}

tokio::task_local! {
    /// The hop the current tokio task is working under.
    ///
    /// A span follows neither a future into `tokio::spawn` nor a stream into
    /// whoever polls it, and dist runs every task on a runtime of its own. So a
    /// task that needs to know whose work it is doing — the driver of a stage
    /// that fetches another node's output — finds it here instead of in an
    /// argument threaded down from the submitter: this is the environment a
    /// context is, rather than the parameter it is not.
    ///
    /// Installed by [`in_hop`] where work starts (submit, receive, the driver of
    /// a task), read by [`current_hop`].
    static CURRENT_HOP: Hop;
}

/// The hop this task is working under, if one was installed.
pub(crate) fn current_hop() -> Option<Hop> {
    CURRENT_HOP.try_with(Hop::clone).ok()
}

/// Run a future as the given hop.
///
/// Everything that starts work on a runtime of its own wraps it in this: the
/// scope, unlike a span, reaches the task that is spawned.
pub(crate) fn in_hop<F>(hop: Hop, f: F) -> impl std::future::Future<Output = F::Output>
where
    F: std::future::Future,
{
    CURRENT_HOP.scope(hop, f)
}

/// The hop covering [`DistRuntime::submit`]: the job as the submitting node
/// sees it. `stages` and `nodes` are recorded once the plan has been staged and
/// the tasks distributed.
///
/// [`DistRuntime::submit`]: crate::runtime::DistRuntime::submit
pub(crate) fn submit_hop(parent: TraceContext, job_id: &str) -> Hop {
    hop!(
        tracing::Level::INFO,
        span_name::JOB_SUBMIT,
        parent,
        job_id = %job_id,
        stages = tracing::field::Empty,
        nodes = tracing::field::Empty,
    )
}

/// The hop covering delivery of the tasks of one job to one node.
pub(crate) fn send_tasks_hop(parent: TraceContext, node_id: &NodeId, tasks: usize) -> Hop {
    hop!(
        tracing::Level::DEBUG,
        span_name::JOB_SEND_TASKS,
        parent,
        node_id = %node_id,
        tasks,
    )
}

/// The hop covering a node receiving the tasks of a job.
pub(crate) fn receive_tasks_hop(
    parent: TraceContext,
    job_id: &str,
    node_id: &NodeId,
    tasks: usize,
) -> Hop {
    hop!(
        tracing::Level::DEBUG,
        span_name::JOB_RECEIVE_TASKS,
        parent,
        job_id = %job_id,
        node_id = %node_id,
        tasks,
    )
}

/// The hop covering the execution of one task on the node that runs it.
pub(crate) fn task_hop(parent: TraceContext, task_id: &TaskId, node_id: &NodeId) -> Hop {
    hop!(
        tracing::Level::DEBUG,
        span_name::TASK_EXECUTE,
        parent,
        job_id = %task_id.job_id,
        stage = task_id.stage,
        partition = task_id.partition,
        node_id = %node_id,
    )
}

/// The hop covering the fetch of a task's output from another node, on the node
/// that consumes it.
///
/// Named apart from [`span_name::TASK_EXECUTE`]: the two happen for the same
/// task, in the same trace, on different nodes, and a shared name would read as
/// one task executed twice. `link_span_id` names the local span whose read
/// caused the fetch, which is an edge a parent cannot express — empty when the
/// fetch was not caused by a span of ours.
pub(crate) fn fetch_hop(
    parent: TraceContext,
    task_id: &TaskId,
    node_id: &NodeId,
    link_span_id: Option<[u8; 8]>,
) -> Hop {
    hop!(
        tracing::Level::DEBUG,
        span_name::TASK_FETCH,
        parent,
        job_id = %task_id.job_id,
        stage = task_id.stage,
        partition = task_id.partition,
        node_id = %node_id,
        link_span_id = %span_id_hex(link_span_id),
    )
}

/// The hop covering the reporting of one job's completion, so that the line
/// that says a job ended names the job and the trace even though the batch it
/// was checked in held many.
pub(crate) fn job_completed_hop(parent: TraceContext, job_id: &str) -> Hop {
    hop!(
        tracing::Level::INFO,
        span_name::JOB_COMPLETED,
        parent,
        job_id = %job_id,
    )
}

/// The hop covering the cleanup of one job on the nodes that ran it.
pub(crate) fn job_cleanup_hop(parent: TraceContext, job_id: &str) -> Hop {
    hop!(
        tracing::Level::INFO,
        span_name::JOB_CLEANUP,
        parent,
        job_id = %job_id,
    )
}

/// The hop covering the check that gave up on one job -- its nodes are gone, or
/// its task distribution was never seen.
pub(crate) fn job_check_hop(parent: TraceContext, job_id: &str) -> Hop {
    hop!(
        tracing::Level::INFO,
        span_name::JOB_CHECK_COMPLETED,
        parent,
        job_id = %job_id,
    )
}

/// The span covering the cleanup of a batch of jobs whose trace the caller has
/// no context for, e.g. an admin RPC naming job ids.
pub(crate) fn cleanup_batch_span(jobs: usize) -> Span {
    tracing::info_span!(span_name::JOB_CLEANUP_BATCH, jobs)
}

/// The span covering the periodic check for jobs that completed everywhere.
pub(crate) fn check_completed_span(jobs: usize) -> Span {
    tracing::info_span!(span_name::JOB_CHECK_COMPLETED_BATCH, jobs)
}

/// The span covering the wait for a job's stage0 tasks to be polled, after
/// which the job is given up on.
pub(crate) fn stage0_poll_span(jobs: usize) -> Span {
    tracing::info_span!(span_name::STAGE0_POLL, jobs)
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

    use super::{TRACEPARENT_HEADER, TraceContext, hex};
    use crate::{
        DistError, DistResult, JobId,
        cluster::{DistCluster, NodeId, NodeState},
        config::DistConfig,
        executor::DistExecutor,
        network::{DistNetwork, ScheduledTasks, StageInfo},
        planner::{StageId, TaskId},
        runtime::{DistRuntime, Submit},
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

    #[test]
    fn a_context_from_ids_round_trips_and_rejects_zero() {
        let trace_id = [7u8; 16];
        let parent = [9u8; 8];
        let ctx = TraceContext::from_parts(trace_id, parent, 0x01);
        assert!(!ctx.is_none());
        assert_eq!(ctx.trace_id(), Some(trace_id));
        assert_eq!(ctx.parent_span_id(), Some(parent));
        assert_eq!(ctx.flags(), 0x01);
        assert!(ctx.is_sampled());
        assert_eq!(ctx.trace_id_hex(), hex(trace_id));
        assert_eq!(
            ctx.traceparent(),
            Some(format!("00-{}-{}-01", hex(trace_id), hex(parent)))
        );

        // An all-zero id is the recommendation's "no id".
        assert!(TraceContext::from_parts([0u8; 16], parent, 0x01).is_none());
        assert!(TraceContext::from_parts(trace_id, [0u8; 8], 0x01).is_none());
    }

    #[test]
    fn a_context_can_point_at_a_span_the_caller_opened() {
        let caller = TraceContext::from_traceparent(SAMPLE).expect("valid");
        let mine = [3u8; 8];
        let ctx = caller.with_parent_span_id(mine);
        assert_eq!(ctx.trace_id(), caller.trace_id());
        assert_eq!(ctx.parent_span_id(), Some(mine));
        assert_eq!(ctx.flags(), caller.flags());
        assert_eq!(
            ctx.traceparent(),
            Some(format!("00-{}-{}-01", caller.trace_id_hex(), hex(mine)))
        );

        // Attaching a span to no trace stays no trace, and a zero span is no id.
        assert!(TraceContext::none().with_parent_span_id(mine).is_none());
        assert!(caller.with_parent_span_id([0u8; 8]).is_none());
    }

    #[test]
    fn an_untraced_context_has_no_bytes_and_no_flags() {
        let ctx = TraceContext::none();
        assert_eq!(ctx.trace_id(), None);
        assert_eq!(ctx.parent_span_id(), None);
        assert_eq!(ctx.flags(), 0);
        assert!(!ctx.is_sampled());
    }

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

    /// A node that is not this one, and a fetch that records what it announced
    /// instead of opening a connection.
    #[derive(Debug)]
    struct RecordingNetwork {
        node: NodeId,
        /// The context of the last `execute_task_with_trace`, i.e. what a peer
        /// would read out of the metadata.
        announced: Mutex<Option<TraceContext>>,
    }

    #[async_trait::async_trait]
    impl DistNetwork for RecordingNetwork {
        fn local_node(&self) -> NodeId {
            self.node.clone()
        }

        async fn send_tasks(&self, _node_id: NodeId, _tasks: ScheduledTasks) -> DistResult<()> {
            Err(DistError::internal("test"))
        }

        async fn execute_task(
            &self,
            _node_id: NodeId,
            _task_id: TaskId,
        ) -> DistResult<SendableRecordBatchStream> {
            Err(DistError::internal("test uses the traced entry point"))
        }

        async fn execute_task_with_trace(
            &self,
            _node_id: NodeId,
            _task_id: TaskId,
            trace: TraceContext,
        ) -> DistResult<SendableRecordBatchStream> {
            *self.announced.lock() = Some(trace);
            Err(DistError::internal("test does not open a connection"))
        }

        async fn get_jobs(
            &self,
            _node_id: NodeId,
            _job_ids: Option<Vec<crate::JobId>>,
        ) -> DistResult<HashMap<StageId, StageInfo>> {
            Err(DistError::internal("test"))
        }

        async fn cleanup_jobs(
            &self,
            _node_id: NodeId,
            _job_ids: Vec<crate::JobId>,
        ) -> DistResult<()> {
            Err(DistError::internal("test"))
        }
    }

    /// A plan that does what `ProxyExec` does for a partition of another node:
    /// asks the runtime for the remote task's stream, from inside the driver
    /// task of the stage that consumes it.
    #[derive(Debug)]
    struct FetchExec {
        schema: SchemaRef,
        properties: Arc<PlanProperties>,
        runtime: DistRuntime,
        remote: NodeId,
        remote_task: TaskId,
    }

    impl DisplayAs for FetchExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "FetchExec")
        }
    }

    impl ExecutionPlan for FetchExec {
        fn name(&self) -> &str {
            "FetchExec"
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
            let runtime = self.runtime.clone();
            let remote = self.remote.clone();
            let remote_task = self.remote_task.clone();
            let schema = self.schema.clone();
            let stream = futures::stream::once(async move {
                runtime
                    .execute_remote(remote, remote_task)
                    .await
                    .map(|_| RecordBatch::new_empty(schema))
                    .map_err(DataFusionError::from)
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream,
            )))
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

    /// The trace of a job is readable from the runtime afterwards, which is what
    /// a host needs to keep its own context in step with the job's.
    #[tokio::test]
    async fn the_runtime_hands_the_trace_back() {
        let recorder = Arc::new(Recorder::default());
        let _guard = tracing::subscriber::set_default(Recording(recorder));

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
        let caller = TraceContext::from_traceparent(SAMPLE).expect("valid");
        let stage0 = runtime
            .submit(job_id.clone(), plan, Arc::new(HashMap::new()))
            .with_trace_context(caller)
            .await
            .expect("submit");

        let (task_id, _) = stage0.iter().next().expect("one stage0 task");
        let trace = runtime.trace_of(task_id).expect("the task's stage is here");
        assert_eq!(trace.trace_id(), caller.trace_id());
        // The stage continues the span this node opened when the tasks arrived,
        // not the caller's: the hop across nodes is one more hop.
        assert!(trace.parent_span_id().is_some());
        assert_ne!(trace.parent_span_id(), caller.parent_span_id());

        let job_trace = runtime.job_trace_of(&job_id).expect("the job is here");
        assert_eq!(job_trace.trace_id(), caller.trace_id());

        // A job this node never saw has no trace, rather than an empty one.
        assert!(runtime.job_trace_of(&"elsewhere".into()).is_none());
        let elsewhere = TaskId {
            job_id: "elsewhere".into(),
            stage: 0,
            partition: 0,
        };
        assert!(runtime.trace_of(&elsewhere).is_none());
    }

    /// A caller that read an unusable header off the wire ends up with an empty
    /// context, and an empty context must not erase the trace the job already
    /// has: the task continues the job's trace, one parent short.
    #[tokio::test]
    async fn an_empty_parent_falls_back_to_the_jobs_trace() {
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

        let caller = TraceContext::from_traceparent(SAMPLE).expect("valid");
        let stage0 = runtime
            .submit("test-job", plan, Arc::new(HashMap::new()))
            .with_trace_context(caller)
            .await
            .expect("submit");
        let (task_id, _) = stage0.iter().next().expect("one stage0 task");

        // What the transport hands over when the header was unreadable.
        let mut stream = runtime
            .execute_local_with_parent(task_id.clone(), Some(TraceContext::none()))
            .await
            .expect("execute local");
        while stream.next().await.is_some() {}
        drop(stream);

        let spans = recorder.spans.lock().clone();
        let task = spans
            .iter()
            .find(|span| span.name == super::span_name::TASK_EXECUTE)
            .expect("the task ran");
        assert_eq!(
            task.field("trace_id"),
            Some("4bf92f3577b34da6a3ce929d0e0e4736"),
            "an empty parent must not blank the job's trace"
        );
        assert_ne!(task.field("span_id"), Some(""));
        assert_ne!(task.field("parent_span_id"), Some(""));
    }

    // ── the contract with a log sink ──────────────────────────────────────────

    /// The name and the three id fields are what a sink joins a line by, and a
    /// span built without one of them compiles fine and only loses a column. So
    /// every hop constructor is built here, traced and untraced, and read back
    /// out of a subscriber.
    #[test]
    fn every_hop_names_itself_and_carries_the_ids() {
        let recorder = Arc::new(Recorder::default());
        let _guard = tracing::subscriber::set_default(Recording(recorder.clone()));

        let node = NodeId {
            host: "127.0.0.1".to_string(),
            port: 50050,
        };
        let task_id = TaskId {
            job_id: "contract".into(),
            stage: 1,
            partition: 2,
        };
        let traced = TraceContext::from_traceparent(SAMPLE).expect("valid");

        let mut hops = Vec::new();
        for parent in [traced, TraceContext::none()] {
            hops.push((
                super::span_name::JOB_SUBMIT,
                super::submit_hop(parent, "job"),
            ));
            hops.push((
                super::span_name::JOB_SEND_TASKS,
                super::send_tasks_hop(parent, &node, 3),
            ));
            hops.push((
                super::span_name::JOB_RECEIVE_TASKS,
                super::receive_tasks_hop(parent, "job", &node, 3),
            ));
            hops.push((
                super::span_name::TASK_EXECUTE,
                super::task_hop(parent, &task_id, &node),
            ));
            hops.push((
                super::span_name::TASK_FETCH,
                super::fetch_hop(parent, &task_id, &node, None),
            ));
            hops.push((
                super::span_name::JOB_COMPLETED,
                super::job_completed_hop(parent, "job"),
            ));
            hops.push((
                super::span_name::JOB_CLEANUP,
                super::job_cleanup_hop(parent, "job"),
            ));
            hops.push((
                super::span_name::JOB_CHECK_COMPLETED,
                super::job_check_hop(parent, "job"),
            ));
        }

        for (name, hop) in &hops {
            let id = hop.span.id().expect("the recorder saw the span").into_u64();
            let record = {
                let spans = recorder.spans.lock();
                spans
                    .iter()
                    .find(|span| span.id == id)
                    .cloned()
                    .expect("recorded")
            };
            assert_eq!(&record.name, name, "a span is renamed");
            for field in ["trace_id", "span_id", "parent_span_id"] {
                assert!(
                    record.field(field).is_some(),
                    "{name} does not carry {field}"
                );
            }
            // The ids in the fields are the ids in the context: a hop whose
            // span says one thing and whose children continue another leaves a
            // parent that no span ever wrote.
            assert_eq!(
                record.field("trace_id"),
                Some(hop.ctx.trace_id_hex().as_str())
            );
            assert_eq!(
                record.field("span_id"),
                Some(super::span_id_hex(hop.span_id).as_str())
            );
            assert_eq!(hop.ctx.parent_span_id(), hop.span_id);
        }

        // The batch spans name a unit of the event loop rather than a hop, so
        // they carry no ids — but a host renders them as a scope, so they stay.
        for (name, span) in [
            (
                super::span_name::JOB_CLEANUP_BATCH,
                super::cleanup_batch_span(2),
            ),
            (
                super::span_name::JOB_CHECK_COMPLETED_BATCH,
                super::check_completed_span(2),
            ),
            (super::span_name::STAGE0_POLL, super::stage0_poll_span(2)),
        ] {
            let id = span.id().expect("the recorder saw the span").into_u64();
            let recorded = {
                let spans = recorder.spans.lock();
                spans.iter().find(|s| s.id == id).map(|s| s.name.clone())
            };
            assert_eq!(recorded.as_deref(), Some(name));
        }
    }

    // ── the hop a task works under ────────────────────────────────────────────

    /// One node, one traced job, and the plan of the local stage asks for a
    /// partition of another node — which is what `ProxyExec` does for the stage
    /// it consumes. The fetch must name the task that caused it, and the call it
    /// makes must name the fetch: that is the chain a trace cannot express as a
    /// parent.
    #[tokio::test]
    async fn a_fetch_names_the_task_that_caused_it() {
        let recorder = Arc::new(Recorder::default());
        let _guard = tracing::subscriber::set_default(Recording(recorder.clone()));

        let node = NodeId {
            host: "127.0.0.1".to_string(),
            port: 50050,
        };
        let remote = NodeId {
            host: "127.0.0.1".to_string(),
            port: 50051,
        };
        let network = Arc::new(RecordingNetwork {
            node: node.clone(),
            announced: Mutex::new(None),
        });
        let ctx = SessionContext::new();
        let runtime = DistRuntime::new(
            ctx.task_ctx(),
            Arc::new(DistConfig::default()),
            Arc::new(SingleNodeCluster { node: node.clone() }),
            network.clone(),
        )
        .with_executor(Arc::new(CurrentRuntime {
            handle: Handle::current(),
        }));

        let remote_task = TaskId {
            job_id: "test-job".into(),
            stage: 1,
            partition: 0,
        };
        let schema: SchemaRef = Arc::new(Schema::new(vec![ArrowField::new(
            "a",
            DataType::Int32,
            false,
        )]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(FetchExec {
            schema: schema.clone(),
            properties: EmptyExec::new(schema).properties().clone(),
            runtime: runtime.clone(),
            remote: remote.clone(),
            remote_task,
        });

        let caller = TraceContext::from_traceparent(SAMPLE).expect("valid");
        let stage0 = runtime
            .submit("test-job", plan, Arc::new(HashMap::new()))
            .with_trace_context(caller)
            .await
            .expect("submit");
        let (task_id, _) = stage0.iter().next().expect("one stage0 task");

        // Draining the stream runs the plan, which fetches.
        let mut stream = runtime
            .execute_local(task_id.clone())
            .await
            .expect("execute local");
        while let Some(_batch) = stream.next().await {}
        drop(stream);

        let spans = recorder.spans.lock().clone();
        let by_name = |name: &str| {
            spans
                .iter()
                .find(|span| span.name == name)
                .unwrap_or_else(|| panic!("no {name} span"))
                .clone()
        };
        let local_task = by_name(super::span_name::TASK_EXECUTE);
        let fetch = by_name(super::span_name::TASK_FETCH);

        assert_eq!(local_task.field("job_id"), Some("test-job"));
        assert_eq!(fetch.field("job_id"), Some("test-job"));
        // The fetch is a child of the task that caused it, not of the job root.
        assert_eq!(
            fetch.field("parent_span_id"),
            local_task.field("span_id"),
            "the fetch must continue the task that asked for it"
        );
        // And it says so in a field of its own, which a tracer backend would
        // otherwise be the only one to see.
        assert_eq!(fetch.field("link_span_id"), local_task.field("span_id"));

        // The call the fetch makes announces the fetch itself, so the execution
        // on the other node is its child.
        let announced = network.announced.lock().expect("the fetch called out");
        assert_eq!(announced.trace_id(), caller.trace_id());
        assert_eq!(
            announced.parent_span_id_hex(),
            fetch.field("span_id").expect("the fetch logged its id")
        );
    }

    /// The same fetch with nobody having installed a scope — a host calling the
    /// runtime directly — still reports the job's trace, taken from the stages.
    #[tokio::test]
    async fn a_fetch_without_a_scope_falls_back_to_the_jobs_trace() {
        let recorder = Arc::new(Recorder::default());
        let _guard = tracing::subscriber::set_default(Recording(recorder.clone()));

        let node = NodeId {
            host: "127.0.0.1".to_string(),
            port: 50050,
        };
        let remote = NodeId {
            host: "127.0.0.1".to_string(),
            port: 50051,
        };
        let network = Arc::new(RecordingNetwork {
            node: node.clone(),
            announced: Mutex::new(None),
        });
        let ctx = SessionContext::new();
        let runtime = DistRuntime::new(
            ctx.task_ctx(),
            Arc::new(DistConfig::default()),
            Arc::new(SingleNodeCluster { node }),
            network.clone(),
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
        let caller = TraceContext::from_traceparent(SAMPLE).expect("valid");
        runtime
            .submit("test-job", plan, Arc::new(HashMap::new()))
            .with_trace_context(caller)
            .await
            .expect("submit");

        let remote_task = TaskId {
            job_id: "test-job".into(),
            stage: 1,
            partition: 0,
        };
        // Not awaited for its stream: the call to the peer fails in this test,
        // and what is being read is what it was told before it failed.
        let _ = runtime.execute_remote(remote, remote_task).await;

        let spans = recorder.spans.lock().clone();
        let fetch = spans
            .iter()
            .find(|span| span.name == super::span_name::TASK_FETCH)
            .expect("the fetch opened a span");
        assert_eq!(
            fetch.field("trace_id"),
            Some("4bf92f3577b34da6a3ce929d0e0e4736"),
            "a fetch with no scope still belongs to its job"
        );
        // No task of ours caused it, so there is no link to name.
        assert_eq!(fetch.field("link_span_id"), Some(""));
    }

    /// `submit` used to be an `async fn`, so callers spawned and joined it. The
    /// value that replaced it has to stay a `Future`, not only an `IntoFuture`.
    #[test]
    fn a_submit_value_is_a_send_future() {
        fn assert_send_future<F: std::future::Future + Send>(_: F) {}
        fn check(submit: Submit<'_>) {
            assert_send_future(submit)
        }
        let _ = check;
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
