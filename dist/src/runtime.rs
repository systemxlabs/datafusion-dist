use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    task::{Context, Poll},
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use datafusion_common::DataFusionError;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{
    ExecutionPlan, RecordBatchStream, display::DisplayableExecutionPlan,
    execution_plan::reset_plan_states, stream::RecordBatchStreamAdapter,
};

use futures::{Stream, StreamExt, TryStreamExt, future::BoxFuture, future::join_all};
use log::{debug, error, warn};
use tokio::{sync::mpsc::Sender, task::AbortHandle};
use tracing::{Instrument as _, Span};

use crate::{
    DistError, DistResult, JobId,
    cluster::{DistCluster, NodeId, NodeStatus},
    config::DistConfig,
    event::{Event, EventHandler, local_jobs, send_event_with_timeout, start_event_handler},
    executor::{DefaultExecutor, DistExecutor, logging_executor_metrics},
    heartbeat::Heartbeater,
    network::{DistNetwork, ScheduledTasks, StageInfo},
    planner::{
        DefaultPlanner, DisplayableStagePlans, DistPlanner, StageId, TaskId,
        check_initial_stage_plans, resolve_stage_plan,
    },
    scheduler::{DefaultScheduler, DisplayableTaskDistribution, DistScheduler},
    trace::{self, TraceContext},
    util::{ReceiverStreamBuilder, timestamp_ms},
};

#[derive(Debug, Clone)]
pub struct DistRuntime {
    pub node_id: NodeId,
    pub status: Arc<Mutex<NodeStatus>>,
    /// Task context used to execute stage partitions, see [`stage_task_ctx`].
    pub task_ctx: Arc<TaskContext>,
    pub config: Arc<DistConfig>,
    pub cluster: Arc<dyn DistCluster>,
    pub network: Arc<dyn DistNetwork>,
    pub planner: Arc<dyn DistPlanner>,
    pub scheduler: Arc<dyn DistScheduler>,
    pub executor: Arc<dyn DistExecutor>,
    pub heartbeater: Arc<Heartbeater>,
    pub stages: Arc<Mutex<HashMap<StageId, StageState>>>,
    pub event_sender: Sender<Event>,
    /// The trace of each job this node holds a stage of.
    ///
    /// Kept in step with `stages` — inserted when the tasks of a job arrive,
    /// dropped wherever its stages are — so that the trace of a job is a lookup
    /// rather than a scan of every stage, which is what `execute_remote` would
    /// otherwise pay once per fetched partition. Private: it is a shortcut for
    /// [`DistRuntime::job_trace_of`], not a second source of truth.
    job_traces: Arc<Mutex<HashMap<JobId, TraceContext>>>,
}

impl DistRuntime {
    pub fn new(
        task_ctx: Arc<TaskContext>,
        config: Arc<DistConfig>,
        cluster: Arc<dyn DistCluster>,
        network: Arc<dyn DistNetwork>,
    ) -> Self {
        let node_id = network.local_node();
        let status = Arc::new(Mutex::new(NodeStatus::Available));
        let stages = Arc::new(Mutex::new(HashMap::new()));
        let heartbeater = Heartbeater {
            node_id: node_id.clone(),
            cluster: cluster.clone(),
            stages: stages.clone(),
            heartbeat_interval: config.heartbeat_interval,
            status: status.clone(),
        };

        let (sender, receiver) = tokio::sync::mpsc::channel::<Event>(config.event_queue_size);

        let job_traces = Arc::new(Mutex::new(HashMap::new()));

        let event_handler = EventHandler {
            config: config.clone(),
            cluster: cluster.clone(),
            network: network.clone(),
            local_stages: stages.clone(),
            job_traces: job_traces.clone(),
            sender: sender.clone(),
            receiver,
        };
        start_event_handler(event_handler);

        Self {
            node_id: network.local_node(),
            status,
            task_ctx: stage_task_ctx(task_ctx),
            config,
            cluster,
            network,
            planner: Arc::new(DefaultPlanner),
            scheduler: Arc::new(DefaultScheduler::new()),
            executor: Arc::new(DefaultExecutor::new()),
            heartbeater: Arc::new(heartbeater),
            stages,
            event_sender: sender,
            job_traces,
        }
    }

    pub fn with_planner(self, planner: Arc<dyn DistPlanner>) -> Self {
        Self { planner, ..self }
    }

    pub fn with_scheduler(self, scheduler: Arc<dyn DistScheduler>) -> Self {
        Self { scheduler, ..self }
    }

    pub fn with_executor(self, executor: Arc<dyn DistExecutor>) -> Self {
        Self { executor, ..self }
    }

    pub async fn start(&self) {
        self.heartbeater.start();
        start_stage_cleaner(
            self.stages.clone(),
            self.job_traces.clone(),
            self.config.clone(),
        );
    }

    pub async fn shutdown(&self) {
        // Set status to Terminating
        *self.status.lock() = NodeStatus::Terminating;
        debug!("Set node status to Terminating, no new tasks will be assigned");

        self.heartbeater.send_heartbeat().await;
    }

    /// Submit a job for distributed execution.
    ///
    /// Returns a future: `.await` it as before, or name the trace the job belongs
    /// to first with [`Submit::with_trace_context`]. The span opened on awaiting
    /// is the job's entry point in this process; a caller that hands over a
    /// context gets its own span as the parent, and every line the nodes write
    /// about this job carries the same `trace_id`.
    pub fn submit(
        &self,
        job_id: impl Into<JobId>,
        plan: Arc<dyn ExecutionPlan>,
        job_meta: Arc<HashMap<String, String>>,
    ) -> Submit<'_> {
        Submit {
            state: SubmitState::Pending(SubmitParts {
                runtime: self,
                job_id: job_id.into(),
                plan,
                job_meta,
                trace: TraceContext::none(),
            }),
        }
    }

    async fn submit_inner(
        &self,
        job_id: JobId,
        plan: Arc<dyn ExecutionPlan>,
        job_meta: Arc<HashMap<String, String>>,
        // The context to hand to the children of the job's span: sending the
        // tasks to one node is one of them.
        job_trace: TraceContext,
        span: Span,
    ) -> DistResult<HashMap<TaskId, NodeId>> {
        debug!(
            "Submitting job {job_id} with meta {job_meta:?} and physical plan: \n{}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );

        let mut stage_plans = self.planner.plan_stages(job_id.clone(), plan)?;
        debug!(
            "job {job_id} initial stage plans:\n{}",
            DisplayableStagePlans(&stage_plans)
        );
        check_initial_stage_plans(job_id.clone(), &stage_plans)?;

        let node_states = self.cluster.alive_nodes().await?;
        span.record("stages", stage_plans.len());
        span.record("nodes", node_states.len());
        debug!(
            "alive nodes: {}",
            node_states
                .keys()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let task_distribution = self
            .scheduler
            .schedule(&self.node_id, &node_states, &stage_plans)
            .await?;
        debug!(
            "job {job_id} task distribution: {}",
            DisplayableTaskDistribution(&task_distribution)
        );
        let stage0_task_distribution: HashMap<TaskId, NodeId> = task_distribution
            .iter()
            .filter(|(task_id, _)| task_id.stage == 0)
            .map(|(task_id, node_id)| (task_id.clone(), node_id.clone()))
            .collect();
        if stage0_task_distribution.is_empty() {
            return Err(DistError::internal(format!(
                "Not found stage0 task distribution in {task_distribution:?} for job {job_id}"
            )));
        }

        // Resolve stage plans based on task distribution
        for stage_plan in stage_plans.values_mut() {
            *stage_plan = resolve_stage_plan(stage_plan.clone(), &task_distribution, self.clone())?;
        }
        debug!(
            "job {job_id} final stage plans:\n{}",
            DisplayableStagePlans(&stage_plans)
        );

        let mut node_stages = HashMap::new();
        let mut node_tasks = HashMap::new();
        for (task_id, node_id) in task_distribution.iter() {
            node_stages
                .entry(node_id.clone())
                .or_insert_with(HashSet::new)
                .insert(task_id.stage_id());
            node_tasks
                .entry(node_id.clone())
                .or_insert_with(Vec::new)
                .push(task_id.clone());
        }

        // Send stage plans to cluster nodes
        let mut handles = Vec::with_capacity(node_stages.len());
        for (node_id, stage_ids) in node_stages {
            let node_stage_plans = stage_ids
                .iter()
                .map(|stage_id| {
                    (
                        stage_id.clone(),
                        stage_plans
                            .get(stage_id)
                            .cloned()
                            .expect("stage id should be valid"),
                    )
                })
                .collect::<HashMap<_, _>>();

            let tasks = node_tasks.get(&node_id).cloned().unwrap_or_default();

            let scheduled_tasks = ScheduledTasks::new(
                node_stage_plans,
                tasks,
                Arc::new(task_distribution.clone()),
                job_meta.clone(),
            );

            if node_id == self.node_id {
                // Handing the tasks to ourselves is not a hop: the trace goes
                // straight from the job's span to the one this node opens for
                // receiving them, with no span id in between that nobody wrote.
                self.receive_tasks(scheduled_tasks.with_trace(job_trace))
                    .await?;
            } else {
                // Far away it is a hop: the delivery is a span of its own, and
                // the context the executor receives points at it, so the node
                // that runs the tasks can say where they came from without the
                // two sides sharing a clock.
                let (send_trace, send_span_id) = job_trace.child();
                debug!(
                    "Sending job {job_id} tasks [{}] to {node_id}",
                    scheduled_tasks
                        .task_ids
                        .iter()
                        .map(|t| format!("{}/{}", t.stage, t.partition))
                        .collect::<Vec<String>>()
                        .join(", ")
                );
                // The send runs on a task of its own, which is where a trace
                // of this job would otherwise stop: neither `tokio::spawn` nor
                // the transport below it carries the span of the caller.
                let send_span = trace::send_tasks_span(
                    &job_trace,
                    send_span_id,
                    &node_id,
                    scheduled_tasks.task_ids.len(),
                );
                let scheduled_tasks = scheduled_tasks.with_trace(send_trace);
                let network = self.network.clone();
                let handle = tokio::spawn(
                    async move {
                        network.send_tasks(node_id.clone(), scheduled_tasks).await?;
                        Ok::<_, DistError>(())
                    }
                    .instrument(send_span),
                );
                handles.push(handle);
            }
        }

        for handle in handles {
            handle.await??;
        }

        logging_executor_metrics(self.executor.handle());

        Ok(stage0_task_distribution)
    }

    /// Execute one task of a job on this node.
    ///
    /// The task runs in two places this future does not reach: the driver task
    /// that produces the batches, which is spawned on the CPU-bound executor,
    /// and the stream that is polled by whoever fetched the task. Both carry the
    /// span themselves, because neither inherits the span of its creator.
    pub async fn execute_local(&self, task_id: TaskId) -> DistResult<SendableRecordBatchStream> {
        self.execute_local_with_parent(task_id, None).await
    }

    /// Execute one task under a trace the caller supplies, rather than the one
    /// this node stored with the job.
    ///
    /// A node that learned the caller's context off the wire — the `ExecuteTask`
    /// call carries one when the caller has a span to name — passes it here: the
    /// task still belongs to its job, but the fetch that asked for it is its
    /// immediate parent. `parent` is used as given, so the task span derives its
    /// id from it exactly as it would from the stored context.
    ///
    /// `None` falls back to the stage's own trace, which is what
    /// [`DistRuntime::execute_local`] does — and so does
    /// `Some(TraceContext::none())`, which is what a caller that found an
    /// unusable header ends up with. An empty context is not an instruction to
    /// stop tracing the job: it is the absence of a parent, and the job's own
    /// trace is still worth continuing.
    pub async fn execute_local_with_parent(
        &self,
        task_id: TaskId,
        parent: Option<TraceContext>,
    ) -> DistResult<SendableRecordBatchStream> {
        let trace = match parent {
            Some(parent) if !parent.is_none() => parent,
            _ => self.stage_trace(&task_id.stage_id()),
        };
        let (_, span_id) = trace.child();
        let span = trace::task_span(&trace, span_id, &task_id, &self.node_id);
        // The task belongs to the job, and it is also the continuation of the
        // fetch that asked for it — two facts a single parent cannot express.
        span.follows_from(tracing::Span::current());
        let task_stream = self.prepare_task_stream(task_id, span)?;

        Ok(Box::pin(task_stream))
    }

    /// The trace of the stage a task belongs to, as this node holds it.
    ///
    /// `None` when this node holds no stage of the job: a task id alone does not
    /// tell us that the task belongs here, and inventing a trace for it would
    /// put a span in a trace nobody asked for.
    pub fn trace_of(&self, task_id: &TaskId) -> Option<TraceContext> {
        self.stages
            .lock()
            .get(&task_id.stage_id())
            .map(|stage| stage.trace)
    }

    /// The trace of a job this node holds any stage of.
    ///
    /// `None` when the job is unknown here, which is also what a job looks like
    /// after its stages were cleaned up.
    pub fn job_trace_of(&self, job_id: &JobId) -> Option<TraceContext> {
        job_trace(&self.stages, &self.job_traces, job_id)
    }

    /// The trace of a stage this node holds, or none if it holds no such stage.
    fn stage_trace(&self, stage_id: &StageId) -> TraceContext {
        self.stages
            .lock()
            .get(stage_id)
            .map(|stage| stage.trace)
            .unwrap_or_default()
    }

    fn prepare_task_stream(&self, task_id: TaskId, span: Span) -> DistResult<TaskStream> {
        // The setup below runs on the caller's task, so being in the span is
        // enough for it; the driver and the stream are handed over.
        let _enter = span.clone().entered();
        let stage_id = task_id.stage_id();

        let mut guard = self.stages.lock();
        let stage_state = guard
            .get_mut(&stage_id)
            .ok_or_else(|| DistError::internal(format!("Stage {stage_id} not found")))?;
        let (task_set_id, plan) = stage_state.get_plan(task_id.partition as usize)?;
        let schema = plan.schema();
        let last_active_ms = stage_state.last_active_ms.clone();

        let mut receiver_stream_builder = ReceiverStreamBuilder::new(2);

        let tx = receiver_stream_builder.tx();
        let partition = task_id.partition as usize;
        let task_ctx = self.task_ctx.clone();
        let driver_task = async move {
            let mut df_stream = plan.execute(partition, task_ctx)?;

            while let Some(batch) = df_stream.next().await {
                // Producing a batch means the task is still making progress.
                last_active_ms.store(timestamp_ms(), Ordering::Relaxed);
                let batch = batch.map_err(DistError::from);
                match tx.send(batch).await {
                    Ok(()) => {}
                    Err(e) => {
                        warn!("Dist driver task failed to send batch to channel: {e}");
                        return Ok(());
                    }
                }
            }
            Ok(()) as DistResult<()>
        }
        .instrument(span.clone());

        let abort_handle = receiver_stream_builder.spawn_on(driver_task, self.executor.handle());
        stage_state.start_task(task_id.partition as usize, task_set_id, abort_handle)?;
        drop(guard);

        let stream = receiver_stream_builder.build();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream.map_err(DataFusionError::from),
        ));

        Ok(TaskStream::new(
            task_id,
            task_set_id,
            self.stages.clone(),
            self.event_sender.clone(),
            stream,
            span,
        ))
    }

    pub async fn execute_remote(
        &self,
        node_id: NodeId,
        task_id: TaskId,
    ) -> DistResult<SendableRecordBatchStream> {
        if node_id == self.node_id {
            return Err(DistError::internal(format!(
                "remote node id {node_id} is actually self"
            )));
        }

        let trace = self.job_trace_of(&task_id.job_id).unwrap_or_default();
        // The hop the callee should treat as its parent is the span opened right
        // here, so the ids of the fetch and of the execution it causes line up.
        let (hop, span_id) = trace.child();
        let span = trace::task_fetch_span(&trace, span_id, &task_id, &node_id);
        span.follows_from(tracing::Span::current());
        async move {
            debug!("Executing remote task {task_id} on node {node_id}");
            self.network
                .execute_task_with_trace(node_id, task_id, hop)
                .await
        }
        .instrument(span)
        .await
    }

    /// Receive the tasks of a job on this node.
    ///
    /// The job id is part of the tasks — every [`TaskId`] carries it — so the
    /// span does not have to go looking for it in the job meta. That map is the
    /// caller's business data, and nothing in this library reads it.
    pub async fn receive_tasks(&self, scheduled_tasks: ScheduledTasks) -> DistResult<()> {
        // A missing task list reads as an empty field rather than an error:
        // the tasks are validated where they are used.
        let job_id = scheduled_tasks
            .job_id()
            .map(|id| id.to_string())
            .unwrap_or_default();
        // The context that arrived belongs to the call that delivered the tasks;
        // the stages keep the one their own children should use.
        let (stage_trace, span_id) = scheduled_tasks.trace.child();
        let span = trace::receive_tasks_span(
            &scheduled_tasks.trace,
            span_id,
            &job_id,
            &self.node_id,
            scheduled_tasks.task_ids.len(),
        );
        self.receive_tasks_inner(scheduled_tasks, stage_trace)
            .instrument(span)
            .await
    }

    async fn receive_tasks_inner(
        &self,
        scheduled_tasks: ScheduledTasks,
        trace: TraceContext,
    ) -> DistResult<()> {
        if matches!(*self.status.lock(), NodeStatus::Terminating) {
            return Err(DistError::internal(
                "Local node is in Terminating status, cannot receive tasks",
            ));
        }

        debug!(
            "Received job {} tasks: [{}] and plans of stages: [{}]",
            scheduled_tasks.job_id()?,
            scheduled_tasks
                .task_ids
                .iter()
                .map(|t| format!("{}/{}", t.stage, t.partition))
                .collect::<Vec<String>>()
                .join(", "),
            scheduled_tasks
                .stage_plans
                .keys()
                .map(|k| k.stage.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );

        let stage_states = StageState::from_scheduled_tasks(scheduled_tasks, trace)?;
        let stage_ids = stage_states.keys().cloned().collect::<Vec<StageId>>();
        {
            let mut guard = self.stages.lock();
            guard.extend(stage_states);
            drop(guard);
        }
        // The index of a job's trace follows its stages in, and out.
        for stage_id in &stage_ids {
            self.job_traces
                .lock()
                .entry(stage_id.job_id.clone())
                .or_insert(trace);
        }

        let stage0_ids = stage_ids
            .iter()
            .filter(|id| id.stage == 0)
            .cloned()
            .collect::<Vec<StageId>>();
        if !stage0_ids.is_empty() {
            send_event_with_timeout(&self.event_sender, Event::ReceivedStage0Tasks(stage0_ids))
                .await?;
        }

        Ok(())
    }

    pub fn cleanup_local_jobs(&self, job_ids: Vec<JobId>) {
        let span = trace::cleanup_batch_span(job_ids.len());
        let _enter = span.enter();

        debug!(
            "Cleaning up local Jobs [{}]",
            job_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        );
        let job_ids: HashSet<JobId> = job_ids.into_iter().collect();
        if job_ids.is_empty() {
            return;
        }

        cleanup_stages(&mut self.stages.lock(), |stage_id| {
            job_ids.contains(&stage_id.job_id)
        });
        let mut job_traces = self.job_traces.lock();
        for job_id in &job_ids {
            job_traces.remove(job_id);
        }
    }

    pub fn get_local_jobs(&self, job_ids: Option<&Vec<JobId>>) -> HashMap<StageId, StageInfo> {
        local_jobs(&self.stages, job_ids)
    }

    pub async fn get_all_jobs(&self) -> DistResult<HashMap<StageId, StageInfo>> {
        // First, get local status for all jobs
        let mut combined_status = local_jobs(&self.stages, None);

        // Then, get status from all other alive nodes
        let node_states = self.cluster.alive_nodes().await?;

        let mut futures = Vec::new();
        for node_id in node_states.keys() {
            if *node_id != self.node_id {
                let network = self.network.clone();
                let node_id = node_id.clone();
                futures.push(async move { network.get_jobs(node_id, None).await });
            }
        }

        for remote_status in join_all(futures).await {
            let remote_status = remote_status?;
            for (stage_id, remote_stage_info) in remote_status {
                combined_status
                    .entry(stage_id)
                    .and_modify(|existing| {
                        existing.merge(&remote_stage_info);
                    })
                    .or_insert(remote_stage_info);
            }
        }

        Ok(combined_status)
    }
}

/// Builds the [`TaskContext`] used to execute stage partitions on this node.
///
/// DataFusion's file stream work stealing lets the partitions of one scan
/// instance rebalance unopened files among themselves at runtime. A distributed
/// stage runs every partition as an isolated task that never polls its
/// siblings, so the partitions executed by a node would read the files of the
/// other nodes and duplicate rows. DataFusion documents that executors which
/// run each partition as an isolated task should turn work stealing off, so a
/// task partition only reads its own file group.
fn stage_task_ctx(task_ctx: Arc<TaskContext>) -> Arc<TaskContext> {
    if !task_ctx
        .session_config()
        .options()
        .execution
        .enable_file_stream_work_stealing
    {
        return task_ctx;
    }

    let mut session_config = task_ctx.session_config().clone();
    session_config
        .options_mut()
        .execution
        .enable_file_stream_work_stealing = false;

    Arc::new(TaskContext::new(
        task_ctx.task_id(),
        task_ctx.session_id(),
        session_config,
        task_ctx.scalar_functions().clone(),
        task_ctx.higher_order_functions().clone(),
        task_ctx.aggregate_functions().clone(),
        task_ctx.window_functions().clone(),
        task_ctx.runtime_env(),
    ))
}

/// A job about to be submitted, see [`DistRuntime::submit`].
///
/// Awaiting it submits the job; naming its trace first is optional, which is why
/// this is a value rather than a parameter on every `submit` call.
///
/// It is a [`Future`], not only an [`IntoFuture`]: a caller that hands the value
/// to `tokio::spawn`, `select!` or `join_all` — all of which took the old
/// `async fn submit` — keeps working.
#[must_use = "submitting a job does nothing until the value is awaited"]
pub struct Submit<'a> {
    state: SubmitState<'a>,
}

/// Where a [`Submit`] is: naming its trace happens before the first poll, and
/// polling is what turns the parts into the future that runs the job.
enum SubmitState<'a> {
    Pending(SubmitParts<'a>),
    /// Built on the first poll: [`Future::poll`] is the only place where the
    /// fields can be moved out, since `with_trace_context` runs before it.
    Started(BoxFuture<'a, DistResult<HashMap<TaskId, NodeId>>>),
}

struct SubmitParts<'a> {
    runtime: &'a DistRuntime,
    job_id: JobId,
    plan: Arc<dyn ExecutionPlan>,
    job_meta: Arc<HashMap<String, String>>,
    trace: TraceContext,
}

impl<'a> Submit<'a> {
    /// Trace the job with the caller's context.
    ///
    /// The caller's span becomes the parent of `dist.job.submit`, and every line
    /// this job produces on every node carries the same `trace_id`. The context
    /// is a W3C `traceparent`, so a host that already speaks it — or that took
    /// one off an incoming request — passes it through without translation:
    ///
    /// ```ignore
    /// runtime
    ///     .submit(job_id, plan, job_meta)
    ///     .with_trace_context(TraceContext::from_traceparent(&header)?)
    ///     .await?;
    /// ```
    ///
    /// A host that already opened the span its request runs under needs no
    /// string at all: [`TraceContext::from_parts`] or
    /// [`TraceContext::with_parent_span_id`] name that span directly.
    ///
    /// It has to be called before the value is awaited: once the job is running
    /// its trace is in the wild, and naming another one could only be ignored.
    pub fn with_trace_context(mut self, trace: TraceContext) -> Self {
        match self.state {
            SubmitState::Pending(ref mut parts) => parts.trace = trace,
            SubmitState::Started(_) => debug_assert!(
                false,
                "with_trace_context is called before the job is awaited"
            ),
        }
        self
    }

    /// The submitted job, as the future that polls it.
    fn start(&mut self) -> BoxFuture<'a, DistResult<HashMap<TaskId, NodeId>>> {
        let parts = match std::mem::replace(
            &mut self.state,
            SubmitState::Started(Box::pin(std::future::pending())),
        ) {
            SubmitState::Pending(parts) => parts,
            SubmitState::Started(_) => unreachable!("a submit future is started once"),
        };
        Box::pin(async move {
            let SubmitParts {
                runtime,
                job_id,
                plan,
                job_meta,
                trace,
            } = parts;
            // The span of this node's view of the job, and the context its
            // children inherit: the caller's span is the parent of both.
            let (job_trace, span_id) = trace.child();
            let span = trace::submit_span(&trace, span_id, &job_id);
            runtime
                .submit_inner(job_id, plan, job_meta, job_trace, span.clone())
                .instrument(span)
                .await
        })
    }
}

impl<'a> Future for Submit<'a> {
    type Output = DistResult<HashMap<TaskId, NodeId>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        if matches!(this.state, SubmitState::Pending(_)) {
            let inner = this.start();
            this.state = SubmitState::Started(inner);
        }
        match this.state {
            SubmitState::Started(ref mut inner) => inner.as_mut().poll(cx),
            SubmitState::Pending(_) => unreachable!("just started"),
        }
    }
}

// `Submit` needs no `IntoFuture` of its own: the standard library implements it
// for every `Future`, so `.await` reaches the `poll` above.

/// Tracks the runtime state of a single execution stage on the local node.
#[derive(Debug)]
pub struct StageState {
    pub stage_id: StageId,
    pub created_at_ms: i64,
    /// Timestamp (ms) of the last observed progress of this stage: a task of the
    /// stage was created, started, completed, or produced an output batch.
    /// Shared with the running task drivers so they can report progress without
    /// taking the stages lock.
    pub last_active_ms: Arc<AtomicI64>,
    pub stage_plan: Arc<dyn ExecutionPlan>,
    /// Partitions assigned to this local node for execution.
    pub assigned_partitions: HashSet<usize>,
    /// Active and completed task groups for this stage on this node.
    pub task_sets: Vec<TaskSet>,
    /// Global mapping of every task in the job to its assigned node.
    pub job_task_distribution: Arc<HashMap<TaskId, NodeId>>,
    pub job_meta: Arc<HashMap<String, String>>,
    /// The trace of the job, as this node should continue it: the context whose
    /// `parent_span_id` is the span this node opened when it received the tasks.
    pub trace: TraceContext,
}

impl StageState {
    pub fn from_scheduled_tasks(
        scheduled_tasks: ScheduledTasks,
        trace: TraceContext,
    ) -> DistResult<HashMap<StageId, StageState>> {
        let mut stage_tasks: HashMap<StageId, HashSet<TaskId>> = HashMap::new();
        for task_id in scheduled_tasks.task_ids {
            let stage_id = task_id.stage_id();
            stage_tasks.entry(stage_id).or_default().insert(task_id);
        }

        let mut stage_states = HashMap::new();
        for (stage_id, assigned_task_ids) in stage_tasks {
            let stage_state = StageState {
                stage_id: stage_id.clone(),
                created_at_ms: timestamp_ms(),
                last_active_ms: Arc::new(AtomicI64::new(timestamp_ms())),
                stage_plan: scheduled_tasks
                    .stage_plans
                    .get(&stage_id)
                    .ok_or_else(|| {
                        DistError::internal(format!("Not found plan of stage {stage_id}"))
                    })?
                    .clone(),
                assigned_partitions: assigned_task_ids
                    .iter()
                    .map(|task_id| task_id.partition as usize)
                    .collect(),
                task_sets: Vec::new(),
                job_task_distribution: scheduled_tasks.job_task_distribution.clone(),
                job_meta: scheduled_tasks.job_meta.clone(),
                trace,
            };
            stage_states.insert(stage_id, stage_state);
        }
        Ok(stage_states)
    }

    pub fn num_running_tasks(&self) -> usize {
        self.task_sets
            .iter()
            .map(|task_set| task_set.running_partitions.len())
            .sum()
    }

    /// Records that the stage made progress now.
    pub fn update_last_active(&self) {
        self.last_active_ms.store(timestamp_ms(), Ordering::Relaxed);
    }

    pub fn last_active_ms(&self) -> i64 {
        self.last_active_ms.load(Ordering::Relaxed)
    }

    /// Returns true if the stage has running tasks, but none of them has made
    /// progress for `inactivity_ms`. Stages without running tasks (never polled
    /// yet, or already finished) are not considered inactive.
    pub fn is_inactive(&self, now_ms: i64, inactivity_ms: i64) -> bool {
        self.num_running_tasks() > 0 && now_ms - self.last_active_ms() >= inactivity_ms
    }

    pub fn num_pending_tasks(&self) -> usize {
        let executed_partitions: HashSet<usize> = self
            .task_sets
            .iter()
            .flat_map(|task_set| {
                let mut executed: HashSet<usize> =
                    task_set.running_partitions.keys().copied().collect();
                executed.extend(task_set.dropped_partitions.keys());
                executed
            })
            .collect();

        self.assigned_partitions
            .difference(&executed_partitions)
            .count()
    }

    /// Returns true if all tasks assigned to this stage on the local node have completed.
    pub fn all_assigned_partitions_completed(&self) -> bool {
        self.num_running_tasks() == 0 && self.num_pending_tasks() == 0
    }

    pub fn get_plan(&mut self, partition: usize) -> DistResult<(Uuid, Arc<dyn ExecutionPlan>)> {
        if !self.assigned_partitions.contains(&partition) {
            let task_id = self.stage_id.task_id(partition as u32);
            return Err(DistError::internal(format!(
                "Task {task_id} not found in this node"
            )));
        }

        self.update_last_active();

        for task_set in self.task_sets.iter_mut() {
            if task_set.never_executed(&partition) {
                return Ok((task_set.id, task_set.shared_plan.clone()));
            }
        }

        let task_set_id = Uuid::new_v4();
        let new_task_set = TaskSet {
            id: task_set_id,
            shared_plan: reset_plan_states(self.stage_plan.clone())
                .map_err(|e| DistError::internal(format!("Failed to reset plan state: {e}")))?,
            running_partitions: HashMap::new(),
            dropped_partitions: HashMap::new(),
        };
        let shared_plan = new_task_set.shared_plan.clone();
        self.task_sets.push(new_task_set);

        Ok((task_set_id, shared_plan))
    }

    pub fn start_task(
        &mut self,
        partition: usize,
        task_set_id: Uuid,
        abort_handle: AbortHandle,
    ) -> DistResult<()> {
        let task_set = self
            .task_sets
            .iter_mut()
            .find(|task_set| task_set.id == task_set_id)
            .ok_or_else(|| DistError::internal(format!("Task set {task_set_id} not found")))?;
        task_set.running_partitions.insert(partition, abort_handle);
        self.update_last_active();
        Ok(())
    }

    pub fn complete_task(&mut self, task_id: TaskId, task_set_id: Uuid, task_metrics: TaskMetrics) {
        if let Some(task_set) = self
            .task_sets
            .iter_mut()
            .find(|task_set| task_set.id == task_set_id)
        {
            task_set
                .running_partitions
                .remove(&(task_id.partition as usize));
            task_set
                .dropped_partitions
                .insert(task_id.partition as usize, task_metrics);
        }
        self.update_last_active();
    }

    pub fn never_executed(&self) -> bool {
        self.task_sets
            .iter()
            .all(|set| set.running_partitions.is_empty() && set.dropped_partitions.is_empty())
    }

    pub fn abort_running_tasks(&mut self) {
        for task_set in &mut self.task_sets {
            task_set.abort_running_partitions();
        }
    }
}

#[derive(Debug)]
pub struct TaskSet {
    pub id: Uuid,
    pub shared_plan: Arc<dyn ExecutionPlan>,
    pub running_partitions: HashMap<usize, AbortHandle>,
    pub dropped_partitions: HashMap<usize, TaskMetrics>,
}

impl TaskSet {
    pub fn never_executed(&self, partition: &usize) -> bool {
        !self.running_partitions.contains_key(partition)
            && !self.dropped_partitions.contains_key(partition)
    }

    pub fn abort_running_partitions(&mut self) {
        for (_, abort_handle) in self.running_partitions.drain() {
            abort_handle.abort();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetrics {
    pub output_rows: usize,
    pub output_bytes: usize,
    pub completed: bool,
}

pub struct TaskStream {
    pub task_id: TaskId,
    pub task_set_id: Uuid,
    pub stages: Arc<Mutex<HashMap<StageId, StageState>>>,
    pub event_sender: Sender<Event>,
    pub stream: SendableRecordBatchStream,
    /// Span of this task. The stream is polled, and dropped, by whoever holds
    /// it — a tonic server, or the `ProxyExec` of the stage consuming this one
    /// — so it carries its own span instead of relying on the caller's. Private:
    /// the tracer this crate happens to use is not part of the public type.
    span: Span,
    pub output_rows: usize,
    pub output_bytes: usize,
    pub completed: bool,
}

impl TaskStream {
    pub fn new(
        task_id: TaskId,
        task_set_id: Uuid,
        stages: Arc<Mutex<HashMap<StageId, StageState>>>,
        event_sender: Sender<Event>,
        stream: SendableRecordBatchStream,
        span: Span,
    ) -> Self {
        Self {
            task_id,
            task_set_id,
            stages,
            event_sender,
            stream,
            span,
            output_rows: 0,
            output_bytes: 0,
            completed: false,
        }
    }
}

impl Stream for TaskStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // A host that runs at `datafusion_dist=debug` in production polls this
        // once per batch; when no subscriber is interested the span is disabled
        // and entering it is pure overhead.
        let _enter = (!self.span.is_disabled()).then(|| self.span.clone().entered());
        match self.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.output_rows += batch.num_rows();
                self.output_bytes += batch.get_array_memory_size();
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                self.completed = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for TaskStream {
    fn schema(&self) -> Arc<Schema> {
        self.stream.schema()
    }
}

impl Drop for TaskStream {
    fn drop(&mut self) {
        let _enter = (!self.span.is_disabled()).then(|| self.span.clone().entered());
        let task_id = self.task_id.clone();
        let task_set_id = self.task_set_id;
        let task_metrics = TaskMetrics {
            output_bytes: self.output_bytes,
            output_rows: self.output_rows,
            completed: self.completed,
        };
        debug!("Task {task_id} dropped with metrics: {task_metrics:?}");

        let should_send = {
            let mut guard = self.stages.lock();
            if let Some(stage_state) = guard.get_mut(&task_id.stage_id()) {
                stage_state.complete_task(task_id.clone(), task_set_id, task_metrics);
                stage_state.stage_id.stage == 0 && stage_state.all_assigned_partitions_completed()
            } else {
                false
            }
        };

        if should_send {
            let event = Event::CheckJobCompleted(task_id.job_id.clone());
            if let Err(e) = self.event_sender.try_send(event) {
                error!(
                    "Failed to send CheckJobCompleted event after task {task_id} stream dropped: {e}"
                );
            }
        }
    }
}

/// Returns the ids of the local stages that should be cleaned up, as
/// `(expired, inactive)`.
fn stages_to_clean(
    stages: &HashMap<StageId, StageState>,
    now_ms: i64,
    config: &DistConfig,
) -> (Vec<StageId>, Vec<StageId>) {
    let job_ttl_ms = config.job_ttl.as_millis() as i64;
    let inactivity_ms = config.stage_inactivity_timeout.as_millis() as i64;

    let mut expired = Vec::new();
    let mut inactive = Vec::new();
    for (stage_id, stage_state) in stages {
        if now_ms - stage_state.created_at_ms >= job_ttl_ms {
            expired.push(stage_id.clone());
        } else if stage_state.is_inactive(now_ms, inactivity_ms) {
            inactive.push(stage_id.clone());
        }
    }

    (expired, inactive)
}

fn start_stage_cleaner(
    stages: Arc<Mutex<HashMap<StageId, StageState>>>,
    job_traces: Arc<Mutex<HashMap<JobId, TraceContext>>>,
    config: Arc<DistConfig>,
) {
    tokio::spawn(async move {
        let display_ids = |ids: &[StageId]| {
            ids.iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };

        loop {
            tokio::time::sleep(config.job_ttl_check_interval).await;

            let mut guard = stages.lock();
            let (expired, inactive) = stages_to_clean(&guard, timestamp_ms(), &config);

            if !expired.is_empty() {
                debug!(
                    "Stages [{}] lifetime exceed job ttl {}s, cleaning up.",
                    display_ids(&expired),
                    config.job_ttl.as_secs()
                );
                cleanup_stages(&mut guard, |stage_id| expired.contains(stage_id));
            }

            if !inactive.is_empty() {
                warn!(
                    "Stages [{}] made no progress for more than {}s, cleaning up.",
                    display_ids(&inactive),
                    config.stage_inactivity_timeout.as_secs()
                );
                cleanup_stages(&mut guard, |stage_id| inactive.contains(stage_id));
            }

            // The trace index follows the stages out: what is no longer here
            // cannot be joined to anything. It is dropped per job rather than
            // per stage, so a job with another stage still alive loses its entry
            // too — `job_trace` then reads the stages once and files the answer
            // again, which costs one scan and is never wrong.
            for stage_id in expired.iter().chain(inactive.iter()) {
                job_traces.lock().remove(&stage_id.job_id);
            }
            drop(guard);
        }
    });
}

/// The trace of a job this node holds any stage of.
///
/// The index is read first, so the lookup does not scan every stage — which is
/// what a fetch of a remote task would otherwise pay once per partition. A miss
/// falls back to the stages, which is where the context lives, and fills the
/// index in: the index is a shortcut, not a second source of truth.
pub(crate) fn job_trace(
    stages: &Mutex<HashMap<StageId, StageState>>,
    index: &Mutex<HashMap<JobId, TraceContext>>,
    job_id: &JobId,
) -> Option<TraceContext> {
    if let Some(trace) = index.lock().get(job_id) {
        return Some(*trace);
    }
    let trace = stages
        .lock()
        .values()
        .find(|stage| stage.stage_id.job_id == *job_id)
        .map(|stage| stage.trace)?;
    index.lock().insert(job_id.clone(), trace);
    Some(trace)
}

pub(crate) fn cleanup_stages(
    stages: &mut HashMap<StageId, StageState>,
    mut should_cleanup: impl FnMut(&StageId) -> bool,
) {
    stages.retain(|stage_id, stage_state| {
        if should_cleanup(stage_id) {
            stage_state.abort_running_tasks();
            false
        } else {
            true
        }
    });
}
