use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{RecordBatch, StringBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_plan::{
    ExecutionPlan, placeholder_row::PlaceholderRowExec, projection::ProjectionExec,
};
use futures::{StreamExt, stream::BoxStream};
use tokio::{
    runtime::Handle,
    sync::mpsc::{Receiver, Sender},
    task::JoinSet,
};

use crate::{DistError, DistResult, network::StageInfo, planner::StageId};

/// Check if the physical plan is a simple `SELECT 1` query.
/// This is used to identify queries that should be executed locally.
pub fn is_plan_select_1(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() else {
        return false;
    };
    if !proj.input().as_any().is::<PlaceholderRowExec>() {
        return false;
    }
    if proj.expr().len() != 1 {
        return false;
    }
    let expr = &proj.expr()[0];
    let Some(literal) = expr.expr.as_any().downcast_ref::<Literal>() else {
        return false;
    };
    matches!(
        literal.value(),
        ScalarValue::Int32(Some(1)) | ScalarValue::Int64(Some(1))
    )
}

pub fn timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64
}

// This function will spawn thread to get the local IP address, so don't call it frequently
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

#[derive(Debug)]
pub struct JobsArrowConverter {
    schema: SchemaRef,
}

impl Default for JobsArrowConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl JobsArrowConverter {
    pub fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("job_id", DataType::Utf8, false),
            Field::new("stages", DataType::Utf8, false),
        ]));
        Self { schema }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn convert(&self, jobs: &HashMap<StageId, StageInfo>) -> Result<RecordBatch, ArrowError> {
        let mut grouped_jobs = BTreeMap::new();
        for (stage_id, stage_info) in jobs {
            grouped_jobs
                .entry(stage_id.job_id.clone())
                .or_insert_with(BTreeMap::new)
                .insert(stage_id.stage.to_string(), stage_info.clone());
        }

        let mut job_id_builder = StringBuilder::new();
        let mut stages_builder = StringBuilder::new();

        for (job_id, stages) in grouped_jobs {
            job_id_builder.append_value(job_id.as_ref());
            let stages_json = serde_json::to_string(&stages)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
            stages_builder.append_value(stages_json);
        }

        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(job_id_builder.finish()),
                Arc::new(stages_builder.finish()),
            ],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_is_plan_select_1() {
        // Create a DataFusion session context
        let ctx = SessionContext::new();

        // Execute SQL "SELECT 1" and create physical plan
        let df = ctx.sql("SELECT 1").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        // Verify that is_plan_select_1 returns true for this plan
        assert!(is_plan_select_1(&plan));
    }
}
