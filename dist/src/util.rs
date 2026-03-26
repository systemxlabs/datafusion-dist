use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use datafusion_physical_plan::{
    placeholder_row::PlaceholderRowExec,
    projection::ProjectionExec,
    ExecutionPlan,
};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::Literal;
use futures::{StreamExt, stream::BoxStream};
use tokio::{
    runtime::Handle,
    sync::mpsc::{Receiver, Sender},
    task::JoinSet,
};

use crate::{DistError, DistResult};

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
    let Some(literal) = expr
        .expr
        .as_any()
        .downcast_ref::<Literal>()
    else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::Literal;
    use datafusion_physical_plan::{
        empty::EmptyExec, placeholder_row::PlaceholderRowExec, projection::ProjectionExec,
    };
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn test_is_plan_select_1_with_int32() {
        let placeholder = Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty()))) as Arc<dyn ExecutionPlan>;
        let literal = Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
        let proj = Arc::new(
            ProjectionExec::try_new(
                vec![(literal, "1".to_string())],
                placeholder,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(is_plan_select_1(&proj));
    }

    #[tokio::test]
    async fn test_is_plan_select_1_with_int64() {
        let placeholder = Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty()))) as Arc<dyn ExecutionPlan>;
        let literal = Arc::new(Literal::new(ScalarValue::Int64(Some(1)))) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
        let proj = Arc::new(
            ProjectionExec::try_new(
                vec![(literal, "1".to_string())],
                placeholder,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(is_plan_select_1(&proj));
    }

    #[tokio::test]
    async fn test_is_plan_select_1_not_matching() {
        // Test with value other than 1
        let placeholder = Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty()))) as Arc<dyn ExecutionPlan>;
        let literal = Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
        let proj = Arc::new(
            ProjectionExec::try_new(
                vec![(literal, "2".to_string())],
                placeholder,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(!is_plan_select_1(&proj));
    }

    #[tokio::test]
    async fn test_is_plan_select_1_not_projection() {
        // Test with non-projection plan
        let empty = Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])))) as Arc<dyn ExecutionPlan>;
        assert!(!is_plan_select_1(&empty));
    }

    #[tokio::test]
    async fn test_is_plan_select_1_wrong_input() {
        // Test with projection that doesn't have PlaceholderRowExec as input
        let empty = Arc::new(EmptyExec::new(Arc::new(Schema::empty()))) as Arc<dyn ExecutionPlan>;
        let literal = Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
        let proj = Arc::new(
            ProjectionExec::try_new(
                vec![(literal, "1".to_string())],
                empty,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(!is_plan_select_1(&proj));
    }

    #[tokio::test]
    async fn test_is_plan_select_1_multiple_exprs() {
        // Test with multiple expressions
        let placeholder = Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty()))) as Arc<dyn ExecutionPlan>;
        let literal1 = Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
        let literal2 = Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
        let proj = Arc::new(
            ProjectionExec::try_new(
                vec![(literal1, "1".to_string()), (literal2, "2".to_string())],
                placeholder,
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(!is_plan_select_1(&proj));
    }
}
