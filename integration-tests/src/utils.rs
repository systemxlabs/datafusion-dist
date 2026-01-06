use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::Arc,
};

use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::{
    arrow::{array::RecordBatch, util::pretty::pretty_format_batches},
    physical_plan::{ExecutionPlan, display::DisplayableExecutionPlan},
};
use datafusion_dist::{
    DistResult,
    cluster::{NodeId, NodeState},
    planner::{DefaultPlanner, DisplayableStagePlans, DistPlanner, StageId, TaskId},
    schedule::{DefaultScheduler, DisplayableTaskDistribution, DistSchedule},
};
use futures::TryStreamExt;
use tonic::transport::Endpoint;
use uuid::Uuid;

use crate::data::build_session_context;

pub async fn assert_flightsql(sql: &str, expected_result: &str) {
    let batches = execute_flightsql_query(sql).await.unwrap();
    let batches_str = pretty_format_batches(&batches).unwrap().to_string();
    println!("Actual result: {batches_str}");
    assert_eq!(batches_str, expected_result,);
}

pub async fn execute_flightsql_query(sql: &str) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let endpoint = Endpoint::from_static("http://localhost:50061");
    let channel = endpoint.connect().await?;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);

    let flight_info = flight_sql_client.execute(sql.to_string(), None).await?;

    let mut batches = Vec::new();
    for endpoint in flight_info.endpoint {
        let ticket = endpoint
            .ticket
            .as_ref()
            .expect("ticket is required")
            .clone();
        let stream = flight_sql_client.do_get(ticket).await?;
        let result: Vec<RecordBatch> = stream.try_collect().await?;
        batches.extend(result);
    }
    Ok(batches)
}

pub async fn healthy_check_all_nodes() -> Result<(), Box<dyn Error>> {
    for port in [50061, 50071, 50081] {
        let endpoint = Endpoint::from_shared(format!("http://localhost:{port}"))?;
        let channel = endpoint.connect().await?;
        let mut flight_sql_client = FlightSqlServiceClient::new(channel);

        // This sql will be executed on connected node
        let flight_info = flight_sql_client
            .execute("SELECT 1".to_string(), None)
            .await?;
        let mut batches = Vec::new();
        for endpoint in flight_info.endpoint {
            let ticket = endpoint
                .ticket
                .as_ref()
                .expect("ticket is required")
                .clone();
            let stream = flight_sql_client.do_get(ticket).await?;
            let result: Vec<RecordBatch> = stream.try_collect().await?;
            batches.extend(result);
        }
        if batches.is_empty() {
            return Err("Healthy check failed: no data returned".into());
        }
    }
    Ok(())
}

pub async fn assert_planner(
    sql: &str,
    expected_plan: &str,
    expected_stage_plans: &str,
) -> HashMap<StageId, Arc<dyn ExecutionPlan>> {
    let ctx = build_session_context();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let plan_str = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    println!("Physical plan: {plan_str}");
    assert_eq!(plan_str, expected_plan);

    let dist_planner = DefaultPlanner {};
    let stage_plans = dist_planner.plan_stages(Uuid::new_v4(), plan).unwrap();
    let actual = DisplayableStagePlans(&stage_plans).to_string();
    println!("Planner output: {actual}");
    assert_eq!(actual, expected_stage_plans);

    stage_plans
}

pub fn mock_alive_nodes() -> (NodeId, HashMap<NodeId, NodeState>) {
    let mut nodes = HashMap::new();
    let local_node = NodeId {
        host: "localhost".to_string(),
        port: 50060,
    };
    nodes.insert(local_node.clone(), NodeState::default());
    nodes.insert(
        NodeId {
            host: "localhost".to_string(),
            port: 50070,
        },
        NodeState::default(),
    );
    nodes.insert(
        NodeId {
            host: "localhost".to_string(),
            port: 50080,
        },
        NodeState::default(),
    );
    (local_node, nodes)
}

pub async fn schedule_tasks(
    local_node: &NodeId,
    node_state: &HashMap<NodeId, NodeState>,
    stage_plans: &HashMap<StageId, Arc<dyn ExecutionPlan>>,
) -> DistResult<HashMap<TaskId, NodeId>> {
    let distribution = DefaultScheduler::new()
        .schedule(local_node, node_state, stage_plans)
        .await?;
    println!(
        "Task distribution: {}",
        DisplayableTaskDistribution(&distribution)
    );

    Ok(distribution)
}

pub fn assert_stage_distributed_into_one_node(stage: u32, distribution: &HashMap<TaskId, NodeId>) {
    let distinct_nodes: HashSet<_> = distribution
        .iter()
        .filter(|(task_id, _)| task_id.stage == stage)
        .map(|(_, node_id)| node_id)
        .collect();
    assert_eq!(distinct_nodes.len(), 1);
}

pub fn assert_stage_distributed_into_nodes(
    stage: u32,
    distribution: &HashMap<TaskId, NodeId>,
    target_num_nodes: usize,
) {
    let distinct_nodes: HashSet<_> = distribution
        .iter()
        .filter(|(task_id, _)| task_id.stage == stage)
        .map(|(_, node_id)| node_id)
        .collect();
    assert_eq!(distinct_nodes.len(), target_num_nodes);
}
