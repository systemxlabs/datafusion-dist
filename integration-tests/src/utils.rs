use std::error::Error;

use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::{
    arrow::{array::RecordBatch, util::pretty::pretty_format_batches},
    physical_plan::display::DisplayableExecutionPlan,
};
use datafusion_dist::planner::{DefaultPlanner, DisplayableStagePlans, DistPlanner};
use futures::TryStreamExt;
use tonic::transport::Endpoint;
use uuid::Uuid;

use crate::data::build_session_context;

pub async fn assert_e2e(sql: &str, expected_result: &str) {
    let batches = execute_e2e_query(sql).await.unwrap();
    let batches_str = pretty_format_batches(&batches).unwrap().to_string();
    println!("Actual result: {batches_str}");
    assert_eq!(batches_str, expected_result,);
}

pub async fn execute_e2e_query(sql: &str) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
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

pub async fn assert_planner(sql: &str, expected_stage_plans: &str) {
    let ctx = build_session_context();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    println!(
        "Physical plan: {}",
        DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
    );

    let dist_planner = DefaultPlanner {};
    let stage_plans = dist_planner.plan_stages(Uuid::new_v4(), plan).unwrap();
    let actual = DisplayableStagePlans(&stage_plans).to_string();
    println!("Planner output: {actual}");
    assert_eq!(actual, expected_stage_plans);
}
