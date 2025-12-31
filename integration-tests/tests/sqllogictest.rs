use std::time::Duration;

use datafusion_dist_integration_tests::{setup_containers, utils::execute_flightsql_query};
use sqllogictest_flightsql::runner::FlightSqlDB;

#[tokio::test]
async fn sqllogictest() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    let mut tester = sqllogictest::Runner::new(|| async {
        FlightSqlDB::new_from_endpoint("dist", "http://localhost:50061").await
    });
    tester.with_column_validator(sqllogictest::strict_column_validator);

    tester.run_file_async("tests/sqllogictest.slt").await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    // all jobs should be cleaned up
    let batches = execute_flightsql_query("select * from running_jobs").await?;
    assert!(batches.is_empty());

    Ok(())
}
