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

    let mut slts_dir = tokio::fs::read_dir(format!("tests/slts")).await?;
    while let Some(entry) = slts_dir.next_entry().await? {
        if entry.file_type().await?.is_file() {
            println!(
                "======== start to run file {} ========",
                entry.file_name().to_str().unwrap()
            );
            tester.run_file_async(entry.path()).await?;
        }
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    // all jobs should be cleaned up
    let batches = execute_flightsql_query("select * from running_jobs").await?;
    assert!(batches.is_empty());

    Ok(())
}
