use std::time::Duration;

use datafusion_dist_integration_tests::{setup_containers, utils::execute_flightsql_query};
use sqllogictest_flightsql::runner::FlightSqlDB;

#[tokio::test(flavor = "multi_thread")]
async fn sqllogictest() -> Result<(), Box<dyn std::error::Error>> {
    // Set a timeout for the entire test to prevent CI from hanging
    let test_future = async {
        println!("Setting up containers...");
        setup_containers().await;
        println!("Containers ready, starting sqllogictest...");

        let mut tester = sqllogictest::Runner::new(|| async {
            FlightSqlDB::new_from_endpoint("dist", "http://localhost:50061").await
        });
        tester.with_column_validator(sqllogictest::strict_column_validator);

        tester.run_file_async("tests/sqllogictest.slt").await?;
        println!("sqllogictest completed successfully");

        tokio::time::sleep(Duration::from_secs(5)).await;

        // all jobs should be cleaned up
        println!("Checking running_jobs cleanup...");
        let batches = execute_flightsql_query("select * from running_jobs").await?;
        assert!(batches.is_empty(), "Expected no running jobs after cleanup");
        println!("Test completed successfully");

        Ok::<(), Box<dyn std::error::Error>>(())
    };

    // 10 minute timeout for the entire test
    tokio::time::timeout(Duration::from_secs(600), test_future)
        .await
        .map_err(|_| "Test timed out after 10 minutes")??;

    Ok(())
}
